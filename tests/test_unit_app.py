import pytest
from unittest.mock import AsyncMock, patch
from datetime import datetime, timezone
from httpx import AsyncClient, ASGITransport

from app.main import app
from app.config import settings

@pytest.fixture(autouse=True)
def mock_app_lifespan(monkeypatch):
    """Bypass actual external DB and Kafka connections during unit tests."""
    monkeypatch.setattr("app.database.db.connect", AsyncMock())
    monkeypatch.setattr("app.database.db.disconnect", AsyncMock())
    monkeypatch.setattr("app.kafka_client.kafka_manager.start", AsyncMock())
    monkeypatch.setattr("app.kafka_client.kafka_manager.stop", AsyncMock())

@pytest.mark.asyncio
async def test_health_check_endpoint():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        res = await ac.get("/us/health")
        assert res.status_code == 200
        data = res.json()
        assert data["status"] == "ok"
        assert data["region"] == settings.REGION
        assert "timestamp" in data

@pytest.mark.asyncio
async def test_replication_lag_endpoint():
    with patch("app.kafka_client.kafka_manager.get_replication_lag", return_value=1.85):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
            res = await ac.get("/us/replication-lag")
            assert res.status_code == 200
            assert res.json() == {"lag_seconds": 1.85}

@pytest.mark.asyncio
async def test_get_property_endpoint():
    mock_prop = {
        "id": 1,
        "price": 350000.00,
        "bedrooms": 3,
        "bathrooms": 2,
        "region_origin": "us",
        "version": 1,
        "updated_at": datetime(2026, 10, 2, 12, 0, 0, tzinfo=timezone.utc)
    }
    with patch("app.main.get_property_by_id", AsyncMock(return_value=mock_prop)):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
            res = await ac.get("/us/properties/1")
            assert res.status_code == 200
            data = res.json()
            assert data["id"] == 1
            assert data["price"] == 350000.00
            assert data["version"] == 1
            assert data["updated_at"] == "2026-10-02T12:00:00.000Z"

@pytest.mark.asyncio
async def test_optimistic_locking_conflict():
    # Record currently at version 2
    mock_prop = {
        "id": 1,
        "price": 350000.00,
        "bedrooms": 3,
        "bathrooms": 2,
        "region_origin": "us",
        "version": 2,
        "updated_at": datetime(2026, 10, 2, 12, 0, 0, tzinfo=timezone.utc)
    }
    # Request submits outdated version 1
    with patch("app.main.get_property_by_id", AsyncMock(return_value=mock_prop)), \
         patch("app.main.check_idempotency", AsyncMock(return_value=None)):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
            res = await ac.put(
                "/us/properties/1",
                json={"price": 400000.00, "version": 1}
            )
            assert res.status_code == 409
            data = res.json()
            assert data["error"] == "Conflict"
            assert data["current_version"] == 2
            assert data["provided_version"] == 1

@pytest.mark.asyncio
async def test_successful_update_increments_version_and_publishes_event():
    # Record currently at version 1
    mock_prop = {
        "id": 1,
        "price": 350000.00,
        "bedrooms": 3,
        "bathrooms": 2,
        "region_origin": "us",
        "version": 1,
        "updated_at": datetime(2026, 10, 2, 12, 0, 0, tzinfo=timezone.utc)
    }
    updated_prop = {
        "id": 1,
        "price": 400000.00,
        "bedrooms": 3,
        "bathrooms": 2,
        "region_origin": "us",
        "version": 2,
        "updated_at": datetime(2026, 10, 2, 12, 5, 0, tzinfo=timezone.utc)
    }
    mock_publish = AsyncMock()
    with patch("app.main.get_property_by_id", AsyncMock(return_value=mock_prop)), \
         patch("app.main.check_idempotency", AsyncMock(return_value=None)), \
         patch("app.main.update_property", AsyncMock(return_value=updated_prop)), \
         patch("app.kafka_client.kafka_manager.publish_property_update", mock_publish):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
            res = await ac.put(
                "/us/properties/1",
                json={"price": 400000.00, "version": 1}
            )
            assert res.status_code == 200
            data = res.json()
            assert data["id"] == 1
            assert data["price"] == 400000.00
            assert data["version"] == 2
            assert data["updated_at"] == "2026-10-02T12:05:00.000Z"
            # Verify Kafka event published
            mock_publish.assert_awaited_once()
            published_event = mock_publish.call_args[0][0]
            assert published_event["id"] == 1
            assert published_event["price"] == 400000.00
            assert published_event["version"] == 2
            assert published_event["region_origin"] == "us"

@pytest.mark.asyncio
async def test_idempotency_returns_422_on_duplicate():
    # Mock idempotency table already containing the key
    with patch("app.main.check_idempotency", AsyncMock(return_value={"key": "dup-uuid-123"})):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
            res = await ac.put(
                "/us/properties/1",
                json={"price": 400000.00, "version": 1},
                headers={"X-Request-ID": "dup-uuid-123"}
            )
            assert res.status_code == 422
            data = res.json()
            assert data["error"] == "Duplicate request"
            assert data["request_id"] == "dup-uuid-123"
