import asyncio
import uuid
import time
import pytest
import httpx

BASE_URL = "http://localhost:8080"

@pytest.fixture
def http_client():
    return httpx.Client(base_url=BASE_URL, timeout=10.0)

def test_health_checks(http_client):
    """Verify health check endpoints for both regions via NGINX."""
    res_us = http_client.get("/us/health")
    assert res_us.status_code == 200, f"US health check failed: {res_us.text}"
    assert res_us.json()["status"] == "ok"
    assert res_us.json()["region"] == "us"

    res_eu = http_client.get("/eu/health")
    assert res_eu.status_code == 200, f"EU health check failed: {res_eu.text}"
    assert res_eu.json()["status"] == "ok"
    assert res_eu.json()["region"] == "eu"

def test_get_property_details(http_client):
    """Verify fetching existing property details."""
    res = http_client.get("/us/properties/1")
    assert res.status_code == 200, f"Get property 1 failed: {res.text}"
    data = res.json()
    assert data["id"] == 1
    assert "price" in data
    assert "version" in data
    assert "updated_at" in data

@pytest.mark.asyncio
async def test_concurrent_updates_optimistic_locking():
    """
    Simulates concurrent PUT requests to the same property from both regions
    with the same version number to verify that optimistic locking detects the race
    and rejects the competing write with HTTP 409 Conflict.
    """
    prop_id = 10
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=10.0) as client:
        # 1. Fetch current version
        res = await client.get(f"/us/properties/{prop_id}")
        assert res.status_code == 200
        current_data = res.json()
        current_version = current_data["version"]
        new_price_1 = round(float(current_data["price"]) + 10000.0, 2)
        new_price_2 = round(float(current_data["price"]) + 20000.0, 2)

        # 2. Prepare two simultaneous PUT requests with the exact same expected version
        req1 = client.put(
            f"/us/properties/{prop_id}",
            json={"price": new_price_1, "version": current_version}
        )
        req2 = client.put(
            f"/eu/properties/{prop_id}",
            json={"price": new_price_2, "version": current_version}
        )

        # 3. Fire both requests concurrently
        res1, res2 = await asyncio.gather(req1, req2)
        statuses = [res1.status_code, res2.status_code]

        print(f"\nConcurrent update responses: {statuses}")
        print(f"Req1 body: {res1.text}")
        print(f"Req2 body: {res2.text}")

        # 4. Exactly one request must succeed (200), and the other must conflict (409)
        assert 200 in statuses, "At least one request should succeed with 200"
        assert 409 in statuses, "The competing concurrent request must be rejected with 409 Conflict"

        # 5. Verify the property version was incremented to current_version + 1
        res_after = await client.get(f"/us/properties/{prop_id}")
        assert res_after.status_code == 200
        assert res_after.json()["version"] == current_version + 1

def test_outdated_version_rejected(http_client):
    """
    Verifies that a sequential update with a stale version number is rejected with HTTP 409.
    """
    prop_id = 3
    # Step 1: Read current version
    res = http_client.get(f"/us/properties/{prop_id}")
    assert res.status_code == 200
    v1 = res.json()["version"]

    # Step 2: Update with current version (should succeed)
    res_update = http_client.put(
        f"/us/properties/{prop_id}",
        json={"price": 600000.00, "version": v1}
    )
    assert res_update.status_code == 200
    assert res_update.json()["version"] == v1 + 1

    # Step 3: Attempt to update again using the old version v1 (must fail with 409)
    res_stale = http_client.put(
        f"/us/properties/{prop_id}",
        json={"price": 650000.00, "version": v1}
    )
    assert res_stale.status_code == 409
    assert "Conflict" in res_stale.text

def test_idempotency_with_x_request_id(http_client):
    """
    Verifies that duplicate PUT requests with the same X-Request-ID header
    are rejected with HTTP 422 Unprocessable Entity.
    """
    prop_id = 4
    res = http_client.get(f"/us/properties/{prop_id}")
    assert res.status_code == 200
    curr_version = res.json()["version"]

    req_id = str(uuid.uuid4())
    headers = {"X-Request-ID": req_id}
    payload = {"price": 750000.00, "version": curr_version}

    # First request: should succeed with 200 OK
    first_res = http_client.put(f"/us/properties/{prop_id}", json=payload, headers=headers)
    assert first_res.status_code == 200, f"First request failed: {first_res.text}"
    assert first_res.json()["version"] == curr_version + 1

    # Duplicate request with same X-Request-ID: must be rejected with 422
    second_res = http_client.put(f"/us/properties/{prop_id}", json=payload, headers=headers)
    assert second_res.status_code == 422, f"Expected 422 for duplicate request, got: {second_res.status_code}"
    assert "Duplicate request" in second_res.text

def test_cross_region_replication_and_lag(http_client):
    """
    Verifies that updates performed in US region replicate to EU region via Kafka,
    and checks the replication-lag endpoint.
    """
    prop_id = 2
    # 1. Fetch current version from US
    res_us = http_client.get(f"/us/properties/{prop_id}")
    assert res_us.status_code == 200
    curr_ver = res_us.json()["version"]
    new_price = round(float(res_us.json()["price"]) + 5000.0, 2)

    # 2. Update on US endpoint
    update_res = http_client.put(
        f"/us/properties/{prop_id}",
        json={"price": new_price, "version": curr_ver}
    )
    assert update_res.status_code == 200
    expected_version = curr_ver + 1

    # 3. Poll EU database until replication arrives (within 10 seconds)
    replicated = False
    for _ in range(20):
        time.sleep(0.5)
        res_eu = http_client.get(f"/eu/properties/{prop_id}")
        if res_eu.status_code == 200:
            eu_data = res_eu.json()
            if eu_data["version"] == expected_version and abs(eu_data["price"] - new_price) < 0.01:
                replicated = True
                break

    assert replicated, f"Replication to EU did not complete within timeout. Last EU state: {res_eu.text}"

    # 4. Verify replication lag endpoint on EU
    lag_res = http_client.get("/eu/replication-lag")
    assert lag_res.status_code == 200
    lag_data = lag_res.json()
    assert "lag_seconds" in lag_data
    assert lag_data["lag_seconds"] >= 0.0
