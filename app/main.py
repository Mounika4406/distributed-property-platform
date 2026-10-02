import logging
import json
from contextlib import asynccontextmanager
from typing import Optional
from fastapi import FastAPI, HTTPException, Header, Request, status, Response
from fastapi.responses import JSONResponse

from app.config import settings
from app.database import (
    db,
    get_property_by_id,
    check_idempotency,
    record_idempotency,
    update_property
)
from app.kafka_client import kafka_manager
from app.schemas import (
    PropertyUpdateRequest,
    PropertyUpdateResponse,
    PropertyDetailsResponse,
    ReplicationLagResponse,
    HealthCheckResponse,
    format_iso_z
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s (region=" + settings.REGION + "): %(message)s"
)
logger = logging.getLogger("property-service")

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"Starting backend service for region: {settings.REGION}")
    await db.connect(settings.DATABASE_URL)
    await kafka_manager.start()
    yield
    logger.info(f"Shutting down backend service for region: {settings.REGION}")
    await kafka_manager.stop()
    await db.disconnect()

app = FastAPI(
    title=f"Property Listing Backend ({settings.REGION.upper()})",
    description="Multi-region distributed property listing service with Kafka replication and optimistic locking.",
    version="1.0.0",
    lifespan=lifespan
)

# Helper function to process update logic
async def process_property_update(
    property_id: int,
    payload: PropertyUpdateRequest,
    x_request_id: Optional[str] = None
) -> JSONResponse:
    # 1. Idempotency Check
    if x_request_id:
        existing_key = await check_idempotency(x_request_id)
        if existing_key:
            logger.warning(f"Duplicate request detected for X-Request-ID: {x_request_id}")
            return JSONResponse(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                content={
                    "error": "Duplicate request",
                    "message": "A request with this X-Request-ID has already been processed.",
                    "request_id": x_request_id
                }
            )

    # 2. Check property existence
    current_property = await get_property_by_id(property_id)
    if not current_property:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Property with ID {property_id} not found."
        )

    # 3. Optimistic Locking Check
    if current_property["version"] != payload.version:
        logger.warning(
            f"Optimistic lock conflict on property {property_id}: "
            f"DB version={current_property['version']}, Payload version={payload.version}"
        )
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={
                "error": "Conflict",
                "message": f"Version mismatch. Current version is {current_property['version']}, but provided version was {payload.version}.",
                "current_version": current_property["version"],
                "provided_version": payload.version
            }
        )

    # 4. Atomic conditional update
    updated_record = await update_property(property_id, payload.price, payload.version)
    if not updated_record:
        # Concurrent update happened between read and write
        logger.warning(f"Concurrent update conflict on property {property_id}")
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={
                "error": "Conflict",
                "message": "Concurrent update detected. Please re-fetch and retry.",
                "provided_version": payload.version
            }
        )

    # 5. Format ISO timestamp with milliseconds and Z
    updated_at_str = format_iso_z(updated_record["updated_at"])

    response_data = {
        "id": updated_record["id"],
        "price": round(float(updated_record["price"]), 2),
        "version": updated_record["version"],
        "updated_at": updated_at_str
    }

    # 6. Record idempotency key if present
    if x_request_id:
        await record_idempotency(x_request_id, 200, json.dumps(response_data))

    # 7. Asynchronously publish event to Kafka
    kafka_event = {
        "id": updated_record["id"],
        "price": round(float(updated_record["price"]), 2),
        "bedrooms": updated_record["bedrooms"],
        "bathrooms": updated_record["bathrooms"],
        "region_origin": settings.REGION,
        "version": updated_record["version"],
        "updated_at": updated_at_str
    }
    try:
        await kafka_manager.publish_property_update(kafka_event)
    except Exception as e:
        logger.error(f"Failed to publish update to Kafka: {e}", exc_info=True)

    return JSONResponse(status_code=status.HTTP_200_OK, content=response_data)


# Helper function to get property details
async def process_get_property(property_id: int) -> JSONResponse:
    record = await get_property_by_id(property_id)
    if not record:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Property with ID {property_id} not found."
        )
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={
            "id": record["id"],
            "price": round(float(record["price"]), 2),
            "bedrooms": record["bedrooms"],
            "bathrooms": record["bathrooms"],
            "region_origin": record["region_origin"],
            "version": record["version"],
            "updated_at": format_iso_z(record["updated_at"])
        }
    )

# --- Routes for Health Check ---

@app.get("/health", response_model=HealthCheckResponse)
@app.get("/{region}/health", response_model=HealthCheckResponse)
async def health_check(region: Optional[str] = None):
    from datetime import datetime, timezone
    return HealthCheckResponse(
        status="ok",
        region=settings.REGION,
        timestamp=format_iso_z(datetime.now(timezone.utc))
    )

# --- Routes for Replication Lag ---

@app.get("/replication-lag", response_model=ReplicationLagResponse)
@app.get("/{region}/replication-lag", response_model=ReplicationLagResponse)
async def get_replication_lag(region: Optional[str] = None):
    lag = kafka_manager.get_replication_lag()
    return ReplicationLagResponse(lag_seconds=lag)

# --- Routes for Property Retrieval ---

@app.get("/properties/{id}", response_model=PropertyDetailsResponse)
@app.get("/{region}/properties/{id}", response_model=PropertyDetailsResponse)
async def get_property(id: int, region: Optional[str] = None):
    return await process_get_property(id)

# --- Routes for Property Update ---

@app.put("/properties/{id}", response_model=PropertyUpdateResponse)
@app.put("/{region}/properties/{id}", response_model=PropertyUpdateResponse)
async def update_property_endpoint(
    id: int,
    payload: PropertyUpdateRequest,
    region: Optional[str] = None,
    x_request_id: Optional[str] = Header(None, alias="X-Request-ID")
):
    return await process_property_update(id, payload, x_request_id)
