from datetime import datetime, timezone
from typing import Optional
from pydantic import BaseModel, Field, field_serializer

def format_iso_z(dt: datetime) -> str:
    """Format datetime as ISO 8601 with millisecond precision and Z suffix."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

class PropertyUpdateRequest(BaseModel):
    price: float = Field(..., gt=0, description="Updated property price")
    version: int = Field(..., ge=1, description="Current version number for optimistic locking")

class PropertyUpdateResponse(BaseModel):
    id: int
    price: float
    version: int
    updated_at: str

    @field_serializer("price")
    def serialize_price(self, val: float) -> float:
        return round(val, 2)

class PropertyDetailsResponse(BaseModel):
    id: int
    price: float
    bedrooms: Optional[int] = None
    bathrooms: Optional[int] = None
    region_origin: str
    version: int
    updated_at: str

    @field_serializer("price")
    def serialize_price(self, val: float) -> float:
        return round(val, 2)

class PropertyKafkaEvent(BaseModel):
    id: int
    price: float
    bedrooms: Optional[int] = None
    bathrooms: Optional[int] = None
    region_origin: str
    version: int
    updated_at: str

class ReplicationLagResponse(BaseModel):
    lag_seconds: float

class HealthCheckResponse(BaseModel):
    status: str
    region: str
    timestamp: str
