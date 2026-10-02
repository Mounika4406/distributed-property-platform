import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any
import asyncpg

logger = logging.getLogger(__name__)

class Database:
    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self, database_url: str, retries: int = 15, delay: float = 2.0):
        for attempt in range(1, retries + 1):
            try:
                self.pool = await asyncpg.create_pool(
                    database_url,
                    min_size=2,
                    max_size=20,
                    command_timeout=10
                )
                # Verify connection
                async with self.pool.acquire() as conn:
                    await conn.execute("SELECT 1")
                logger.info(f"Connected to database successfully on attempt {attempt}")
                return
            except Exception as e:
                logger.warning(f"Database connection attempt {attempt}/{retries} failed: {e}")
                if attempt == retries:
                    raise
                await asyncio.sleep(delay)

    async def disconnect(self):
        if self.pool:
            await self.pool.close()
            logger.info("Closed database connection pool")

db = Database()

async def get_property_by_id(property_id: int) -> Optional[Dict[str, Any]]:
    if not db.pool:
        raise RuntimeError("Database pool not initialized")
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, price, bedrooms, bathrooms, region_origin, version, updated_at
            FROM properties
            WHERE id = $1
            """,
            property_id
        )
        if row:
            return dict(row)
        return None

async def check_idempotency(key: str) -> Optional[Dict[str, Any]]:
    if not db.pool:
        raise RuntimeError("Database pool not initialized")
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT key, status_code, response_body, created_at
            FROM idempotency_keys
            WHERE key = $1
            """,
            key
        )
        if row:
            return dict(row)
        return None

async def record_idempotency(key: str, status_code: int, response_body: str):
    if not db.pool:
        raise RuntimeError("Database pool not initialized")
    async with db.pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO idempotency_keys (key, status_code, response_body, created_at)
            VALUES ($1, $2, $3, NOW())
            ON CONFLICT (key) DO NOTHING
            """,
            key, status_code, response_body
        )

async def update_property(property_id: int, new_price: float, expected_version: int) -> Optional[Dict[str, Any]]:
    """
    Atomically updates the property if and only if the current version matches expected_version.
    Increments version by 1 and updates updated_at timestamp.
    Returns the updated record, or None if version mismatch or not found.
    """
    if not db.pool:
        raise RuntimeError("Database pool not initialized")
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE properties
            SET price = $1,
                version = version + 1,
                updated_at = NOW()
            WHERE id = $2 AND version = $3
            RETURNING id, price, bedrooms, bathrooms, region_origin, version, updated_at
            """,
            new_price, property_id, expected_version
        )
        if row:
            return dict(row)
        return None

async def apply_replicated_update(
    property_id: int,
    price: float,
    bedrooms: Optional[int],
    bathrooms: Optional[int],
    region_origin: str,
    version: int,
    updated_at: datetime
) -> bool:
    """
    Applies an incoming replicated update from another region.
    Ensures optimistic consistency: updates the record if version > local version,
    or inserts if the property does not exist locally.
    """
    if not db.pool:
        raise RuntimeError("Database pool not initialized")
    async with db.pool.acquire() as conn:
        result = await conn.execute(
            """
            INSERT INTO properties (id, price, bedrooms, bathrooms, region_origin, version, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (id) DO UPDATE
            SET price = EXCLUDED.price,
                bedrooms = COALESCE(EXCLUDED.bedrooms, properties.bedrooms),
                bathrooms = COALESCE(EXCLUDED.bathrooms, properties.bathrooms),
                region_origin = EXCLUDED.region_origin,
                version = EXCLUDED.version,
                updated_at = EXCLUDED.updated_at
            WHERE properties.version < EXCLUDED.version
            """,
            property_id, price, bedrooms, bathrooms, region_origin, version, updated_at
        )
        logger.info(f"Replication applied for property {property_id} (version {version}): {result}")
        return True
