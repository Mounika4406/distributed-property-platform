import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from app.config import settings
from app.database import apply_replicated_update

logger = logging.getLogger(__name__)

class KafkaManager:
    def __init__(self):
        self.producer: Optional[AIOKafkaProducer] = None
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.consumer_task: Optional[asyncio.Task] = None
        self.last_consumed_updated_at: Optional[datetime] = None
        self.is_running: bool = False

    async def start(self, retries: int = 20, delay: float = 2.0):
        # Initialize producer
        for attempt in range(1, retries + 1):
            try:
                self.producer = AIOKafkaProducer(
                    bootstrap_servers=settings.KAFKA_BROKER,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8")
                )
                await self.producer.start()
                logger.info(f"Kafka Producer connected to {settings.KAFKA_BROKER} on attempt {attempt}")
                break
            except Exception as e:
                logger.warning(f"Kafka Producer connection attempt {attempt}/{retries} failed: {e}")
                if attempt == retries:
                    raise
                await asyncio.sleep(delay)

        # Initialize consumer
        group_id = f"property-sync-consumer-{settings.REGION}"
        for attempt in range(1, retries + 1):
            try:
                self.consumer = AIOKafkaConsumer(
                    settings.KAFKA_TOPIC,
                    bootstrap_servers=settings.KAFKA_BROKER,
                    group_id=group_id,
                    auto_offset_reset="latest",
                    enable_auto_commit=True,
                    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
                )
                await self.consumer.start()
                logger.info(f"Kafka Consumer ({group_id}) connected to {settings.KAFKA_BROKER} on attempt {attempt}")
                break
            except Exception as e:
                logger.warning(f"Kafka Consumer connection attempt {attempt}/{retries} failed: {e}")
                if attempt == retries:
                    raise
                await asyncio.sleep(delay)

        self.is_running = True
        self.consumer_task = asyncio.create_task(self._consume_loop())

    async def _consume_loop(self):
        logger.info(f"Kafka consumer loop started for region {settings.REGION}")
        try:
            async for msg in self.consumer:
                if not self.is_running:
                    break
                try:
                    payload = msg.value
                    origin = payload.get("region_origin", "").lower()
                    
                    # Ignore messages originating from own region
                    if origin == settings.REGION:
                        logger.debug(f"Ignoring update from own region ({origin}) for property {payload.get('id')}")
                        continue
                    
                    logger.info(f"Received cross-region update from {origin}: {payload}")
                    
                    # Parse updated_at timestamp
                    updated_at_str = payload.get("updated_at")
                    if updated_at_str:
                        # Handle Z suffix
                        clean_str = updated_at_str.replace("Z", "+00:00")
                        updated_at_dt = datetime.fromisoformat(clean_str)
                    else:
                        updated_at_dt = datetime.now(timezone.utc)
                    
                    # Apply update to local PostgreSQL database
                    await apply_replicated_update(
                        property_id=payload["id"],
                        price=float(payload["price"]),
                        bedrooms=payload.get("bedrooms"),
                        bathrooms=payload.get("bathrooms"),
                        region_origin=origin,
                        version=int(payload["version"]),
                        updated_at=updated_at_dt
                    )
                    
                    # Record timestamp for replication lag monitoring
                    self.last_consumed_updated_at = updated_at_dt
                    logger.info(f"Updated last_consumed_updated_at to {updated_at_dt}")

                except Exception as ex:
                    logger.error(f"Error processing Kafka message: {ex}", exc_info=True)
        except asyncio.CancelledError:
            logger.info("Kafka consumer loop cancelled")
        except Exception as e:
            logger.error(f"Kafka consumer loop fatal error: {e}", exc_info=True)

    async def publish_property_update(self, event_data: Dict[str, Any]):
        if not self.producer:
            raise RuntimeError("Kafka Producer is not initialized")
        logger.info(f"Publishing property update to {settings.KAFKA_TOPIC}: {event_data}")
        await self.producer.send_and_wait(settings.KAFKA_TOPIC, event_data)

    def get_replication_lag(self) -> float:
        """
        Calculates time difference between updated_at timestamp of the last message
        consumed from Kafka and the current time.
        """
        if self.last_consumed_updated_at is None:
            return 0.0
        now_utc = datetime.now(timezone.utc)
        if self.last_consumed_updated_at.tzinfo is None:
            last_ts = self.last_consumed_updated_at.replace(tzinfo=timezone.utc)
        else:
            last_ts = self.last_consumed_updated_at
        lag = (now_utc - last_ts).total_seconds()
        return round(max(0.0, lag), 3)

    async def stop(self):
        self.is_running = False
        if self.consumer_task:
            self.consumer_task.cancel()
            try:
                await self.consumer_task
            except asyncio.CancelledError:
                pass
        if self.consumer:
            await self.consumer.stop()
            logger.info("Kafka consumer stopped")
        if self.producer:
            await self.producer.stop()
            logger.info("Kafka producer stopped")

kafka_manager = KafkaManager()
