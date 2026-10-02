import os
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    REGION: str = os.getenv("REGION", "us").lower()
    DATABASE_URL: str = os.getenv("DATABASE_URL", "postgresql://postgres:postgrespassword@localhost:5432/property_db")
    KAFKA_BROKER: str = os.getenv("KAFKA_BROKER", "kafka:29092")
    KAFKA_TOPIC: str = os.getenv("KAFKA_TOPIC", "property-updates")
    PORT: int = int(os.getenv("PORT", "8000"))
    HOST: str = os.getenv("HOST", "0.0.0.0")

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

settings = Settings()
