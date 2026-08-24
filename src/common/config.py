import os
from pydantic import BaseModel

class Settings(BaseModel):
    PROJECT_NAME: str = "Campus Resource Orchestration Platform"
    ENVIRONMENT: str = os.getenv("ENVIRONMENT", "development")
    PORT: int = int(os.getenv("PORT", "8000"))
    
    # Security / Auth
    JWT_SECRET: str = os.getenv("JWT_SECRET", "super-secret-jwt-key-for-campus-orchestration-2026")
    JWT_ALGORITHM: str = "HS256"
    JWT_EXPIRATION_MINUTES: int = 120
    
    # PostgreSQL Configuration
    POSTGRES_USER: str = os.getenv("POSTGRES_USER", "postgres")
    POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "postgres")
    POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT: int = int(os.getenv("POSTGRES_PORT", "5432"))
    POSTGRES_DB: str = os.getenv("POSTGRES_DB", "campus_booking")
    
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL", 
        f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )
    SQLITE_PATH: str = os.getenv("SQLITE_PATH", "campus_local.db")
    USE_SQLITE_FALLBACK: bool = os.getenv("USE_SQLITE_FALLBACK", "true").lower() in ("true", "1", "yes")

    # Redis Configuration
    REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
    REDIS_URL: str = os.getenv("REDIS_URL", f"redis://{REDIS_HOST}:{REDIS_PORT}/0")
    # Set USE_REAL_REDIS=true to keep notification state in a real Redis server
    USE_REAL_REDIS: bool = os.getenv("USE_REAL_REDIS", "false").lower() in ("true", "1", "yes")

    # Kafka Configuration
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    # Set USE_REAL_KAFKA=true to use a real Kafka cluster instead of the in-memory emulation
    USE_REAL_KAFKA: bool = os.getenv("USE_REAL_KAFKA", "false").lower() in ("true", "1", "yes")
    KAFKA_NUM_PARTITIONS: int = int(os.getenv("KAFKA_NUM_PARTITIONS", "4"))
    KAFKA_REPLICATION_FACTOR: int = int(os.getenv("KAFKA_REPLICATION_FACTOR", "1"))
    TOPIC_BOOKING_EVENTS: str = "campus.bookings.events"
    TOPIC_SERVICE_REQUESTS: str = "campus.service_requests.events"
    TOPIC_DLQ: str = "campus.events.dlq"
    TOPIC_AUDIT: str = "campus.audit.events"

    # OLAP DuckDB
    DUCKDB_PATH: str = os.getenv("DUCKDB_PATH", "analytics.duckdb")
    
    # Fault Tolerance & Retries (STR-07)
    MAX_RETRY_ATTEMPTS: int = 3
    INITIAL_BACKOFF_SECONDS: float = 0.5
    BACKOFF_MULTIPLIER: float = 2.0

settings = Settings()
