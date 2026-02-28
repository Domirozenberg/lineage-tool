from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", case_sensitive=True)

    PROJECT_NAME: str = "Universal Data Lineage Tool"
    VERSION: str = "0.1.0"
    API_V1_STR: str = "/api/v1"

    # CORS
    CORS_ORIGINS: list[str] = ["http://localhost:3000", "http://localhost:8080"]

    # Neo4j
    NEO4J_URI: str = "bolt://localhost:7687"
    NEO4J_USER: str = "neo4j"
    NEO4J_PASSWORD: str = "password"
    # Connection pool
    NEO4J_MAX_CONNECTION_POOL_SIZE: int = 50
    NEO4J_MAX_CONNECTION_LIFETIME_S: float = 3600.0
    NEO4J_CONNECTION_ACQUISITION_TIMEOUT_S: float = 60.0
    NEO4J_CONNECTION_TIMEOUT_S: float = 30.0

    # Redis
    REDIS_URL: str = "redis://localhost:6379"

    # PostgreSQL connector defaults
    PG_HOST: str = "localhost"
    PG_PORT: int = 5433
    PG_DBNAME: str = "lineage_sample"
    PG_USER: str = "lineage"
    PG_PASSWORD: str = "lineage"
    PG_MIN_CONN: int = 1
    PG_MAX_CONN: int = 10

    # Auth
    SECRET_KEY: str = "change-me-in-production"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    REFRESH_TOKEN_EXPIRE_DAYS: int = 7
    # Bootstrapped admin (created on first startup when no users exist)
    FIRST_ADMIN_EMAIL: str = "admin@lineage-tool.dev"
    FIRST_ADMIN_PASSWORD: str = "change-me-in-production"


settings = Settings()
