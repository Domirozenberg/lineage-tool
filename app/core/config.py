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

    # Auth
    SECRET_KEY: str = "change-me-in-production"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30


settings = Settings()
