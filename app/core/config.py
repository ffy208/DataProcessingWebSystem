from pydantic_settings import BaseSettings, SettingsConfigDict
from functools import lru_cache


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "dataprocessing"
    postgres_user: str = "app"
    postgres_password: str = "secret"

    redis_host: str = "localhost"
    redis_port: int = 6379

    upload_dir: str = "uploads"

    @property
    def database_url(self) -> str:
        """Async DSN for SQLAlchemy (asyncpg driver), used by the FastAPI app."""
        return (
            f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def sync_database_url(self) -> str:
        """Sync DSN for SQLAlchemy (psycopg2 driver), used by Alembic migrations."""
        return (
            f"postgresql+psycopg2://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def redis_url(self) -> str:
        """Redis connection URL used as Celery broker and result backend."""
        return f"redis://{self.redis_host}:{self.redis_port}/0"


@lru_cache
def get_settings() -> Settings:
    """Return a cached singleton Settings instance.

    Uses lru_cache so the .env file is read only once per process lifetime.
    Call get_settings.cache_clear() in tests to reset between cases.
    """
    return Settings()
