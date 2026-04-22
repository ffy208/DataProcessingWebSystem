from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from app.core.config import get_settings


class Base(DeclarativeBase):
    pass


def _make_engine():
    """Create the async SQLAlchemy engine from application settings.

    pool_pre_ping=True: validates connections before use, so stale connections
    (e.g. after a Postgres restart) are transparently recycled instead of raising.
    """
    settings = get_settings()
    return create_async_engine(
        settings.database_url,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10,
    )


engine = _make_engine()

AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
    autocommit=False,
)


async def get_db() -> AsyncSession:
    """FastAPI dependency that yields a per-request async database session.

    The session is automatically committed or rolled back and closed when the
    request finishes, via the async context manager.
    Usage: db: AsyncSession = Depends(get_db)
    """
    async with AsyncSessionLocal() as session:
        yield session
