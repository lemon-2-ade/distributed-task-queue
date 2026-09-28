"""
Async SQLAlchemy engine and session factory.

One module-level engine per process: SQLAlchemy's async engine wraps
a connection *pool*, so creating a new engine per request would mean
opening a fresh TCP connection to Postgres for every HTTP request
instead of reusing a small set of already-authenticated connections.
`pool_pre_ping` issues a cheap SELECT 1 before handing out a pooled
connection, trading a small amount of latency for not handing the
caller a connection that Postgres (or a load balancer/proxy in
between) has since silently closed -- which otherwise shows up as a
confusing "connection already closed" error deep inside a request.

`expire_on_commit=False` on the sessionmaker: by default SQLAlchemy
expires all ORM objects after commit, so touching an attribute after
commit triggers a fresh SELECT. That's surprising for an API that
wants to commit a Task and immediately serialize it into a response.
"""

from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from config import get_settings

settings = get_settings()

engine = create_async_engine(
    settings.database_url,
    pool_size=10,
    max_overflow=5,
    pool_pre_ping=True,
)

AsyncSessionLocal = async_sessionmaker(
    engine,
    expire_on_commit=False,
    autoflush=False,
)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency: one session per request, always closed."""
    async with AsyncSessionLocal() as session:
        yield session
