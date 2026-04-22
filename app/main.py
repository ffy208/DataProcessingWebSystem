"""FastAPI application factory."""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from app.api.routes import datasets, tasks
from app.core.config import get_settings
from app.core.database import engine
from app.models.task import Task  # noqa: F401 — ensure model is registered with Base

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Run startup and shutdown logic around the app lifecycle.

    On startup: log settings so the first boot is easy to diagnose.
    On shutdown: dispose the async connection pool cleanly.
    """
    settings = get_settings()
    logger.info("Starting up — DB: %s  uploads: %s", settings.postgres_host, settings.upload_dir)
    yield
    await engine.dispose()
    logger.info("Shutdown complete")


def create_app() -> FastAPI:
    """Construct and configure the FastAPI application.

    Registers:
    - CORS middleware (all origins in dev; tighten for production)
    - /api/datasets router
    - /api/tasks router
    - global 500 exception handler
    - static file serving for the frontend (mounted last so API routes take priority)
    """
    app = FastAPI(
        title="Data Processing Web System",
        description="Upload JSON datasets and track async processing tasks.",
        version="1.0.0",
        lifespan=lifespan,
    )

    # CORS — allow the frontend (same origin in prod via static mount, but needed for dev)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # API routers
    app.include_router(datasets.router, prefix="/api/datasets", tags=["datasets"])
    app.include_router(tasks.router, prefix="/api/tasks", tags=["tasks"])

    # Global unhandled exception handler — return JSON instead of HTML 500
    @app.exception_handler(Exception)
    async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
        logger.error("Unhandled exception on %s %s: %s", request.method, request.url, exc, exc_info=True)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "An unexpected error occurred. Please try again later."},
        )

    # Serve frontend static files at root (mounted last — API routes take priority)
    app.mount("/", StaticFiles(directory="app/static", html=True), name="static")

    return app


app = create_app()
