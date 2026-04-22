"""Celery task: process one uploaded dataset file end-to-end."""

import json
import logging
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.core.celery_app import celery_app
from app.core.config import get_settings
from app.models.task import Task, TaskStatus
from app.services.processor import process_dataset
from app.services.validator import DatasetValidationError, validate_dataset

logger = logging.getLogger(__name__)

PROCESSING_DELAY_SECONDS = 15


def _make_sync_session() -> sessionmaker:
    """Create a synchronous SQLAlchemy sessionmaker for use inside Celery workers.

    Celery tasks run in a plain synchronous context, so we cannot use the
    async engine from database.py. This function creates a separate sync
    engine backed by psycopg2 each time it is called (connection pool is
    per-worker-process, so this is called once at task startup).
    """
    settings = get_settings()
    engine = create_engine(settings.sync_database_url, pool_pre_ping=True)
    return sessionmaker(bind=engine, expire_on_commit=False)


@contextmanager
def _get_sync_db():
    """Context manager that yields a sync SQLAlchemy session and auto-closes it."""
    SessionLocal = _make_sync_session()
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


def _update_status(
    session: Session,
    task_id: uuid.UUID,
    status: TaskStatus,
    result: dict | None = None,
    error: str | None = None,
) -> None:
    """Persist a status transition to the tasks table.

    Fetches the Task row, updates status/result/error and updated_at in-place,
    then commits. Raises if the row is not found (indicates a bug upstream).
    """
    task = session.get(Task, task_id)
    if task is None:
        raise RuntimeError(f"Task {task_id} not found in database")
    task.status = status
    task.updated_at = datetime.now(timezone.utc)
    if result is not None:
        task.result = result
    if error is not None:
        task.error = error
    session.commit()


@celery_app.task(
    name="tasks.process_dataset",
    acks_late=True,
    reject_on_worker_lost=True,
)
def process_dataset_task(task_id: str, file_path: str) -> dict:
    """Celery entry point: validate, process, and persist results for one dataset.

    Args:
        task_id:   String UUID of the Task row to update (serialised for JSON transport).
        file_path: Absolute path to the uploaded JSON file on the shared volume.

    Returns:
        The result dict (also written to Task.result in PostgreSQL).

    State transitions:
        PENDING -> RUNNING  on entry
        RUNNING -> COMPLETED on success (result stored)
        RUNNING -> FAILED   on any exception (error message stored)
    """
    tid = uuid.UUID(task_id)
    logger.info("Starting task %s for file %s", task_id, file_path)

    with _get_sync_db() as db:
        # PENDING -> RUNNING
        _update_status(db, tid, TaskStatus.RUNNING)

    try:
        # Simulate long-running computation
        time.sleep(PROCESSING_DELAY_SECONDS)

        # Load and validate
        with open(file_path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        validate_dataset(raw)

        # Compute result
        result = process_dataset(raw)

        with _get_sync_db() as db:
            _update_status(db, tid, TaskStatus.COMPLETED, result=result)

        logger.info("Task %s completed successfully", task_id)
        return result

    except (DatasetValidationError, json.JSONDecodeError, OSError) as exc:
        error_msg = f"{type(exc).__name__}: {exc}"
        logger.warning("Task %s failed (expected): %s", task_id, error_msg)
        with _get_sync_db() as db:
            _update_status(db, tid, TaskStatus.FAILED, error=error_msg)
        raise

    except Exception as exc:
        error_msg = f"{type(exc).__name__}: {exc}"
        logger.error("Task %s failed (unexpected): %s", task_id, error_msg, exc_info=True)
        with _get_sync_db() as db:
            _update_status(db, tid, TaskStatus.FAILED, error=error_msg)
        raise
