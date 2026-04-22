"""Celery task for dataset processing.

Full implementation lives in Phase 3. This stub lets the worker start cleanly
and confirms that task registration works before the logic is wired in.
"""
import uuid

from app.core.celery_app import celery_app


@celery_app.task(
    name="tasks.process_dataset",
    acks_late=True,
    reject_on_worker_lost=True,
)
def process_dataset_task(task_id: str, file_path: str) -> dict:
    """Entry point called by the Celery worker to process one uploaded dataset.

    Args:
        task_id:   UUID string of the Task row in PostgreSQL (used to update status).
        file_path: Absolute path to the uploaded JSON file on disk.

    Returns:
        The processing result dict (also persisted to the Task.result column).

    State transitions driven by this task:
        PENDING -> RUNNING  (at task start)
        RUNNING -> COMPLETED (on success)
        RUNNING -> FAILED    (on any exception)
    """
    raise NotImplementedError("Phase 3: full implementation pending")
