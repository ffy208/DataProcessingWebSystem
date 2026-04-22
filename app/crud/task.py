import uuid
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.task import Task, TaskStatus
from app.schemas.task import TaskCreate, TaskStatusUpdate


async def create_task(db: AsyncSession, data: TaskCreate) -> Task:
    """Insert a new Task row with PENDING status and return the persisted object."""
    task = Task(
        dataset_id=data.dataset_id,
        filename=data.filename,
        status=TaskStatus.PENDING,
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    db.add(task)
    await db.commit()
    await db.refresh(task)
    return task


async def get_task(db: AsyncSession, task_id: uuid.UUID) -> Task | None:
    """Fetch a single Task by primary key; returns None if not found."""
    result = await db.execute(select(Task).where(Task.id == task_id))
    return result.scalar_one_or_none()


async def list_tasks(db: AsyncSession) -> list[Task]:
    """Return all Tasks ordered by creation time descending (newest first)."""
    result = await db.execute(select(Task).order_by(Task.created_at.desc()))
    return list(result.scalars().all())


async def update_task_status(
    db: AsyncSession, task_id: uuid.UUID, update: TaskStatusUpdate
) -> Task | None:
    """Update the status (and optionally result/error) of an existing Task.

    Called by the Celery task worker to transition state:
      PENDING -> RUNNING on start, then RUNNING -> COMPLETED or FAILED on finish.
    Returns None if the task_id does not exist.
    """
    task = await get_task(db, task_id)
    if task is None:
        return None
    task.status = update.status
    task.updated_at = datetime.now(timezone.utc)
    if update.result is not None:
        task.result = update.result
    if update.error is not None:
        task.error = update.error
    await db.commit()
    await db.refresh(task)
    return task
