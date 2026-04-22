"""GET /api/tasks/ and GET /api/tasks/{task_id} — query task status and results."""

import logging
import uuid

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.crud.task import get_task, list_tasks
from app.schemas.task import TaskResponse

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get(
    "/",
    response_model=list[TaskResponse],
    summary="List all tasks ordered by creation time (newest first)",
)
async def get_all_tasks(db: AsyncSession = Depends(get_db)) -> list[TaskResponse]:
    """Return metadata for every submitted task.

    Used by the frontend to render the full task table and drive polling:
    the UI calls this endpoint every 2 seconds and re-renders any rows whose
    status has changed since the last response.
    """
    tasks = await list_tasks(db)
    return [TaskResponse.model_validate(t) for t in tasks]


@router.get(
    "/{task_id}",
    response_model=TaskResponse,
    summary="Get status and result for a single task",
)
async def get_task_detail(
    task_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
) -> TaskResponse:
    """Fetch a single task by its UUID.

    Returns full detail including the result dict (populated once COMPLETED)
    and the error string (populated on FAILED). Returns 404 if the task_id
    does not exist.
    """
    task = await get_task(db, task_id)
    if task is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Task {task_id} not found",
        )
    return TaskResponse.model_validate(task)
