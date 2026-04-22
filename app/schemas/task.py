import uuid
from datetime import datetime

from pydantic import BaseModel

from app.models.task import TaskStatus


class TaskCreate(BaseModel):
    dataset_id: str
    filename: str


class TaskStatusUpdate(BaseModel):
    status: TaskStatus
    result: dict | None = None
    error: str | None = None


class TaskResponse(BaseModel):
    id: uuid.UUID
    dataset_id: str
    filename: str
    status: TaskStatus
    result: dict | None
    error: str | None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}
