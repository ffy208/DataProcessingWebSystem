"""POST /api/datasets/upload — accept a JSON dataset file and enqueue processing."""

import json
import logging
import uuid
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, UploadFile, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.database import get_db
from app.crud.task import create_task
from app.schemas.task import TaskCreate, TaskResponse
from app.services.validator import DatasetValidationError, validate_dataset
from app.tasks.process_dataset import process_dataset_task

logger = logging.getLogger(__name__)
router = APIRouter()

MAX_UPLOAD_BYTES = 10 * 1024 * 1024  # 10 MB

# Accept any content-type that could legitimately be JSON.
# curl and some HTTP clients send text/plain or omit the header entirely.
_ALLOWED_CONTENT_TYPES = {
    "application/json",
    "application/octet-stream",
    "text/plain",
    "text/plain; charset=utf-8",
    None,
}


@router.post(
    "/upload",
    response_model=TaskResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Upload a dataset JSON file and start async processing",
)
async def upload_dataset(
    file: UploadFile,
    db: AsyncSession = Depends(get_db),
) -> TaskResponse:
    """Accept a multipart/form-data upload of a JSON dataset file.

    Steps:
    1. Read and parse the uploaded file (reject non-JSON or structurally invalid files).
    2. Save the file to the uploads directory under a unique filename.
    3. Create a Task row in PENDING state.
    4. Enqueue the Celery processing task (returns immediately — fire and forget).
    5. Return 202 Accepted with the new task details so the client can poll.

    The file is saved before the Celery task is dispatched so the worker always
    finds it on disk, even if the task is picked up after a broker restart.
    """
    # Normalise content-type (strip params like "; charset=utf-8" for comparison)
    ct = (file.content_type or "").split(";")[0].strip().lower() or None
    if ct not in _ALLOWED_CONTENT_TYPES:
        raise HTTPException(
            status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            detail=f"Expected a JSON file, got content-type '{file.content_type}'",
        )

    # Read with a size cap to prevent OOM on huge uploads
    raw_bytes = await file.read(MAX_UPLOAD_BYTES + 1)
    if len(raw_bytes) > MAX_UPLOAD_BYTES:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"File exceeds the {MAX_UPLOAD_BYTES // (1024*1024)} MB limit",
        )

    # Parse and validate structure before writing to disk
    try:
        data = json.loads(raw_bytes.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"File is not valid JSON: {exc}",
        )

    try:
        validate_dataset(data)
    except DatasetValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        )

    # Save to uploads/<uuid>_<original_filename>
    settings = get_settings()
    upload_dir = Path(settings.upload_dir)
    upload_dir.mkdir(parents=True, exist_ok=True)

    safe_name = Path(file.filename or "upload.json").name
    unique_filename = f"{uuid.uuid4().hex}_{safe_name}"
    file_path = upload_dir / unique_filename

    file_path.write_bytes(raw_bytes)
    logger.info("Saved upload to %s", file_path)

    # Create DB record
    task = await create_task(
        db,
        TaskCreate(dataset_id=data["dataset_id"], filename=unique_filename),
    )

    # Enqueue Celery task (non-blocking)
    process_dataset_task.delay(str(task.id), str(file_path.resolve()))
    logger.info("Enqueued task %s for dataset %s", task.id, data["dataset_id"])

    return TaskResponse.model_validate(task)
