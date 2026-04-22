"""Full end-to-end integration tests.

These tests exercise the COMPLETE pipeline without any mocks:
  upload file → DB write (PENDING) → Celery task → DB writes (RUNNING → COMPLETED/FAILED)
  → API read → verify result

What is real:
  - PostgreSQL database (reads and writes via both async and sync engines)
  - Celery task execution (all business logic: validator, processor, DB updates)
  - File I/O (files are written and read from a temp directory)
  - FastAPI request/response cycle (via httpx ASGITransport)

What is NOT mocked:
  - Nothing in the business logic

Two thin test-specific overrides (NOT mocks of business logic):
  - PROCESSING_DELAY_SECONDS = 0  (skip the artificial 15s sleep only)
  - celery_app.conf.task_always_eager = True  (run task synchronously, no broker needed)

Requirements:
  PostgreSQL must be reachable at the configured POSTGRES_* env vars (defaults: localhost:5432).
  Run `docker-compose up postgres -d` before executing this suite.

  uv run pytest tests/test_integration.py -v
"""

import json
import os
import tempfile
import uuid
from pathlib import Path
from unittest.mock import patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

import app.models
from app.core.celery_app import celery_app
from app.core.config import get_settings
from app.core.database import Base, get_db
from app.main import app
from app.models.task import TaskStatus


# ── Skip guard ────────────────────────────────────────────────────────────────

def _postgres_available() -> bool:
    """Return True if PostgreSQL is reachable at the configured address."""
    import psycopg2
    s = get_settings()
    try:
        conn = psycopg2.connect(
            host=s.postgres_host,
            port=s.postgres_port,
            dbname=s.postgres_db,
            user=s.postgres_user,
            password=s.postgres_password,
            connect_timeout=3,
        )
        conn.close()
        return True
    except Exception:
        return False


requires_postgres = pytest.mark.skipif(
    not _postgres_available(),
    reason="PostgreSQL not reachable — run `docker-compose up postgres -d` first",
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest_asyncio.fixture(scope="function")
async def pg_session():
    """Create tables, yield a real async session, then drop tables after the test.

    Uses the same DB URL as the application so both the async FastAPI session
    and the sync Celery session see the same rows.
    """
    settings = get_settings()
    engine = create_async_engine(settings.database_url, pool_pre_ping=True)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    SessionFactory = async_sessionmaker(bind=engine, expire_on_commit=False, class_=AsyncSession)
    async with SessionFactory() as session:
        yield session

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()


@pytest_asyncio.fixture(scope="function")
async def client(pg_session):
    """AsyncClient wired to the real DB session and Celery eager mode."""

    async def override_get_db():
        yield pg_session

    app.dependency_overrides[get_db] = override_get_db

    # Run Celery tasks synchronously within the same process (no broker needed)
    celery_app.conf.update(task_always_eager=True, task_eager_propagates=True)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c

    app.dependency_overrides.clear()
    celery_app.conf.update(task_always_eager=False, task_eager_propagates=False)




# ── Helpers ───────────────────────────────────────────────────────────────────

VALID_DATASET = {
    "dataset_id": "ds_integration_001",
    "records": [
        {"id": "r1", "timestamp": "2026-01-01T00:00:00Z", "value": 10, "category": "A"},
        {"id": "r2", "timestamp": "2026-01-01T00:01:00Z", "value": 20, "category": "B"},
        {"id": "r3", "timestamp": "2026-01-01T00:02:00Z", "value": 30, "category": "A"},
    ],
}

FIXTURE_DIR = Path("tests/fixtures")


async def upload(client, data: dict, filename: str = "dataset.json") -> dict:
    """POST to /api/datasets/upload and return the parsed JSON body."""
    content = json.dumps(data).encode()
    resp = await client.post(
        "/api/datasets/upload",
        files={"file": (filename, content, "application/json")},
    )
    return resp.status_code, resp.json()


# ── Full pipeline: happy path ─────────────────────────────────────────────────

@requires_postgres
class TestFullPipelineHappyPath:
    @pytest.mark.asyncio
    async def test_upload_creates_pending_then_completes(self, client):
        """Full flow: upload → task created (PENDING) → worker runs → COMPLETED + result."""
        with patch("app.tasks.process_dataset.PROCESSING_DELAY_SECONDS", 0):
            status_code, body = await upload(client, VALID_DATASET)

        assert status_code == 202
        task_id = body["id"]

        # Because Celery ran eagerly (synchronously), by the time we get here
        # the task has already been fully processed. Fetch its current state.
        resp = await client.get(f"/api/tasks/{task_id}")
        assert resp.status_code == 200
        task = resp.json()

        assert task["status"] == TaskStatus.COMPLETED
        assert task["result"] is not None

    @pytest.mark.asyncio
    async def test_result_matches_expected_schema(self, client):
        with patch("app.tasks.process_dataset.PROCESSING_DELAY_SECONDS", 0):
            _, body = await upload(client, VALID_DATASET)

        resp = await client.get(f"/api/tasks/{body['id']}")
        result = resp.json()["result"]

        assert result["dataset_id"] == VALID_DATASET["dataset_id"]
        assert result["record_count"] == 3
        assert result["invalid_records"] == 0
        assert result["average_value"] == pytest.approx(20.0)
        assert result["category_summary"] == {"A": 2, "B": 1}

    @pytest.mark.asyncio
    async def test_task_visible_in_list(self, client):
        with patch("app.tasks.process_dataset.PROCESSING_DELAY_SECONDS", 0):
            _, body = await upload(client, VALID_DATASET)
        task_id = body["id"]

        tasks = (await client.get("/api/tasks/")).json()
        ids = {t["id"] for t in tasks}
        assert task_id in ids

    @pytest.mark.asyncio
    async def test_fixture_valid_dataset_end_to_end(self, client):
        content = FIXTURE_DIR.joinpath("valid_dataset.json").read_bytes()
        with patch("app.tasks.process_dataset.PROCESSING_DELAY_SECONDS", 0):
            resp = await client.post(
                "/api/datasets/upload",
                files={"file": ("valid_dataset.json", content, "application/json")},
            )
        assert resp.status_code == 202
        task = (await client.get(f"/api/tasks/{resp.json()['id']}")).json()
        assert task["status"] == TaskStatus.COMPLETED
        assert task["result"]["record_count"] == 10
        assert task["result"]["invalid_records"] == 0
        assert task["result"]["category_summary"] == {"A": 4, "B": 3, "C": 3}


# ── Full pipeline: edge cases ─────────────────────────────────────────────────

@requires_postgres
class TestFullPipelineEdgeCases:
    @pytest.mark.asyncio
    async def test_empty_records_completes_with_zero_stats(self, client):
        data = {"dataset_id": "ds_empty_int", "records": []}
        with patch("app.tasks.process_dataset.PROCESSING_DELAY_SECONDS", 0):
            _, body = await upload(client, data)

        task = (await client.get(f"/api/tasks/{body['id']}")).json()
        assert task["status"] == TaskStatus.COMPLETED
        assert task["result"]["record_count"] == 0
        assert task["result"]["invalid_records"] == 0
        assert task["result"]["average_value"] == 0.0
        assert task["result"]["category_summary"] == {}

    @pytest.mark.asyncio
    async def test_all_invalid_records_completes(self, client):
        content = FIXTURE_DIR.joinpath("all_invalid.json").read_bytes()
        with patch("app.tasks.process_dataset.PROCESSING_DELAY_SECONDS", 0):
            resp = await client.post(
                "/api/datasets/upload",
                files={"file": ("all_invalid.json", content, "application/json")},
            )
        task = (await client.get(f"/api/tasks/{resp.json()['id']}")).json()
        assert task["status"] == TaskStatus.COMPLETED
        assert task["result"]["invalid_records"] == task["result"]["record_count"]
        assert task["result"]["average_value"] == 0.0

    @pytest.mark.asyncio
    async def test_mixed_invalid_records(self, client):
        content = FIXTURE_DIR.joinpath("mixed_invalid.json").read_bytes()
        with patch("app.tasks.process_dataset.PROCESSING_DELAY_SECONDS", 0):
            resp = await client.post(
                "/api/datasets/upload",
                files={"file": ("mixed_invalid.json", content, "application/json")},
            )
        task = (await client.get(f"/api/tasks/{resp.json()['id']}")).json()
        assert task["status"] == TaskStatus.COMPLETED
        result = task["result"]
        assert result["record_count"] == 8
        assert result["invalid_records"] == 5
        assert result["average_value"] == 200.0

    @pytest.mark.asyncio
    async def test_malformed_json_rejected_before_task_created(self, client):
        """Structural errors return 400 and must NOT create a task row."""
        resp = await client.post(
            "/api/datasets/upload",
            files={"file": ("bad.json", b"{not valid json", "application/json")},
        )
        assert resp.status_code == 400
        # Confirm no task was created
        tasks = (await client.get("/api/tasks/")).json()
        assert tasks == []

    @pytest.mark.asyncio
    async def test_duplicate_dataset_id_creates_independent_tasks(self, client):
        """Same dataset_id can be submitted multiple times; each gets its own task."""
        with patch("app.tasks.process_dataset.PROCESSING_DELAY_SECONDS", 0):
            _, b1 = await upload(client, VALID_DATASET)
            _, b2 = await upload(client, VALID_DATASET)

        assert b1["id"] != b2["id"]

        tasks = (await client.get("/api/tasks/")).json()
        assert len(tasks) == 2
        assert all(t["status"] == TaskStatus.COMPLETED for t in tasks)


# ── Full pipeline: concurrency ────────────────────────────────────────────────

@requires_postgres
class TestConcurrency:
    @pytest.mark.asyncio
    async def test_five_concurrent_uploads_all_complete(self, client):
        """Submit 5 datasets in rapid succession; all must complete without loss."""
        datasets = [
            {
                "dataset_id": f"ds_concurrent_{i:03d}",
                "records": [
                    {"id": f"r{j}", "timestamp": "2026-01-01T00:00:00Z",
                     "value": i * 10 + j, "category": chr(65 + j % 3)}
                    for j in range(5)
                ],
            }
            for i in range(5)
        ]

        task_ids = []
        with patch("app.tasks.process_dataset.PROCESSING_DELAY_SECONDS", 0):
            for ds in datasets:
                code, body = await upload(client, ds)
                assert code == 202, f"Upload failed: {body}"
                task_ids.append(body["id"])

        # All 5 task IDs are unique
        assert len(set(task_ids)) == 5

        # All 5 tasks are COMPLETED with correct results
        for i, tid in enumerate(task_ids):
            task = (await client.get(f"/api/tasks/{tid}")).json()
            assert task["status"] == TaskStatus.COMPLETED, \
                f"Task {tid} has status {task['status']}: {task.get('error')}"
            assert task["result"]["dataset_id"] == f"ds_concurrent_{i:03d}"
            assert task["result"]["record_count"] == 5
            assert task["result"]["invalid_records"] == 0

    @pytest.mark.asyncio
    async def test_no_task_data_lost_or_overwritten(self, client):
        """Different datasets must produce different results — no overwriting."""
        datasets = [
            {"dataset_id": "ds_A", "records": [
                {"id": "r1", "timestamp": "t", "value": 100, "category": "X"}
            ]},
            {"dataset_id": "ds_B", "records": [
                {"id": "r1", "timestamp": "t", "value": 999, "category": "Z"},
                {"id": "r2", "timestamp": "t", "value": 1,   "category": "Z"},
            ]},
        ]

        task_ids = []
        with patch("app.tasks.process_dataset.PROCESSING_DELAY_SECONDS", 0):
            for ds in datasets:
                _, body = await upload(client, ds)
                task_ids.append(body["id"])

        results = [
            (await client.get(f"/api/tasks/{tid}")).json()["result"]
            for tid in task_ids
        ]

        assert results[0]["dataset_id"] == "ds_A"
        assert results[0]["average_value"] == 100.0
        assert results[0]["category_summary"] == {"X": 1}

        assert results[1]["dataset_id"] == "ds_B"
        assert results[1]["average_value"] == 500.0
        assert results[1]["category_summary"] == {"Z": 2}


# ── Full pipeline: failed task ────────────────────────────────────────────────

@requires_postgres
class TestFailedTask:
    @pytest.mark.asyncio
    async def test_missing_file_on_disk_marks_task_failed(self, client):
        """If the uploaded file is gone by the time the worker runs, task → FAILED.

        Simulates a race condition or storage error by uploading successfully
        (task row created, file saved) and then calling the Celery task directly
        with a nonexistent path, which triggers the OSError branch in the task.
        """
        # Step 1: upload normally to create a real Task row in the DB
        with patch("app.tasks.process_dataset.PROCESSING_DELAY_SECONDS", 0):
            code, body = await upload(client, VALID_DATASET)
        assert code == 202

        task_id = body["id"]

        # Step 2: call the task again directly with a bad file path.
        # task_always_eager=True so it runs synchronously and raises on failure.
        import app.tasks.process_dataset as ptd
        with patch.object(ptd, "PROCESSING_DELAY_SECONDS", 0):
            with pytest.raises(OSError):
                ptd.process_dataset_task.apply(
                    args=[task_id, "/nonexistent/deleted/file.json"]
                )

        # Step 3: the DB row must now show FAILED with an error message
        task = (await client.get(f"/api/tasks/{task_id}")).json()
        assert task["status"] == TaskStatus.FAILED
        assert task["error"] is not None
        assert "OSError" in task["error"] or "FileNotFoundError" in task["error"]
