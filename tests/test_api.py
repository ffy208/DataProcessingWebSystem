"""API integration tests — no running PostgreSQL or Redis required.

Strategy:
- Override FastAPI's get_db dependency with an in-memory async SQLite session
  (SQLAlchemy supports SQLite via aiosqlite; model columns are dialect-agnostic
  except UUID which is mapped to String for SQLite).
- Patch process_dataset_task.delay so no Celery broker is needed.
- Use httpx.AsyncClient with ASGITransport to test the full FastAPI request cycle.
"""

import json
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy import event

from app.core.database import Base, get_db
from app.main import app
from app.models.task import Task, TaskStatus


# ── Test database setup ───────────────────────────────────────────────────────

TEST_DB_URL = "sqlite+aiosqlite:///:memory:"


@pytest_asyncio.fixture(scope="function")
async def db_session():
    """Yield a fresh in-memory SQLite session for each test."""
    engine = create_async_engine(TEST_DB_URL, connect_args={"check_same_thread": False})

    # SQLite doesn't support UUID natively; remap at connection level
    @event.listens_for(engine.sync_engine, "connect")
    def set_sqlite_pragma(conn, _):
        conn.execute("PRAGMA journal_mode=WAL")

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session_factory = async_sessionmaker(bind=engine, expire_on_commit=False, class_=AsyncSession)

    async with session_factory() as session:
        yield session

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()


@pytest_asyncio.fixture(scope="function")
async def client(db_session):
    """Yield an AsyncClient with the DB dependency overridden."""
    async def override_get_db():
        yield db_session

    app.dependency_overrides[get_db] = override_get_db
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c
    app.dependency_overrides.clear()


# ── Helpers ───────────────────────────────────────────────────────────────────

VALID_DATASET = {
    "dataset_id": "ds_test_001",
    "records": [
        {"id": "r1", "timestamp": "2026-01-01T00:00:00Z", "value": 10, "category": "A"},
        {"id": "r2", "timestamp": "2026-01-01T00:01:00Z", "value": 20, "category": "B"},
    ],
}

FIXTURE_DIR = Path("tests/fixtures")


def make_upload_files(data: dict, filename="dataset.json"):
    content = json.dumps(data).encode()
    return {"file": (filename, content, "application/json")}


# ── POST /api/datasets/upload ─────────────────────────────────────────────────

class TestUploadDataset:
    @pytest.mark.asyncio
    async def test_upload_valid_file_returns_202(self, client):
        with patch("app.api.routes.datasets.process_dataset_task") as mock_task:
            mock_task.delay = MagicMock()
            resp = await client.post("/api/datasets/upload", files=make_upload_files(VALID_DATASET))

        assert resp.status_code == 202
        body = resp.json()
        assert body["status"] == "PENDING"
        assert body["dataset_id"] == "ds_test_001"
        assert "id" in body
        mock_task.delay.assert_called_once()

    @pytest.mark.asyncio
    async def test_upload_returns_task_id(self, client):
        with patch("app.api.routes.datasets.process_dataset_task") as mock_task:
            mock_task.delay = MagicMock()
            resp = await client.post("/api/datasets/upload", files=make_upload_files(VALID_DATASET))

        task_id = resp.json()["id"]
        # Must be a valid UUID
        uuid.UUID(task_id)

    @pytest.mark.asyncio
    async def test_upload_invalid_json_returns_400(self, client):
        files = {"file": ("bad.json", b"not valid json {{", "application/json")}
        resp = await client.post("/api/datasets/upload", files=files)
        assert resp.status_code == 400
        assert "JSON" in resp.json()["detail"]

    @pytest.mark.asyncio
    async def test_upload_missing_dataset_id_returns_400(self, client):
        data = {"records": []}
        resp = await client.post("/api/datasets/upload", files=make_upload_files(data))
        assert resp.status_code == 400
        assert "dataset_id" in resp.json()["detail"]

    @pytest.mark.asyncio
    async def test_upload_missing_records_returns_400(self, client):
        data = {"dataset_id": "ds_001"}
        resp = await client.post("/api/datasets/upload", files=make_upload_files(data))
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_upload_list_top_level_returns_400(self, client):
        files = {"file": ("bad.json", b"[1,2,3]", "application/json")}
        resp = await client.post("/api/datasets/upload", files=files)
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_upload_empty_file_returns_400(self, client):
        files = {"file": ("empty.json", b"", "application/json")}
        resp = await client.post("/api/datasets/upload", files=files)
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_upload_duplicate_dataset_id_creates_separate_tasks(self, client):
        with patch("app.api.routes.datasets.process_dataset_task") as mock_task:
            mock_task.delay = MagicMock()
            r1 = await client.post("/api/datasets/upload", files=make_upload_files(VALID_DATASET))
            r2 = await client.post("/api/datasets/upload", files=make_upload_files(VALID_DATASET))

        assert r1.status_code == 202
        assert r2.status_code == 202
        assert r1.json()["id"] != r2.json()["id"]
        assert mock_task.delay.call_count == 2

    @pytest.mark.asyncio
    async def test_upload_from_fixture(self, client):
        content = FIXTURE_DIR.joinpath("valid_dataset.json").read_bytes()
        with patch("app.api.routes.datasets.process_dataset_task") as mock_task:
            mock_task.delay = MagicMock()
            resp = await client.post(
                "/api/datasets/upload",
                files={"file": ("valid_dataset.json", content, "application/json")},
            )
        assert resp.status_code == 202
        assert resp.json()["dataset_id"] == "ds_demo_001"


# ── GET /api/tasks/ ───────────────────────────────────────────────────────────

class TestListTasks:
    @pytest.mark.asyncio
    async def test_empty_list(self, client):
        resp = await client.get("/api/tasks/")
        assert resp.status_code == 200
        assert resp.json() == []

    @pytest.mark.asyncio
    async def test_returns_uploaded_tasks(self, client):
        with patch("app.api.routes.datasets.process_dataset_task") as mock_task:
            mock_task.delay = MagicMock()
            await client.post("/api/datasets/upload", files=make_upload_files(VALID_DATASET))

        resp = await client.get("/api/tasks/")
        assert resp.status_code == 200
        tasks = resp.json()
        assert len(tasks) == 1
        assert tasks[0]["status"] == "PENDING"

    @pytest.mark.asyncio
    async def test_multiple_uploads_all_listed(self, client):
        with patch("app.api.routes.datasets.process_dataset_task") as mock_task:
            mock_task.delay = MagicMock()
            for i in range(3):
                d = {**VALID_DATASET, "dataset_id": f"ds_{i}"}
                await client.post("/api/datasets/upload", files=make_upload_files(d))

        resp = await client.get("/api/tasks/")
        assert len(resp.json()) == 3


# ── GET /api/tasks/{task_id} ──────────────────────────────────────────────────

class TestGetTask:
    @pytest.mark.asyncio
    async def test_get_existing_task(self, client):
        with patch("app.api.routes.datasets.process_dataset_task") as mock_task:
            mock_task.delay = MagicMock()
            upload = await client.post("/api/datasets/upload", files=make_upload_files(VALID_DATASET))

        task_id = upload.json()["id"]
        resp = await client.get(f"/api/tasks/{task_id}")
        assert resp.status_code == 200
        assert resp.json()["id"] == task_id
        assert resp.json()["status"] == "PENDING"

    @pytest.mark.asyncio
    async def test_get_nonexistent_task_returns_404(self, client):
        fake_id = str(uuid.uuid4())
        resp = await client.get(f"/api/tasks/{fake_id}")
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_get_invalid_uuid_returns_422(self, client):
        resp = await client.get("/api/tasks/not-a-uuid")
        assert resp.status_code == 422


# ── Concurrency scenario (serial simulation) ─────────────────────────────────

class TestConcurrentUploads:
    @pytest.mark.asyncio
    async def test_five_simultaneous_uploads_all_queued(self, client):
        """Simulate 5 rapid uploads and verify all create independent PENDING tasks."""
        task_ids = []
        with patch("app.api.routes.datasets.process_dataset_task") as mock_task:
            mock_task.delay = MagicMock()
            for i in range(5):
                data = {
                    "dataset_id": f"ds_concurrent_{i:03d}",
                    "records": [{"id": "r1", "timestamp": "t", "value": i * 10, "category": "X"}],
                }
                resp = await client.post("/api/datasets/upload", files=make_upload_files(data))
                assert resp.status_code == 202, f"Upload {i} failed: {resp.text}"
                task_ids.append(resp.json()["id"])

        # All 5 tasks enqueued
        assert mock_task.delay.call_count == 5
        # All task IDs are unique
        assert len(set(task_ids)) == 5

        # All visible in task list
        resp = await client.get("/api/tasks/")
        listed_ids = {t["id"] for t in resp.json()}
        for tid in task_ids:
            assert tid in listed_ids
