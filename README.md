# Data Processing Web System

An async data-processing pipeline with a web UI. Upload one or more JSON datasets, watch them move through background workers, and inspect the computed statistics — all without page refreshes.

**Stack:** FastAPI · Celery · Redis · PostgreSQL · Docker Compose · uv


## Architecture

```
Browser
  │  POST /api/datasets/upload (multipart, supports batch drag-and-drop)
  │  GET  /api/tasks/            (poll every 2 s)
  ▼
FastAPI (api)          ──── Redis ────  Celery Worker (worker)
  │  writes Task(PENDING)              │  PENDING → RUNNING → COMPLETED / FAILED
  │  saves file to /app/uploads        │  reads file, validates, processes
  ▼                                    ▼
PostgreSQL ◄───────────────────────────┘
  tasks table: id, dataset_id, status, result, error, …
```

Both `api` and `worker` containers share the `uploads_data` Docker volume so the worker can always read the file the API saved.


## Prerequisites

Docker and Docker Compose are required. If not installed:

- **Mac**: Download [Docker Desktop for Mac](https://docs.docker.com/desktop/install/mac-install/) and install the `.dmg`. Docker Compose is included.
- **Windows**: Download [Docker Desktop for Windows](https://docs.docker.com/desktop/install/windows-install/).
- **Linux**:
  ```bash
  curl -fsSL https://get.docker.com | sh
  sudo usermod -aG docker $USER   # re-login after this
  ```

Verify the installation:

```bash
docker --version
docker compose version
```


## Quick Start (Docker)

```bash
# 1. Clone and enter the project
git clone https://github.com/ffy208/DataProcessingWebSystem.git
cd DataProcessingWebSystem

# 2. Copy environment file
cp .env.example .env          # defaults work out-of-the-box

# 3. Start all services
docker compose up --build -d

# 4. Run database migrations
docker compose exec api uv run alembic upgrade head

# 5. Open the UI
open http://localhost:8000
```

Services exposed to the host:


| Service          | Port |
| ---------------- | ---- |
| FastAPI API + UI | 8000 |
| PostgreSQL       | 5432 |
| Redis            | 6379 |

### Stopping the Application

```bash
# Stop all containers but keep data (can restart without re-running migrations)
docker compose down

# Stop and delete all data volumes (full reset — migrations must be re-run)
docker compose down -v
```

## Design Decisions

### Celery + Redis over asyncio background tasks

FastAPI's `BackgroundTasks` dies with the process. Celery tasks survive restarts, support retries, and can be distributed across multiple workers without code changes. `task_acks_late=True` combined with `reject_on_worker_lost=True` ensures a task is re-queued if the worker crashes mid-execution rather than silently dropped.

### PostgreSQL over SQLite

Multiple Celery workers write to the same database concurrently. SQLite's write lock would serialize all workers; PostgreSQL handles concurrent writes safely and provides real UUID column types and JSONB storage for the result.

### Async FastAPI + sync Celery worker

FastAPI uses `asyncpg` for non-blocking database I/O. Celery workers run in forked OS processes and cannot use an async engine, so the worker uses a separate `psycopg2`-based sync session factory. The engine is a **per-process singleton** (`pool_size=1, max_overflow=0`) so that N worker processes create exactly N connections — preventing connection pool exhaustion under load.

### File saved before task enqueue

The uploaded file is written to disk **before** `process_dataset_task.delay()` is called. This guarantees the file exists on the shared volume when the worker starts, even if the broker queues the task and delivers it seconds later.

### Alpine.js polling over WebSocket

WebSocket would require managing connection state, reconnect logic, and a separate protocol. Polling every 2 seconds is simpler, works through any proxy, and is more than sufficient for tasks that complete within 15–30 seconds.

### uv for dependency management

`uv` resolves and locks the full dependency graph deterministically. `uv sync --frozen` in the Dockerfile ensures the container image always uses exactly the versions in `uv.lock`.


## Dataset Format

Upload a JSON file with this structure:

```json
{
  "dataset_id": "my_dataset_001",
  "records": [
    {
      "id": "r1",
      "timestamp": "2026-01-01T10:00:00Z",
      "value": 42.5,
      "category": "A"
    }
  ]
}
```

**Required fields per record:** `id`, `timestamp`, `value` (numeric, not NaN/Infinity), `category`.

Records missing any field, having a non-numeric `value`, or containing `NaN`/`Infinity` are counted as `invalid_records` and excluded from `average_value`.

**Constraints:** JSON file only, max 10 MB.

### Sample Files

22 ready-to-upload test files are in `tests/manual/`:


| File                              | Expected Result                               |
| --------------------------------- | --------------------------------------------- |
| `valid_small.json`                | COMPLETED, record_count=3, invalid=0          |
| `valid_large.json`                | COMPLETED, record_count=30, 4 categories      |
| `valid_floats.json`               | COMPLETED, avg≈2001.45, 5 categories         |
| `valid_negative_values.json`      | COMPLETED, avg=-32.1                          |
| `valid_zero_values.json`          | COMPLETED, avg=0.0                            |
| `valid_single_record.json`        | COMPLETED, record_count=1                     |
| `valid_many_categories.json`      | COMPLETED, 9 distinct categories              |
| `edge_empty_records.json`         | COMPLETED, record_count=0, avg=0.0            |
| `edge_duplicate_ids.json`         | COMPLETED, duplicate record IDs allowed       |
| `mixed_half_invalid.json`         | COMPLETED, invalid=5, avg from valid only     |
| `mixed_nan_inf.json`              | COMPLETED, NaN/Inf strings treated as invalid |
| `mixed_wrong_types.json`          | COMPLETED, bool/null/array values are invalid |
| `all_invalid_records.json`        | COMPLETED, invalid=record_count, avg=0.0      |
| `invalid_missing_dataset_id.json` | **400** — no task created                    |
| `invalid_malformed_syntax.json`   | **400** — no task created                    |
| *(+ 7 more invalid/edge cases)*   | **400** — no task created                    |


## API Reference

### Interactive Docs (Swagger UI)

FastAPI generates an interactive API explorer automatically. Once the stack is running, open:

```
http://localhost:8000/docs
```

You can try every endpoint directly in the browser — upload a file, copy the returned task ID, then call `GET /api/tasks/{task_id}` to watch the status change. No `curl` or Postman needed.

A read-only OpenAPI schema is also available at `http://localhost:8000/openapi.json`.


### `POST /api/datasets/upload`

Upload a JSON dataset. Returns immediately with a task ID; processing happens asynchronously.

**Request:** `multipart/form-data`, field name `file`.

**Response `202`:**

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "dataset_id": "my_dataset_001",
  "filename": "my_dataset_001.json",
  "status": "PENDING",
  "result": null,
  "error": null,
  "created_at": "2026-04-22T10:00:00Z",
  "updated_at": "2026-04-22T10:00:00Z"
}
```

**Error responses:**


| Code | Cause                                                      |
| ---- | ---------------------------------------------------------- |
| 400  | Malformed JSON, missing`dataset_id`, missing `records` key |
| 413  | File exceeds 10 MB                                         |
| 422  | No file attached                                           |

### `GET /api/tasks/`

List all tasks, newest first.

**Response `200`:** Array of task objects (same schema as above).


### `GET /api/tasks/{task_id}`

Get a single task by UUID.

**Response `200`** when `status` is `COMPLETED`:

```json
{
  "id": "550e8400-...",
  "status": "COMPLETED",
  "result": {
    "dataset_id": "my_dataset_001",
    "record_count": 10,
    "invalid_records": 2,
    "average_value": 32.05,
    "category_summary": { "A": 4, "B": 3, "C": 3 }
  }
}
```

**Response `200`** when `status` is `FAILED`:

```json
{
  "status": "FAILED",
  "error": "OSError: [Errno 2] No such file or directory: '...'"
}
```


| Code | Cause                  |
| ---- | ---------------------- |
| 404  | Task ID not found      |
| 422  | ID is not a valid UUID |


## Running Tests

See [`tests/HowToTest.md`](tests/HowToTest.md) for the full guide. Quick reference:

```bash
# Unit tests — no external services needed
uv run pytest tests/test_processor.py tests/test_validator.py -v

# API integration tests — in-memory SQLite, no Docker needed
uv run pytest tests/test_api.py -v

# End-to-end integration tests — requires PostgreSQL
docker compose up postgres -d
uv run pytest tests/test_integration.py -v

# All 74 tests at once
uv run pytest -v
```

Integration tests are **auto-skipped** when PostgreSQL is unreachable.

### Test Breakdown


| Suite                 | Tests  | Strategy                               |
| --------------------- | ------ | -------------------------------------- |
| `test_processor.py`   | 30     | Pure function unit tests — no I/O     |
| `test_validator.py`   | 16     | Pure function unit tests — no I/O     |
| `test_api.py`         | 16     | SQLite in-memory DB + mocked Celery    |
| `test_integration.py` | 12     | Real PostgreSQL + Celery`always_eager` |
| **Total**             | **74** |                                        |

### Load Test

Simulates N users uploading M files concurrently against the live Docker stack:

```bash
docker compose up --build -d
docker compose exec api uv run alembic upgrade head

# Default: 5 users × 3 files = 15 concurrent requests
uv run python tests/load_test.py

# Stress test: 50 concurrent single-file uploads
uv run python tests/load_test.py --users 50 --files 1

# Scale workers before stress test
docker compose up --scale worker=3 -d
uv run python tests/load_test.py --users 50 --files 1
```

The timeout is auto-calculated based on task count and worker concurrency. Pass `--timeout <seconds>` to override.



## Environment Variables


| Variable            | Default          | Description                  |
| ------------------- | ---------------- | ---------------------------- |
| `POSTGRES_HOST`     | `localhost`      | PostgreSQL hostname          |
| `POSTGRES_PORT`     | `5432`           | PostgreSQL port              |
| `POSTGRES_DB`       | `dataprocessing` | Database name                |
| `POSTGRES_USER`     | `app`            | Database user                |
| `POSTGRES_PASSWORD` | `secret`         | Database password            |
| `REDIS_HOST`        | `localhost`      | Redis hostname               |
| `REDIS_PORT`        | `6379`           | Redis port                   |
| `UPLOAD_DIR`        | `uploads`        | Directory for uploaded files |

In Docker Compose, `POSTGRES_HOST=postgres` and `REDIS_HOST=redis` are set via the `environment:` block in `docker-compose.yml` and override `.env` automatically.


## Project Structure

```
app/
├── api/routes/
│   ├── datasets.py         # POST /api/datasets/upload
│   └── tasks.py            # GET /api/tasks/, GET /api/tasks/{id}
├── core/
│   ├── config.py           # pydantic-settings, DB + Redis URLs
│   ├── database.py         # async SQLAlchemy engine + get_db()
│   └── celery_app.py       # Celery instance with crash-safe config
├── crud/task.py            # create / get / list / update_status
├── models/task.py          # Task ORM model + TaskStatus enum
├── schemas/task.py         # Pydantic request/response models
├── services/
│   ├── validator.py        # Structural JSON validation
│   └── processor.py        # record_count / category_summary / average_value
├── tasks/
│   └── process_dataset.py  # Celery task, per-process DB singleton
├── static/index.html       # Single-page UI (Alpine.js + Tailwind CDN)
└── main.py                 # FastAPI app factory + CORS + static files

alembic/
├── versions/
│   └── 0001_create_tasks_table.py  # Creates tasks table and task_status enum
└── env.py

tests/
├── fixtures/               # 4 JSON files used by test_integration.py
├── manual/                 # 22 sample JSON files for UI and load testing
├── conftest.py             # autouse fixture: redirects UPLOAD_DIR to tmp_path
├── test_processor.py       # 30 unit tests
├── test_validator.py       # 16 unit tests
├── test_api.py             # 16 API integration tests
├── test_integration.py     # 12 end-to-end tests
├── load_test.py            # Concurrent load test (httpx + asyncio)
└── HowToTest.md            # Full testing guide
```
