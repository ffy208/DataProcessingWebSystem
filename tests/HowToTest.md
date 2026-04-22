# How To Test

This project has four layers of tests. Run them from the project root directory.


## Prerequisites

```bash
# Install dependencies (if not already done)
uv sync

# For integration tests: start PostgreSQL only (no api container needed)
docker compose up postgres redis -d
# No migration needed — integration test fixtures create/drop tables automatically
```


## 1. Unit Tests — Pure Logic (no I/O)

Tests the processing and validation functions in isolation. No database, no network, no Docker required.

```bash
uv run pytest tests/test_processor.py tests/test_validator.py -v
```

**What is tested:**
- `test_processor.py` (30 tests) — `process_dataset()` and `_is_valid_record()`:
  - Valid int / float / zero / negative values
  - Invalid: string value, bool, null, NaN, Infinity, missing fields
  - Edge cases: empty records, all-invalid, mixed, no division-by-zero
- `test_validator.py` (16 tests) — `validate_dataset()`:
  - Missing `dataset_id` / `records` key
  - Non-string `dataset_id`, empty `dataset_id`
  - Top-level array, non-list `records`

**Expected result:** 46 passed


## 2. API Integration Tests — FastAPI + SQLite (no Docker)

Tests the full HTTP request/response cycle using an in-memory SQLite database and a mocked Celery task queue. No Docker required.

```bash
uv run pytest tests/test_api.py -v
```

**What is tested:**
- `POST /api/datasets/upload` returns 202 with a valid UUID task ID
- `POST /api/datasets/upload` returns 400 for malformed JSON, missing fields, empty file, top-level array
- `GET /api/tasks/` returns empty list or all uploaded tasks
- `GET /api/tasks/{id}` returns the task or 404 for unknown ID
- `GET /api/tasks/not-a-uuid` returns 422
- Duplicate `dataset_id` creates two independent tasks
- 5 concurrent uploads all create distinct pending tasks

**Expected result:** 16 passed


## 3. End-to-End Integration Tests — Real PostgreSQL + Celery (requires Docker)

Tests the complete pipeline without any mocks: upload → DB write → Celery task runs → DB updated → API read.

```bash
# Start PostgreSQL first
docker compose up postgres -d

uv run pytest tests/test_integration.py -v
```

If PostgreSQL is unreachable, these tests are automatically skipped (not failed).

**What is tested:**
- Full flow: upload → COMPLETED with correct result schema
- `record_count`, `average_value`, `category_summary`, `invalid_records` match expected values
- Task appears in list endpoint after upload
- Fixture file `valid_dataset.json` produces correct statistics
- Empty records → `average_value: 0.0`, `category_summary: {}`
- All-invalid records → `average_value: 0.0`, `invalid_records == record_count`
- Mixed invalid records → only valid records contribute to average
- Malformed JSON returns 400 and creates NO task row in DB
- Same `dataset_id` submitted twice → two independent tasks, both COMPLETED
- 5 concurrent uploads → all COMPLETED, no data overwriting between tasks
- Missing file on disk → task status becomes FAILED with OSError message

**Expected result:** 12 passed


## 4. Run All Automated Tests

```bash
# All 74 tests at once
uv run pytest -v
```

**Expected result:** 74 passed (or 62 passed + 12 skipped if PostgreSQL is not running)


## 5. Load Test — Concurrent Users (requires full Docker stack)

Simulates multiple users uploading files simultaneously and measures API latency and worker throughput.

```bash
# Start the full stack first
docker compose up --build -d
docker compose exec api uv run alembic upgrade head

# Default: 5 users × 3 files = 15 concurrent requests
uv run python tests/load_test.py

# Moderate load: 20 users × 5 files = 100 requests
uv run python tests/load_test.py --users 20 --files 5

# Stress test: 50 concurrent single-file uploads
uv run python tests/load_test.py --users 50 --files 1

# Scale workers before stress test for faster completion
docker compose up --scale worker=3 -d
uv run python tests/load_test.py --users 50 --files 1
```

**Output explained:**
- `Queued` — uploads accepted (HTTP 202), task created in DB
- `Rejected` — uploads refused (HTTP 400/413/500), no task created
- `Upload latency` — time from request to 202 response (measures API throughput)
- `COMPLETED / FAILED / TIMEOUT` — final task states after workers finish
- `TIMEOUT` means the script's 120-second polling window expired; those tasks are still running and will eventually complete — check with `curl http://localhost:8000/api/tasks/`

**Concurrency math:**
```
N workers × 4 threads = concurrent task slots
100 tasks ÷ slots × 15s = expected total time

Example: 3 workers × 4 = 12 slots → 100 tasks ÷ 12 × 15s ≈ 125s
```


## 6. Manual UI Testing

```bash
docker compose up --build -d
docker compose exec api uv run alembic upgrade head
open http://localhost:8000
```

**Test scenarios using `tests/manual/` files:**

| File | Expected result |
|------|----------------|
| `valid_small.json` | COMPLETED, record_count=3, invalid=0 |
| `valid_large.json` | COMPLETED, record_count=30, 4 categories |
| `valid_floats.json` | COMPLETED, avg≈2001.45 (5 different categories) |
| `valid_negative_values.json` | COMPLETED, avg=-32.1 (negative average is valid) |
| `valid_zero_values.json` | COMPLETED, avg=0.0, all "Zero" category |
| `valid_single_record.json` | COMPLETED, record_count=1 |
| `valid_many_categories.json` | COMPLETED, 9 distinct categories in summary |
| `edge_empty_records.json` | COMPLETED, record_count=0, avg=0.0, summary={} |
| `edge_duplicate_ids.json` | COMPLETED, all 3 records processed (duplicate record IDs allowed) |
| `edge_same_dataset_id.json` | COMPLETED, creates a new task — same dataset_id is allowed |
| `mixed_half_invalid.json` | COMPLETED, invalid=5, avg only from 3 valid records |
| `mixed_nan_inf.json` | COMPLETED, "NaN"/"Inf" strings treated as invalid |
| `mixed_wrong_types.json` | COMPLETED, bool/null/array/object values are invalid |
| `mixed_missing_fields.json` | COMPLETED, records missing any required field are invalid |
| `all_invalid_records.json` | COMPLETED, invalid=record_count, avg=0.0 |
| `invalid_missing_dataset_id.json` | **400** — no task created |
| `invalid_missing_records_key.json` | **400** — no task created |
| `invalid_top_level_array.json` | **400** — no task created |
| `invalid_empty_dataset_id.json` | **400** — no task created |
| `invalid_records_is_not_list.json` | **400** — no task created |
| `invalid_malformed_syntax.json` | **400** — no task created |
| `invalid_dataset_id_wrong_type.json` | **400** — dataset_id must be a string |

**Bulk upload:** Select all files in `tests/manual/` (Cmd+A) and drag them into the drop zone at once to test concurrent multi-file upload.


## 7. Manual API Testing with curl

```bash
# Upload a file
curl -X POST http://localhost:8000/api/datasets/upload \
  -F "file=@tests/manual/valid_small.json"

# List all tasks
curl http://localhost:8000/api/tasks/

# Get a specific task (replace UUID)
curl http://localhost:8000/api/tasks/<task-id>

# Summarise all task statuses
curl -s http://localhost:8000/api/tasks/ | python3 -c "
import json,sys
from collections import Counter
tasks = json.load(sys.stdin)
print(f'Total: {len(tasks)}')
for s,n in sorted(Counter(t[\"status\"] for t in tasks).items()):
    print(f'  {s}: {n}')
"

# View results table
curl -s http://localhost:8000/api/tasks/ | python3 -c "
import json,sys
tasks = json.load(sys.stdin)
print(f'{\"dataset_id\":<35} {\"status\":<12} {\"total\":>6} {\"invalid\":>8} {\"avg\":>10}')
print('-'*75)
for t in tasks:
    r = t.get('result') or {}
    print(f\"{t['dataset_id']:<35} {t['status']:<12} {r.get('record_count',''):>6} {r.get('invalid_records',''):>8} {r.get('average_value',''):>10}\")
"

# Interactive Swagger UI
open http://localhost:8000/docs
```
