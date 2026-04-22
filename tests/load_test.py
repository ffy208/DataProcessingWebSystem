"""
Concurrent load test — simulates N users each uploading M files simultaneously.

Usage:
    uv run python tests/load_test.py                  # default: 5 users * 3 files
    uv run python tests/load_test.py --users 20 --files 5
    uv run python tests/load_test.py --users 50 --files 1 --host http://localhost:8000
"""

import argparse
import asyncio
import json
import random
import time
from pathlib import Path

import httpx

BASE_URL = "http://localhost:8000"
FIXTURE_DIR = Path("tests/manual")

# Only valid-structure files (will complete successfully)
VALID_FILES = [
    "valid_small.json",
    "valid_large.json",
    "valid_floats.json",
    "valid_negative_values.json",
    "valid_many_categories.json",
    "valid_single_record.json",
    "valid_zero_values.json",
    "edge_empty_records.json",
    "edge_duplicate_ids.json",
    "mixed_half_invalid.json",
    "mixed_nan_inf.json",
    "mixed_wrong_types.json",
    "mixed_missing_fields.json",
    "all_invalid_records.json",
]


async def upload_one(client: httpx.AsyncClient, user_id: int, file_name: str) -> dict:
    path = FIXTURE_DIR / file_name
    content = path.read_bytes()
    start = time.perf_counter()
    try:
        resp = await client.post(
            f"{BASE_URL}/api/datasets/upload",
            files={"file": (file_name, content, "application/json")},
            timeout=30,
        )
        elapsed = time.perf_counter() - start
        if resp.status_code == 202:
            return {"user": user_id, "file": file_name, "status": "queued",
                    "task_id": resp.json()["id"], "ms": round(elapsed * 1000)}
        else:
            return {"user": user_id, "file": file_name, "status": f"error_{resp.status_code}",
                    "detail": resp.json().get("detail", ""), "ms": round(elapsed * 1000)}
    except Exception as e:
        elapsed = time.perf_counter() - start
        return {"user": user_id, "file": file_name, "status": "exception",
                "detail": str(e), "ms": round(elapsed * 1000)}


async def simulate_user(user_id: int, files: list[str]) -> list[dict]:
    async with httpx.AsyncClient() as client:
        tasks = [upload_one(client, user_id, f) for f in files]
        return await asyncio.gather(*tasks)


async def wait_for_completion(task_ids: list[str], timeout: int = 120) -> dict:
    """Poll until all tasks reach a terminal state or timeout."""
    deadline = time.time() + timeout
    pending = set(task_ids)
    results = {}

    async with httpx.AsyncClient() as client:
        while pending and time.time() < deadline:
            await asyncio.sleep(2)
            for tid in list(pending):
                try:
                    r = await client.get(f"{BASE_URL}/api/tasks/{tid}", timeout=5)
                    if r.status_code == 200:
                        task = r.json()
                        if task["status"] in ("COMPLETED", "FAILED"):
                            results[tid] = task["status"]
                            pending.discard(tid)
                except Exception:
                    pass

    for tid in pending:
        results[tid] = "TIMEOUT"
    return results


TASK_DELAY_SECONDS = 15   # mirrors PROCESSING_DELAY_SECONDS in the worker
DEFAULT_CONCURRENCY = 4   # worker --concurrency default


def _auto_timeout(num_tasks: int) -> int:
    """Calculate a generous timeout based on task count.

    Assumes 1 worker with DEFAULT_CONCURRENCY slots and adds 50% buffer.
    Minimum 60s, no upper cap (user controls this).
    """
    rounds = (num_tasks + DEFAULT_CONCURRENCY - 1) // DEFAULT_CONCURRENCY
    return max(60, int(rounds * TASK_DELAY_SECONDS * 1.5))


async def run(num_users: int, files_per_user: int, host: str, timeout: int | None):
    global BASE_URL
    BASE_URL = host

    num_tasks = num_users * files_per_user
    effective_timeout = timeout if timeout is not None else _auto_timeout(num_tasks)

    print(f"\n{'─'*60}")
    print(f"  Load test: {num_users} users * {files_per_user} files = {num_tasks} total requests")
    print(f"  Target  : {BASE_URL}")
    print(f"  Timeout : {effective_timeout}s {'(auto)' if timeout is None else '(manual)'}")
    print(f"{'─'*60}\n")

    # Each user gets a random selection of files
    user_file_lists = [
        random.choices(VALID_FILES, k=files_per_user)
        for _ in range(num_users)
    ]

    # All users fire at the same time
    t0 = time.perf_counter()
    user_results = await asyncio.gather(*[
        simulate_user(uid, files)
        for uid, files in enumerate(user_file_lists, 1)
    ])
    upload_elapsed = time.perf_counter() - t0

    # Flatten results
    all_uploads = [r for user in user_results for r in user]
    queued   = [r for r in all_uploads if r["status"] == "queued"]
    failed   = [r for r in all_uploads if r["status"] != "queued"]
    task_ids = [r["task_id"] for r in queued]

    print(f"Upload phase complete in {upload_elapsed:.2f}s")
    print(f"  Queued  : {len(queued)}")
    print(f"  Rejected: {len(failed)}")
    if failed:
        for f in failed[:5]:
            print(f"    user={f['user']} file={f['file']} → {f['status']} {f.get('detail','')}")

    latencies = [r["ms"] for r in queued]
    if latencies:
        print(f"  Upload latency — min: {min(latencies)}ms  avg: {sum(latencies)//len(latencies)}ms  max: {max(latencies)}ms")

    if not task_ids:
        print("\nNo tasks to wait for. Done.")
        return

    # Wait for workers to finish
    print(f"\nWaiting for {len(task_ids)} tasks to complete (up to {effective_timeout}s)…")
    t1 = time.perf_counter()
    final = await wait_for_completion(task_ids, timeout=effective_timeout)
    wait_elapsed = time.perf_counter() - t1

    completed = sum(1 for s in final.values() if s == "COMPLETED")
    task_failed = sum(1 for s in final.values() if s == "FAILED")
    timed_out  = sum(1 for s in final.values() if s == "TIMEOUT")

    print(f"\nProcessing phase complete in {wait_elapsed:.2f}s")
    print(f"  COMPLETED : {completed}")
    print(f"  FAILED    : {task_failed}")
    print(f"  TIMEOUT   : {timed_out}")
    print(f"\nTotal wall time: {upload_elapsed + wait_elapsed:.2f}s")
    print(f"{'─'*60}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--users",   type=int,  default=5,    help="Number of concurrent users")
    parser.add_argument("--files",   type=int,  default=3,    help="Files per user")
    parser.add_argument("--host",    type=str,  default="http://localhost:8000")
    parser.add_argument("--timeout", type=int,  default=None,
                        help="Max seconds to wait for tasks (default: auto-calculated)")
    args = parser.parse_args()

    asyncio.run(run(args.users, args.files, args.host, args.timeout))
