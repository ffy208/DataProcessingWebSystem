"""Shared pytest fixtures for all test suites."""

import os
import pytest
from app.core.config import get_settings


@pytest.fixture(autouse=True)
def temp_upload_dir(tmp_path):
    """Redirect UPLOAD_DIR to a per-test temp directory.

    Prevents tests from writing to /app/uploads (Docker path) or polluting
    the local uploads/ directory. Clears the lru_cache so Settings re-reads
    the patched env var, then restores the original value after each test.
    """
    original = os.environ.get("UPLOAD_DIR")
    os.environ["UPLOAD_DIR"] = str(tmp_path / "uploads")
    get_settings.cache_clear()

    yield

    if original is None:
        os.environ.pop("UPLOAD_DIR", None)
    else:
        os.environ["UPLOAD_DIR"] = original
    get_settings.cache_clear()
