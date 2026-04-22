"""Core dataset processing logic (pure function, no I/O)."""

import math
from typing import Any


REQUIRED_FIELDS = {"id", "timestamp", "value", "category"}


def _is_valid_record(record: Any) -> bool:
    """Return True if a record has all required fields with acceptable types.

    A record is invalid if:
    - It is not a dict
    - Any of 'id', 'timestamp', 'value', 'category' is missing
    - 'value' is not a finite real number:
        * bool is excluded (bool is int subclass but semantically wrong)
        * NaN and Infinity are excluded (math.isfinite) — they would corrupt
          the running sum and produce non-serialisable JSON output

    Args:
        record: A single element from the 'records' list.
    """
    if not isinstance(record, dict):
        return False
    if not REQUIRED_FIELDS.issubset(record.keys()):
        return False
    value = record["value"]
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    if not math.isfinite(value):
        return False
    return True


def process_dataset(data: dict) -> dict:
    """Compute summary statistics for a validated dataset dict.

    Iterates over every record exactly once, classifying each as valid or
    invalid, and accumulates counts and sums in a single pass (O(n)).

    Processing rules (per spec):
    - record_count:      total records including invalid ones
    - category_summary:  count of valid records per category value
    - average_value:     mean of 'value' across valid records only;
                         0.0 when there are no valid records (no division by zero)
    - invalid_records:   count of records that fail _is_valid_record

    Args:
        data: A validated dataset dict (output of validator.validate_dataset).

    Returns:
        A result dict matching the specified output schema.
    """
    records: list = data["records"]

    record_count = len(records)
    invalid_count = 0
    category_summary: dict[str, int] = {}
    value_sum: float = 0.0
    valid_count = 0

    for record in records:
        if not _is_valid_record(record):
            invalid_count += 1
            continue

        category = str(record["category"])
        category_summary[category] = category_summary.get(category, 0) + 1
        value_sum += record["value"]
        valid_count += 1

    average_value = round(value_sum / valid_count, 6) if valid_count > 0 else 0.0

    return {
        "dataset_id": data["dataset_id"],
        "record_count": record_count,
        "category_summary": category_summary,
        "average_value": average_value,
        "invalid_records": invalid_count,
    }
