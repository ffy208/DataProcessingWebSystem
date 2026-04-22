"""JSON dataset validation before processing."""

from typing import Any


class DatasetValidationError(ValueError):
    """Raised when the top-level dataset JSON structure is invalid.

    Distinct from per-record invalidity (which is counted but not fatal).
    Only raised for structural problems that make the whole file unprocessable,
    e.g. missing 'records' key or wrong top-level type.
    """


def validate_dataset(data: Any) -> dict:
    """Validate the top-level structure of an uploaded dataset.

    Checks that:
    - The parsed JSON is a dict (not a list, string, etc.)
    - 'dataset_id' is present and is a non-empty string
    - 'records' is present and is a list

    Individual record validity (missing fields, wrong types) is intentionally
    NOT checked here — that is handled per-record in processor.py so invalid
    records can be counted rather than rejected outright.

    Args:
        data: The parsed JSON object from the uploaded file.

    Returns:
        The validated dict (same object, no copy).

    Raises:
        DatasetValidationError: If the structure is unprocessable.
    """
    if not isinstance(data, dict):
        raise DatasetValidationError(
            f"Expected a JSON object at the top level, got {type(data).__name__}"
        )

    dataset_id = data.get("dataset_id")
    if not dataset_id or not isinstance(dataset_id, str):
        raise DatasetValidationError(
            "'dataset_id' must be a non-empty string"
        )

    records = data.get("records")
    if records is None:
        raise DatasetValidationError("'records' key is missing")
    if not isinstance(records, list):
        raise DatasetValidationError(
            f"'records' must be a list, got {type(records).__name__}"
        )

    return data
