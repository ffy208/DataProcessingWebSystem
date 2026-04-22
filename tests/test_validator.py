"""Unit tests for services/validator.py."""

import pytest

from app.services.validator import DatasetValidationError, validate_dataset


class TestValidateDataset:
    def test_valid_dataset(self):
        data = {"dataset_id": "ds_001", "records": []}
        result = validate_dataset(data)
        assert result is data  # returns same object, no copy

    def test_valid_with_records(self):
        data = {
            "dataset_id": "ds_002",
            "records": [{"id": "r1", "timestamp": "t", "value": 1, "category": "A"}],
        }
        assert validate_dataset(data) is data

    # ── top-level type errors ──

    def test_rejects_list(self):
        with pytest.raises(DatasetValidationError, match="JSON object"):
            validate_dataset([1, 2, 3])

    def test_rejects_string(self):
        with pytest.raises(DatasetValidationError, match="JSON object"):
            validate_dataset("not a dict")

    def test_rejects_none(self):
        with pytest.raises(DatasetValidationError, match="JSON object"):
            validate_dataset(None)

    def test_rejects_integer(self):
        with pytest.raises(DatasetValidationError, match="JSON object"):
            validate_dataset(42)

    # ── dataset_id errors ──

    def test_rejects_missing_dataset_id(self):
        with pytest.raises(DatasetValidationError, match="dataset_id"):
            validate_dataset({"records": []})

    def test_rejects_empty_dataset_id(self):
        with pytest.raises(DatasetValidationError, match="dataset_id"):
            validate_dataset({"dataset_id": "", "records": []})

    def test_rejects_null_dataset_id(self):
        with pytest.raises(DatasetValidationError, match="dataset_id"):
            validate_dataset({"dataset_id": None, "records": []})

    def test_rejects_numeric_dataset_id(self):
        with pytest.raises(DatasetValidationError, match="dataset_id"):
            validate_dataset({"dataset_id": 123, "records": []})

    # ── records errors ──

    def test_rejects_missing_records(self):
        with pytest.raises(DatasetValidationError, match="records"):
            validate_dataset({"dataset_id": "ds_001"})

    def test_rejects_records_as_dict(self):
        with pytest.raises(DatasetValidationError, match="records"):
            validate_dataset({"dataset_id": "ds_001", "records": {}})

    def test_rejects_records_as_string(self):
        with pytest.raises(DatasetValidationError, match="records"):
            validate_dataset({"dataset_id": "ds_001", "records": "not-a-list"})

    def test_rejects_records_as_null(self):
        with pytest.raises(DatasetValidationError, match="records"):
            validate_dataset({"dataset_id": "ds_001", "records": None})

    def test_accepts_empty_records_list(self):
        # empty list is valid — processor handles it gracefully
        data = validate_dataset({"dataset_id": "ds_001", "records": []})
        assert data["records"] == []

    def test_extra_top_level_keys_ignored(self):
        data = {"dataset_id": "ds_001", "records": [], "meta": {"version": 2}}
        assert validate_dataset(data) is data
