"""Unit tests for services/processor.py — pure function, no I/O required."""

import math
import pytest

from app.services.processor import _is_valid_record, process_dataset


# ── _is_valid_record ──────────────────────────────────────────────────────────

class TestIsValidRecord:
    def _valid(self, **overrides):
        base = {"id": "r1", "timestamp": "2026-01-01T00:00:00Z", "value": 42.0, "category": "A"}
        base.update(overrides)
        return base

    def test_valid_int_value(self):
        assert _is_valid_record(self._valid(value=10)) is True

    def test_valid_float_value(self):
        assert _is_valid_record(self._valid(value=3.14)) is True

    def test_valid_zero_value(self):
        assert _is_valid_record(self._valid(value=0)) is True

    def test_valid_negative_value(self):
        assert _is_valid_record(self._valid(value=-100)) is True

    def test_not_a_dict(self):
        assert _is_valid_record("string") is False
        assert _is_valid_record(None) is False
        assert _is_valid_record([1, 2, 3]) is False

    def test_missing_id(self):
        r = self._valid()
        del r["id"]
        assert _is_valid_record(r) is False

    def test_missing_timestamp(self):
        r = self._valid()
        del r["timestamp"]
        assert _is_valid_record(r) is False

    def test_missing_value(self):
        r = self._valid()
        del r["value"]
        assert _is_valid_record(r) is False

    def test_missing_category(self):
        r = self._valid()
        del r["category"]
        assert _is_valid_record(r) is False

    def test_value_is_string(self):
        assert _is_valid_record(self._valid(value="ten")) is False

    def test_value_is_none(self):
        assert _is_valid_record(self._valid(value=None)) is False

    def test_value_is_bool_true(self):
        # bool is a subclass of int in Python — must be explicitly rejected
        assert _is_valid_record(self._valid(value=True)) is False

    def test_value_is_bool_false(self):
        assert _is_valid_record(self._valid(value=False)) is False

    def test_value_is_nan(self):
        assert _is_valid_record(self._valid(value=float("nan"))) is False

    def test_value_is_inf(self):
        assert _is_valid_record(self._valid(value=float("inf"))) is False

    def test_value_is_neg_inf(self):
        assert _is_valid_record(self._valid(value=float("-inf"))) is False

    def test_extra_fields_ok(self):
        # additional fields should not invalidate a record
        assert _is_valid_record(self._valid(extra="ignored")) is True


# ── process_dataset ───────────────────────────────────────────────────────────

def _make_dataset(dataset_id="ds_test", records=None):
    return {"dataset_id": dataset_id, "records": records or []}


class TestProcessDataset:
    def test_empty_records(self):
        result = process_dataset(_make_dataset(records=[]))
        assert result["record_count"] == 0
        assert result["invalid_records"] == 0
        assert result["average_value"] == 0.0
        assert result["category_summary"] == {}
        assert result["dataset_id"] == "ds_test"

    def test_all_valid(self):
        records = [
            {"id": "r1", "timestamp": "t", "value": 10, "category": "A"},
            {"id": "r2", "timestamp": "t", "value": 20, "category": "B"},
            {"id": "r3", "timestamp": "t", "value": 30, "category": "A"},
        ]
        result = process_dataset(_make_dataset(records=records))
        assert result["record_count"] == 3
        assert result["invalid_records"] == 0
        assert result["average_value"] == 20.0
        assert result["category_summary"] == {"A": 2, "B": 1}

    def test_all_invalid(self):
        records = [{"id": "r1"}, {"value": 10}, None, "string"]
        result = process_dataset(_make_dataset(records=records))
        assert result["record_count"] == 4
        assert result["invalid_records"] == 4
        assert result["average_value"] == 0.0
        assert result["category_summary"] == {}

    def test_mixed_valid_invalid(self):
        records = [
            {"id": "r1", "timestamp": "t", "value": 100, "category": "X"},
            {"id": "r2", "timestamp": "t", "value": 200, "category": "Y"},
            {"id": "r3"},                                                       # invalid: missing fields
            {"id": "r4", "timestamp": "t", "value": "bad", "category": "X"},    # invalid: str value
        ]
        result = process_dataset(_make_dataset(records=records))
        assert result["record_count"] == 4
        assert result["invalid_records"] == 2
        assert result["average_value"] == 150.0
        assert result["category_summary"] == {"X": 1, "Y": 1}

    def test_average_precision(self):
        records = [
            {"id": "r1", "timestamp": "t", "value": 1, "category": "A"},
            {"id": "r2", "timestamp": "t", "value": 2, "category": "A"},
            {"id": "r3", "timestamp": "t", "value": 3, "category": "A"},
        ]
        result = process_dataset(_make_dataset(records=records))
        assert result["average_value"] == 2.0

    def test_no_division_by_zero_when_all_invalid(self):
        records = [{"bad": "record"}]
        result = process_dataset(_make_dataset(records=records))
        assert result["average_value"] == 0.0
        assert not math.isnan(result["average_value"])

    def test_nan_value_counted_as_invalid(self):
        records = [
            {"id": "r1", "timestamp": "t", "value": float("nan"), "category": "A"},
            {"id": "r2", "timestamp": "t", "value": 10, "category": "A"},
        ]
        result = process_dataset(_make_dataset(records=records))
        assert result["invalid_records"] == 1
        assert result["average_value"] == 10.0

    def test_category_summary_counts_only_valid(self):
        records = [
            {"id": "r1", "timestamp": "t", "value": 1, "category": "A"},
            {"id": "r2", "timestamp": "t", "value": "bad", "category": "A"},  # invalid
        ]
        result = process_dataset(_make_dataset(records=records))
        assert result["category_summary"] == {"A": 1}

    def test_dataset_id_preserved(self):
        result = process_dataset(_make_dataset(dataset_id="my_special_id"))
        assert result["dataset_id"] == "my_special_id"

    def test_record_count_includes_invalid(self):
        records = [
            {"id": "r1", "timestamp": "t", "value": 1, "category": "A"},
            {"bad": True},
        ]
        result = process_dataset(_make_dataset(records=records))
        assert result["record_count"] == 2   # total
        assert result["invalid_records"] == 1

    def test_fixture_valid_dataset(self):
        import json, pathlib
        data = json.loads(pathlib.Path("tests/fixtures/valid_dataset.json").read_text())
        result = process_dataset(data)
        assert result["record_count"] == 10
        assert result["invalid_records"] == 0
        assert result["average_value"] == pytest.approx(32.05)
        assert result["category_summary"] == {"A": 4, "B": 3, "C": 3}

    def test_fixture_mixed_invalid(self):
        import json, pathlib
        data = json.loads(pathlib.Path("tests/fixtures/mixed_invalid.json").read_text())
        result = process_dataset(data)
        assert result["record_count"] == 8
        assert result["invalid_records"] == 5
        assert result["average_value"] == 200.0

    def test_fixture_all_invalid(self):
        import json, pathlib
        data = json.loads(pathlib.Path("tests/fixtures/all_invalid.json").read_text())
        result = process_dataset(data)
        assert result["invalid_records"] == result["record_count"]
        assert result["average_value"] == 0.0
