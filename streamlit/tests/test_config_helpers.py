"""Unit tests for config.py helper functions.

Covers functions NOT tested by test_config.py:
  - _parse_variant()
  - get_field_names_from_labels()
  - get_all_field_values()
  - get_field_name_for_key()
  - get_doc_type_config()
  - get_all_doc_type_configs()
  - get_raw_extraction_fields()

All tests mock the Snowpark session — no Snowflake connection needed.
"""

import importlib
import json
import sys
import types
from unittest import mock

import pytest


# ---------------------------------------------------------------------------
# Setup: import config with mocked Snowpark
# ---------------------------------------------------------------------------
def _make_mock_session(db="AI_EXTRACT_POC", schema="DOCUMENTS"):
    mock_session = mock.MagicMock()
    mock_row = {"DB": db, "SCH": schema}
    mock_session.sql.return_value.collect.return_value = [mock_row]
    return mock_session


def _import_config(mock_session):
    module_name = "config"
    if module_name in sys.modules:
        del sys.modules[module_name]
    for key in list(sys.modules):
        if key.startswith("snowflake.snowpark"):
            del sys.modules[key]

    fake_context = types.ModuleType("snowflake.snowpark.context")
    fake_context.get_active_session = mock.MagicMock(return_value=mock_session)
    fake_snowflake = types.ModuleType("snowflake")
    fake_snowpark = types.ModuleType("snowflake.snowpark")
    fake_snowflake.snowpark = fake_snowpark
    fake_snowpark.context = fake_context

    with mock.patch.dict(sys.modules, {
        "snowflake": fake_snowflake,
        "snowflake.snowpark": fake_snowpark,
        "snowflake.snowpark.context": fake_context,
    }):
        import os
        config_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "..", "streamlit"
        )
        sys.path.insert(0, config_dir)
        try:
            config = importlib.import_module(module_name)
        finally:
            sys.path.pop(0)
    return config


@pytest.fixture(scope="module")
def config():
    return _import_config(_make_mock_session())


# ---------------------------------------------------------------------------
# _parse_variant
# ---------------------------------------------------------------------------
class TestParseVariant:

    def test_none_returns_none(self, config):
        assert config._parse_variant(None) is None

    def test_valid_json_string(self, config):
        result = config._parse_variant('{"field_1": "Vendor"}')
        assert result == {"field_1": "Vendor"}

    def test_dict_passthrough(self, config):
        d = {"field_1": "Vendor"}
        assert config._parse_variant(d) is d

    def test_invalid_json_string(self, config):
        assert config._parse_variant("not json") is None

    def test_empty_string(self, config):
        assert config._parse_variant("") is None

    def test_non_dict_type(self, config):
        """Integers, lists, etc. should return None."""
        assert config._parse_variant(42) is None
        assert config._parse_variant([1, 2]) is None

    def test_nested_json(self, config):
        raw = '{"correctable": ["vendor_name", "total"], "approval_field": "status"}'
        result = config._parse_variant(raw)
        assert result["correctable"] == ["vendor_name", "total"]


# ---------------------------------------------------------------------------
# get_field_names_from_labels
# ---------------------------------------------------------------------------
class TestGetFieldNamesFromLabels:

    def test_extracts_field_keys_only(self, config):
        labels = {
            "field_1": "Vendor",
            "field_2": "Invoice #",
            "sender_label": "Vendor / Sender",
            "amount_label": "Total",
        }
        result = config.get_field_names_from_labels(labels)
        assert result == ["field_1", "field_2"]

    def test_sorts_numerically(self, config):
        labels = {"field_10": "Total", "field_2": "Inv#", "field_1": "Vendor"}
        result = config.get_field_names_from_labels(labels)
        assert result == ["field_1", "field_2", "field_10"]

    def test_empty_labels(self, config):
        assert config.get_field_names_from_labels({}) == []

    def test_no_field_keys(self, config):
        labels = {"sender_label": "Vendor", "amount_label": "Total"}
        assert config.get_field_names_from_labels(labels) == []

    def test_handles_13_fields_utility_bill(self, config):
        """Utility bills have fields 1-13; all should be returned."""
        labels = {f"field_{i}": f"Label {i}" for i in range(1, 14)}
        result = config.get_field_names_from_labels(labels)
        assert len(result) == 13
        assert result[0] == "field_1"
        assert result[-1] == "field_13"


# ---------------------------------------------------------------------------
# get_all_field_values
# ---------------------------------------------------------------------------
class TestGetAllFieldValues:

    def test_reads_fixed_columns_1_through_10(self, config):
        labels = {f"field_{i}": f"Label {i}" for i in range(1, 11)}
        row = {
            "FIELD_1": "Acme Corp",
            "FIELD_2": "INV-001",
            "FIELD_3": "PO-100",
            "FIELD_4": "2024-01-15",
            "FIELD_5": "2024-02-15",
            "FIELD_6": "Net 30",
            "FIELD_7": "Widgets Inc",
            "FIELD_8": "1000.00",
            "FIELD_9": "70.00",
            "FIELD_10": "1070.00",
            "RAW_EXTRACTION": None,
        }
        result = config.get_all_field_values(row, labels)
        assert result["field_1"] == "Acme Corp"
        assert result["field_10"] == "1070.00"

    def test_reads_overflow_fields_from_raw_extraction(self, config):
        """Fields 11-13 (utility bill) should come from raw_extraction."""
        labels = {
            "field_1": "Utility Company",
            "field_11": "Rate Schedule",
            "field_12": "Meter Number",
            "field_13": "Service Address",
        }
        raw = {
            "utility_company": "PSE&G",
            "rate_schedule": "RS-1",
            "meter_number": "M-12345",
            "service_address": "123 Main St",
        }
        row = {
            "FIELD_1": "PSE&G",
            "RAW_EXTRACTION": json.dumps(raw),
        }
        result = config.get_all_field_values(row, labels)
        assert result["field_1"] == "PSE&G"
        assert result["field_11"] == "RS-1"
        assert result["field_12"] == "M-12345"
        assert result["field_13"] == "123 Main St"

    def test_missing_raw_extraction_returns_none_for_overflow(self, config):
        labels = {"field_1": "Vendor", "field_11": "Extra Field"}
        row = {"FIELD_1": "Acme", "RAW_EXTRACTION": None}
        result = config.get_all_field_values(row, labels)
        assert result["field_1"] == "Acme"
        assert result["field_11"] is None

    def test_lowercase_key_fallback(self, config):
        """Should handle both FIELD_1 (uppercase) and field_1 (lowercase) keys."""
        labels = {"field_1": "Vendor"}
        row = {"field_1": "Acme", "RAW_EXTRACTION": None}
        result = config.get_all_field_values(row, labels)
        assert result["field_1"] == "Acme"

    def test_empty_raw_extraction_json(self, config):
        labels = {"field_1": "Vendor", "field_11": "Extra"}
        row = {"FIELD_1": "Acme", "RAW_EXTRACTION": "{}"}
        result = config.get_all_field_values(row, labels)
        assert result["field_11"] is None


# ---------------------------------------------------------------------------
# get_field_name_for_key
# ---------------------------------------------------------------------------
class TestGetFieldNameForKey:

    def test_uses_correctable_list(self, config):
        labels = {"field_1": "Vendor Name", "field_2": "Invoice Number"}
        review_fields = {"correctable": ["vendor_name", "invoice_number"]}
        assert config.get_field_name_for_key(labels, review_fields, "field_1") == "vendor_name"
        assert config.get_field_name_for_key(labels, review_fields, "field_2") == "invoice_number"

    def test_falls_back_to_snake_case_label(self, config):
        labels = {"field_1": "Vendor Name"}
        result = config.get_field_name_for_key(labels, None, "field_1")
        assert result == "vendor_name"

    def test_no_review_fields_no_label(self, config):
        labels = {"field_1": ""}
        result = config.get_field_name_for_key(labels, None, "field_1")
        assert result is None or result == ""

    def test_correctable_out_of_range(self, config):
        """field_5 with only 2 correctable entries should fall back."""
        labels = {"field_5": "Due Date"}
        review_fields = {"correctable": ["vendor_name", "invoice_number"]}
        result = config.get_field_name_for_key(labels, review_fields, "field_5")
        assert result == "due_date"

    def test_review_fields_no_correctable_key(self, config):
        labels = {"field_1": "Vendor Name"}
        review_fields = {"other_key": "value"}
        result = config.get_field_name_for_key(labels, review_fields, "field_1")
        assert result == "vendor_name"


# ---------------------------------------------------------------------------
# get_doc_type_config
# ---------------------------------------------------------------------------
class TestGetDocTypeConfig:

    def test_returns_dict_with_expected_keys(self, config):
        mock_session = _make_mock_session()
        labels = json.dumps({"field_1": "Vendor"})
        table_schema = json.dumps({"columns": [{"name": "description"}]})
        review = json.dumps({"correctable": ["vendor_name"]})
        validation = json.dumps({"vendor_name": {"required": True}})

        mock_session.sql.return_value.collect.return_value = [{
            "DOC_TYPE": "INVOICE",
            "DISPLAY_NAME": "Invoice",
            "EXTRACTION_PROMPT": "Extract vendor, amount...",
            "FIELD_LABELS": labels,
            "TABLE_EXTRACTION_SCHEMA": table_schema,
            "REVIEW_FIELDS": review,
            "VALIDATION_RULES": validation,
            "ACTIVE": True,
        }]
        result = config.get_doc_type_config(mock_session, "INVOICE")
        assert result is not None
        assert result["doc_type"] == "INVOICE"
        assert result["field_labels"] == {"field_1": "Vendor"}
        assert result["validation_rules"] == {"vendor_name": {"required": True}}
        assert result["active"] is True

    def test_returns_none_for_unknown_type(self, config):
        mock_session = _make_mock_session()
        mock_session.sql.return_value.collect.return_value = []
        result = config.get_doc_type_config(mock_session, "NONEXISTENT")
        assert result is None

    def test_returns_none_on_exception(self, config):
        mock_session = _make_mock_session()
        mock_session.sql.return_value.collect.side_effect = Exception("boom")
        result = config.get_doc_type_config(mock_session, "INVOICE")
        assert result is None

    def test_handles_variant_field_labels(self, config):
        """FIELD_LABELS may come as a dict (VARIANT) instead of JSON string."""
        mock_session = _make_mock_session()
        mock_session.sql.return_value.collect.return_value = [{
            "DOC_TYPE": "RECEIPT",
            "DISPLAY_NAME": "Receipt",
            "EXTRACTION_PROMPT": "Extract merchant...",
            "FIELD_LABELS": {"field_1": "Merchant"},  # already a dict
            "TABLE_EXTRACTION_SCHEMA": None,
            "REVIEW_FIELDS": None,
            "VALIDATION_RULES": None,
            "ACTIVE": True,
        }]
        result = config.get_doc_type_config(mock_session, "RECEIPT")
        assert result["field_labels"] == {"field_1": "Merchant"}


# ---------------------------------------------------------------------------
# get_all_doc_type_configs
# ---------------------------------------------------------------------------
class TestGetAllDocTypeConfigs:

    def test_returns_list(self, config):
        mock_session = _make_mock_session()
        mock_session.sql.return_value.collect.return_value = [
            {
                "DOC_TYPE": "INVOICE", "DISPLAY_NAME": "Invoice",
                "EXTRACTION_PROMPT": "...", "FIELD_LABELS": "{}",
                "TABLE_EXTRACTION_SCHEMA": None, "REVIEW_FIELDS": None,
                "VALIDATION_RULES": None, "ACTIVE": True,
            },
            {
                "DOC_TYPE": "RECEIPT", "DISPLAY_NAME": "Receipt",
                "EXTRACTION_PROMPT": "...", "FIELD_LABELS": "{}",
                "TABLE_EXTRACTION_SCHEMA": None, "REVIEW_FIELDS": None,
                "VALIDATION_RULES": None, "ACTIVE": True,
            },
        ]
        result = config.get_all_doc_type_configs(mock_session)
        assert len(result) == 2
        assert result[0]["doc_type"] == "INVOICE"

    def test_returns_empty_on_exception(self, config):
        mock_session = _make_mock_session()
        mock_session.sql.return_value.collect.side_effect = Exception("gone")
        result = config.get_all_doc_type_configs(mock_session)
        assert result == []


# ---------------------------------------------------------------------------
# get_raw_extraction_fields
# ---------------------------------------------------------------------------
class TestGetRawExtractionFields:

    def test_returns_parsed_dict(self, config):
        mock_session = _make_mock_session()
        raw = {"vendor_name": "Acme", "total": "500.00"}
        mock_session.sql.return_value.collect.return_value = [
            {"RAW_EXTRACTION": json.dumps(raw)}
        ]
        result = config.get_raw_extraction_fields(mock_session, 1)
        assert result == raw

    def test_returns_empty_on_null(self, config):
        mock_session = _make_mock_session()
        mock_session.sql.return_value.collect.return_value = [
            {"RAW_EXTRACTION": None}
        ]
        result = config.get_raw_extraction_fields(mock_session, 1)
        assert result == {}

    def test_returns_empty_on_no_rows(self, config):
        mock_session = _make_mock_session()
        mock_session.sql.return_value.collect.return_value = []
        result = config.get_raw_extraction_fields(mock_session, 999)
        assert result == {}

    def test_returns_empty_on_exception(self, config):
        mock_session = _make_mock_session()
        mock_session.sql.return_value.collect.side_effect = Exception("boom")
        result = config.get_raw_extraction_fields(mock_session, 1)
        assert result == {}
