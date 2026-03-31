"""Unit tests for validate_extraction.py — extraction output validator.

Covers:
  - ValidationReport properties (success_rate, is_valid)
  - Type conformance checks (DATE, NUMBER, VARCHAR)
  - Cross-field rules (total >= subtotal, due_date >= doc_date, billing period order)
  - Value sanity warnings (unusually high, zero, short, long, out-of-range dates)
  - validate_extraction() end-to-end with various doc types
  - validate_extraction_batch() aggregation
  - Edge cases: empty extractions, unknown fields, skip keys

Pure Python — no Snowflake connection needed.
"""

import importlib
import os
import sys

import pytest


# ---------------------------------------------------------------------------
# Import validate_extraction from the streamlit directory
# ---------------------------------------------------------------------------
@pytest.fixture(scope="module")
def ve():
    """Import validate_extraction module."""
    ve_dir = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "..", "streamlit"
    )
    sys.path.insert(0, ve_dir)
    try:
        mod = importlib.import_module("validate_extraction")
    finally:
        sys.path.pop(0)
    return mod


# ---------------------------------------------------------------------------
# ValidationReport properties
# ---------------------------------------------------------------------------
class TestValidationReport:

    def test_success_rate_all_passed(self, ve):
        r = ve.ValidationReport("test.pdf", "INVOICE", total_fields=10, passed=10)
        assert r.success_rate == 100.0

    def test_success_rate_partial(self, ve):
        r = ve.ValidationReport("test.pdf", "INVOICE", total_fields=10, passed=7)
        assert r.success_rate == 70.0

    def test_success_rate_zero_fields(self, ve):
        r = ve.ValidationReport("test.pdf", "INVOICE", total_fields=0, passed=0)
        assert r.success_rate == 0.0

    def test_is_valid_no_failures(self, ve):
        r = ve.ValidationReport("test.pdf", "INVOICE")
        assert r.is_valid is True

    def test_is_valid_with_failures(self, ve):
        r = ve.ValidationReport("test.pdf", "INVOICE", failures=["MISSING: vendor_name"])
        assert r.is_valid is False

    def test_warnings_dont_invalidate(self, ve):
        r = ve.ValidationReport(
            "test.pdf", "INVOICE",
            warnings=["total=0 may indicate extraction failure"],
        )
        assert r.is_valid is True


# ---------------------------------------------------------------------------
# Date parsing
# ---------------------------------------------------------------------------
class TestDateParsing:

    @pytest.mark.parametrize("val,expected_year", [
        ("2024-01-15", 2024),
        ("01/15/2024", 2024),
        ("01-15-2024", 2024),
        ("15/01/2024", 2024),
        ("January 15, 2024", 2024),
        ("Jan 15, 2024", 2024),
        ("01/15/24", 2024),
        ("15 January 2024", 2024),
    ])
    def test_valid_dates_parse(self, ve, val, expected_year):
        result = ve._parse_date(val)
        assert result is not None
        assert result.year == expected_year

    def test_none_returns_none(self, ve):
        assert ve._parse_date(None) is None

    def test_garbage_returns_none(self, ve):
        assert ve._parse_date("not-a-date-at-all") is None


# ---------------------------------------------------------------------------
# Number parsing
# ---------------------------------------------------------------------------
class TestNumberParsing:

    @pytest.mark.parametrize("val,expected", [
        ("123.45", 123.45),
        ("$1,234.56", 1234.56),
        ("-12.34", -12.34),
        ("0", 0.0),
        (42, 42.0),
        (3.14, 3.14),
    ])
    def test_valid_numbers_parse(self, ve, val, expected):
        result = ve._parse_number(val)
        assert result == pytest.approx(expected)

    def test_none_returns_none(self, ve):
        assert ve._parse_number(None) is None

    def test_garbage_returns_none(self, ve):
        assert ve._parse_number("not-a-number") is None

    def test_just_dollar_sign(self, ve):
        assert ve._parse_number("$") is None


# ---------------------------------------------------------------------------
# Type conformance checks
# ---------------------------------------------------------------------------
class TestTypeConformance:

    def test_valid_date_passes(self, ve):
        parsed, err = ve._check_type_conformance("invoice_date", "2024-01-15", "DATE")
        assert err is None
        assert parsed is not None

    def test_invalid_date_fails(self, ve):
        parsed, err = ve._check_type_conformance("invoice_date", "garbage", "DATE")
        assert err is not None
        assert "Cannot parse" in err

    def test_valid_number_passes(self, ve):
        parsed, err = ve._check_type_conformance("total", "1234.56", "NUMBER")
        assert err is None
        assert parsed == pytest.approx(1234.56)

    def test_invalid_number_fails(self, ve):
        parsed, err = ve._check_type_conformance("total", "N/A", "NUMBER")
        assert err is not None

    def test_negative_number_fails(self, ve):
        """Negative values should fail type conformance (returns error)."""
        parsed, err = ve._check_type_conformance("total", "-50.00", "NUMBER")
        assert err is not None
        assert "Negative" in err

    def test_varchar_passthrough(self, ve):
        parsed, err = ve._check_type_conformance("vendor_name", "Acme Corp", "VARCHAR")
        assert err is None
        assert parsed == "Acme Corp"

    def test_none_value(self, ve):
        parsed, err = ve._check_type_conformance("any_field", None, "VARCHAR")
        assert err is None
        assert parsed is None


# ---------------------------------------------------------------------------
# Value sanity checks
# ---------------------------------------------------------------------------
class TestValueSanity:

    def test_high_total_warns(self, ve):
        warnings = ve._check_value_sanity("total", "2000000", "NUMBER")
        assert any(">1M" in w for w in warnings)

    def test_zero_total_warns(self, ve):
        warnings = ve._check_value_sanity("total", "0", "NUMBER")
        assert any("=0" in w for w in warnings)

    def test_normal_total_no_warning(self, ve):
        warnings = ve._check_value_sanity("total", "1500.00", "NUMBER")
        assert len(warnings) == 0

    def test_old_date_warns(self, ve):
        warnings = ve._check_value_sanity("invoice_date", "1998-05-01", "DATE")
        assert any("before 2000" in w for w in warnings)

    def test_future_date_warns(self, ve):
        warnings = ve._check_value_sanity("invoice_date", "2031-06-15", "DATE")
        assert any("after 2030" in w for w in warnings)

    def test_short_varchar_warns(self, ve):
        warnings = ve._check_value_sanity("vendor_name", "X", "VARCHAR")
        assert any("short" in w for w in warnings)

    def test_long_varchar_warns(self, ve):
        long_val = "A" * 600
        warnings = ve._check_value_sanity("vendor_name", long_val, "VARCHAR")
        assert any("chars" in w for w in warnings)

    def test_normal_varchar_no_warning(self, ve):
        warnings = ve._check_value_sanity("vendor_name", "Acme Corporation", "VARCHAR")
        assert len(warnings) == 0

    def test_non_total_field_no_zero_warning(self, ve):
        """Fields without 'total'/'charges'/'balance' in name shouldn't warn on zero."""
        warnings = ve._check_value_sanity("kwh_usage", "0", "NUMBER")
        assert not any("=0" in w for w in warnings)


# ---------------------------------------------------------------------------
# Cross-field rules
# ---------------------------------------------------------------------------
class TestCrossFieldRules:

    def test_total_gte_subtotal_passes(self, ve):
        raw = {"total": "1070.00", "subtotal": "1000.00"}
        report = ve.validate_extraction("test.pdf", "INVOICE", raw)
        cross = [w for w in report.warnings if "CROSS_FIELD" in w]
        total_sub = [w for w in cross if "total_gte_subtotal" in w]
        assert len(total_sub) == 0

    def test_total_lt_subtotal_warns(self, ve):
        raw = {"total": "500.00", "subtotal": "1000.00"}
        report = ve.validate_extraction("test.pdf", "INVOICE", raw)
        cross = [w for w in report.warnings if "total_gte_subtotal" in w]
        assert len(cross) == 1

    def test_due_before_doc_date_warns(self, ve):
        raw = {"document_date": "2024-06-15", "due_date": "2024-05-01"}
        report = ve.validate_extraction("test.pdf", "INVOICE", raw)
        cross = [w for w in report.warnings if "due_after_doc_date" in w]
        assert len(cross) == 1

    def test_due_after_doc_date_passes(self, ve):
        raw = {"document_date": "2024-06-01", "due_date": "2024-07-01"}
        report = ve.validate_extraction("test.pdf", "INVOICE", raw)
        cross = [w for w in report.warnings if "due_after_doc_date" in w]
        assert len(cross) == 0

    def test_billing_period_order_warns(self, ve):
        raw = {"billing_period_start": "2024-07-01", "billing_period_end": "2024-06-01"}
        report = ve.validate_extraction("test.pdf", "UTILITY_BILL", raw)
        cross = [w for w in report.warnings if "billing_period_order" in w]
        assert len(cross) == 1

    def test_missing_cross_field_pair_skipped(self, ve):
        """If one field in a pair is missing, the rule should be silently skipped."""
        raw = {"total": "500.00"}  # no subtotal
        report = ve.validate_extraction("test.pdf", "INVOICE", raw)
        cross = [w for w in report.warnings if "total_gte_subtotal" in w]
        assert len(cross) == 0

    def test_alt_fields_used(self, ve):
        """total_due / current_charges should be checked when total/subtotal are absent."""
        raw = {"total_due": "500.00", "current_charges": "1000.00"}
        report = ve.validate_extraction("test.pdf", "UTILITY_BILL", raw)
        cross = [w for w in report.warnings if "total_gte_subtotal" in w]
        assert len(cross) == 1


# ---------------------------------------------------------------------------
# validate_extraction end-to-end
# ---------------------------------------------------------------------------
class TestValidateExtraction:

    def test_perfect_invoice(self, ve):
        raw = {
            "vendor_name": "Acme Corp",
            "invoice_number": "INV-001",
            "invoice_date": "2024-06-15",
            "due_date": "2024-07-15",
            "subtotal": "1000.00",
            "tax": "70.00",
            "total": "1070.00",
        }
        report = ve.validate_extraction("inv_001.pdf", "INVOICE", raw)
        assert report.is_valid
        assert report.success_rate > 90.0
        assert report.total_fields == 7

    def test_empty_extraction(self, ve):
        report = ve.validate_extraction("empty.pdf", "INVOICE", {})
        assert report.total_fields == 0
        assert report.is_valid  # no fields = no failures

    def test_all_null_values(self, ve):
        raw = {"vendor_name": None, "total": None, "invoice_date": None}
        report = ve.validate_extraction("nulls.pdf", "INVOICE", raw)
        assert len(report.failures) == 3
        assert report.is_valid is False

    def test_skip_keys_excluded(self, ve):
        raw = {
            "_confidence": 0.95,
            "_validation_warnings": [],
            "vendor_name": "Acme",
        }
        report = ve.validate_extraction("test.pdf", "INVOICE", raw)
        assert report.total_fields == 1  # only vendor_name, not the _ keys

    def test_none_string_fails(self, ve):
        raw = {"vendor_name": "None", "total": "null"}
        report = ve.validate_extraction("test.pdf", "INVOICE", raw)
        assert len(report.failures) >= 2

    def test_mixed_pass_fail(self, ve):
        raw = {
            "vendor_name": "Acme Corp",
            "invoice_date": "not-a-date",
            "total": "1000.00",
        }
        report = ve.validate_extraction("mixed.pdf", "INVOICE", raw)
        assert report.passed == 2  # vendor + total
        assert len(report.failures) == 1  # date

    def test_unknown_field_defaults_to_varchar(self, ve):
        """Fields not in FIELD_TYPE_MAP should be treated as VARCHAR."""
        raw = {"custom_field_xyz": "some value"}
        report = ve.validate_extraction("test.pdf", "CUSTOM", raw)
        assert report.passed == 1
        assert report.is_valid


# ---------------------------------------------------------------------------
# validate_extraction_batch
# ---------------------------------------------------------------------------
class TestValidateExtractionBatch:

    def test_batch_returns_messages(self, ve):
        records = [
            {"file_name": "inv_01.pdf", "doc_type": "INVOICE",
             "raw_extraction": {"vendor_name": "Acme", "total": "500"}},
            {"file_name": "inv_02.pdf", "doc_type": "INVOICE",
             "raw_extraction": {"vendor_name": None, "total": "N/A"}},
        ]
        messages = ve.validate_extraction_batch(records)
        assert any("Validating 2" in m for m in messages)

    def test_batch_handles_json_string(self, ve):
        """raw_extraction as JSON string (from Snowflake VARIANT) should be parsed."""
        import json
        records = [
            {"file_name": "test.pdf", "doc_type": "INVOICE",
             "raw_extraction": json.dumps({"vendor_name": "Acme"})},
        ]
        messages = ve.validate_extraction_batch(records)
        assert isinstance(messages, list)

    def test_batch_empty_list(self, ve):
        messages = ve.validate_extraction_batch([])
        assert any("0" in m for m in messages)


# ---------------------------------------------------------------------------
# FIELD_TYPE_MAP coverage
# ---------------------------------------------------------------------------
class TestFieldTypeMap:

    def test_invoice_fields_mapped(self, ve):
        invoice_fields = ["vendor_name", "invoice_number", "invoice_date",
                          "due_date", "subtotal", "tax", "total"]
        for f in invoice_fields:
            assert f in ve.FIELD_TYPE_MAP, f"Missing {f} from FIELD_TYPE_MAP"

    def test_utility_bill_fields_mapped(self, ve):
        ub_fields = ["utility_company", "account_number", "kwh_usage",
                      "demand_kw", "billing_period_start", "billing_period_end"]
        for f in ub_fields:
            assert f in ve.FIELD_TYPE_MAP, f"Missing {f} from FIELD_TYPE_MAP"

    def test_all_date_fields_have_date_type(self, ve):
        for name, ftype in ve.FIELD_TYPE_MAP.items():
            if "date" in name or "period" in name:
                assert ftype == "DATE", f"{name} should be DATE, not {ftype}"

    def test_all_amount_fields_have_number_type(self, ve):
        amount_keywords = ["subtotal", "tax", "total", "charges", "balance", "kwh", "demand"]
        for name, ftype in ve.FIELD_TYPE_MAP.items():
            if any(kw in name for kw in amount_keywords):
                assert ftype == "NUMBER", f"{name} should be NUMBER, not {ftype}"
