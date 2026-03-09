"""
End-to-end user workflow test — fake user modifies invoices, verifies
propagation through V_DOCUMENT_SUMMARY, then reverses all changes.

Simulates the exact Snowpark calls that 3_Review.py makes:
  1. Read documents from V_DOCUMENT_SUMMARY
  2. INSERT INTO INVOICE_REVIEW ... SELECT ... PARSE_JSON(?)
  3. Verify view reflects corrected values via COALESCE
  4. Verify append-only audit trail in INVOICE_REVIEW
  5. Clean up all test rows and verify originals are restored

Tests are numbered to enforce execution order.  All test review rows
are tagged with a unique reviewer_notes prefix so cleanup is safe.
"""

import json
import os
import time

import pytest


pytestmark = pytest.mark.sql

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
CONNECTION_NAME = os.environ.get("POC_CONNECTION", "default")
POC_DB = os.environ.get("POC_DB", "AI_EXTRACT_POC")
POC_SCHEMA = os.environ.get("POC_SCHEMA", "DOCUMENTS")
POC_WH = os.environ.get("POC_WH", "AI_EXTRACT_WH")
POC_ROLE = os.environ.get("POC_ROLE", "AI_EXTRACT_APP")
FQ = f"{POC_DB}.{POC_SCHEMA}"

# Unique tag — every test review row uses this prefix in reviewer_notes
TAG = "__e2e_user_workflow__"
TAG_LIKE = f"{TAG}%"  # LIKE pattern for cleanup queries


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture(scope="module")
def sf_cursor():
    import snowflake.connector

    conn = snowflake.connector.connect(connection_name=CONNECTION_NAME)
    cur = conn.cursor()
    cur.execute(f"USE ROLE {POC_ROLE}")
    cur.execute(f"USE DATABASE {POC_DB}")
    cur.execute(f"USE SCHEMA {POC_SCHEMA}")
    cur.execute(f"USE WAREHOUSE {POC_WH}")
    yield cur
    # Safety-net cleanup: remove any leftover test rows even if tests fail
    cur.execute(
        f"DELETE FROM {FQ}.INVOICE_REVIEW "
        f"WHERE reviewer_notes LIKE '{TAG}%'"
    )
    cur.close()
    conn.close()


@pytest.fixture(scope="module")
def targets(sf_cursor):
    """Pick 3 unreviewed invoices as test targets.

    Returns a list of dicts with original values from V_DOCUMENT_SUMMARY.
    """
    sf_cursor.execute(f"""
        SELECT record_id, file_name, doc_type,
               vendor_name, invoice_number, po_number,
               invoice_date, due_date, payment_terms,
               recipient, subtotal, tax_amount, total_amount,
               review_status
        FROM {FQ}.V_DOCUMENT_SUMMARY
        WHERE review_status IS NULL AND doc_type = 'INVOICE'
        ORDER BY record_id
        LIMIT 3
    """)
    cols = [d[0] for d in sf_cursor.description]
    rows = sf_cursor.fetchall()
    assert len(rows) >= 3, (
        f"Need at least 3 unreviewed invoices, found {len(rows)}. "
        "Run the extraction pipeline first."
    )
    return [dict(zip(cols, r)) for r in rows]


# ---------------------------------------------------------------------------
# Tests — numbered for execution order
# ---------------------------------------------------------------------------


class TestUserWorkflowE2E:
    """Simulate a real user editing invoices on the Review page."""

    def test_01_baseline_unreviewed(self, sf_cursor, targets):
        """All 3 target invoices start with review_status=NULL."""
        for t in targets:
            assert t["REVIEW_STATUS"] is None, (
                f"Record {t['RECORD_ID']} already reviewed — "
                "test requires unreviewed invoices"
            )
            assert t["VENDOR_NAME"] is not None, (
                f"Record {t['RECORD_ID']} has no vendor_name"
            )
            assert t["TOTAL_AMOUNT"] is not None, (
                f"Record {t['RECORD_ID']} has no total_amount"
            )

    def test_02_approve_no_corrections(self, sf_cursor, targets):
        """User approves invoice #1 without changing any fields.

        View should show REVIEW_STATUS='APPROVED' and all original
        field values unchanged (COALESCE falls through to ef.field_*).
        """
        t = targets[0]
        rid, fname = t["RECORD_ID"], t["FILE_NAME"]

        sf_cursor.execute(
            f"""INSERT INTO {FQ}.INVOICE_REVIEW (
                record_id, file_name, review_status,
                reviewer_notes, corrections
            ) SELECT %s, %s, 'APPROVED', %s, PARSE_JSON(%s)""",
            (rid, fname, f"{TAG}approve_01", json.dumps({})),
        )

        # Verify view
        sf_cursor.execute(
            f"SELECT review_status, vendor_name, total_amount "
            f"FROM {FQ}.V_DOCUMENT_SUMMARY WHERE record_id = %s",
            (rid,),
        )
        row = sf_cursor.fetchone()
        assert row is not None, f"Record {rid} not in V_DOCUMENT_SUMMARY"
        assert row[0] == "APPROVED", f"Expected APPROVED, got {row[0]}"
        # Fields should be unchanged — COALESCE falls through
        assert str(row[1]) == str(t["VENDOR_NAME"]), (
            f"Vendor changed unexpectedly: {row[1]} != {t['VENDOR_NAME']}"
        )
        assert float(row[2]) == float(t["TOTAL_AMOUNT"]), (
            f"Total changed unexpectedly: {row[2]} != {t['TOTAL_AMOUNT']}"
        )

    def test_03_correct_vendor_and_total(self, sf_cursor, targets):
        """User corrects invoice #2: changes vendor_name and total_amount.

        View should show corrected values via COALESCE, not originals.
        """
        t = targets[1]
        rid, fname = t["RECORD_ID"], t["FILE_NAME"]
        new_vendor = "CORRECTED Vendor Inc"
        new_total = 999.99
        notes = f"{TAG}correct_vendor_total_02"

        corrections = {
            "vendor_name": new_vendor,
            "total_amount": new_total,
        }

        sf_cursor.execute(
            f"""INSERT INTO {FQ}.INVOICE_REVIEW (
                record_id, file_name, review_status,
                corrected_vendor_name, corrected_total,
                reviewer_notes, corrections
            ) SELECT %s, %s, 'CORRECTED', %s, %s, %s, PARSE_JSON(%s)""",
            (rid, fname, new_vendor, new_total, notes,
             json.dumps(corrections)),
        )

        # Verify view shows corrected values
        sf_cursor.execute(
            f"SELECT review_status, vendor_name, total_amount "
            f"FROM {FQ}.V_DOCUMENT_SUMMARY WHERE record_id = %s",
            (rid,),
        )
        row = sf_cursor.fetchone()
        assert row[0] == "CORRECTED"
        assert row[1] == new_vendor, (
            f"Vendor not corrected: got '{row[1]}', expected '{new_vendor}'"
        )
        assert float(row[2]) == new_total, (
            f"Total not corrected: got {row[2]}, expected {new_total}"
        )
        # Original values should differ
        assert row[1] != t["VENDOR_NAME"], "Vendor should differ from original"

    def test_04_correct_multiple_fields(self, sf_cursor, targets):
        """User corrects invoice #3: invoice_number, due_date, payment_terms, recipient.

        Tests that multiple COALESCE columns propagate independently.
        """
        t = targets[2]
        rid, fname = t["RECORD_ID"], t["FILE_NAME"]
        new_inv_num = "CORRECTED-INV-99999"
        new_due = "2099-12-31"
        new_terms = "NET 999"
        new_recipient = "Test Recipient Corp"
        notes = f"{TAG}correct_multi_03"

        corrections = {
            "invoice_number": new_inv_num,
            "due_date": new_due,
            "payment_terms": new_terms,
            "recipient": new_recipient,
        }

        sf_cursor.execute(
            f"""INSERT INTO {FQ}.INVOICE_REVIEW (
                record_id, file_name, review_status,
                corrected_invoice_number, corrected_due_date,
                corrected_payment_terms, corrected_recipient,
                reviewer_notes, corrections
            ) SELECT %s, %s, 'CORRECTED', %s, %s, %s, %s, %s, PARSE_JSON(%s)""",
            (rid, fname, new_inv_num, new_due, new_terms, new_recipient,
             notes, json.dumps(corrections)),
        )

        # Verify all 4 corrected fields in view
        sf_cursor.execute(
            f"""SELECT review_status, invoice_number, due_date,
                       payment_terms, recipient, vendor_name, total_amount
                FROM {FQ}.V_DOCUMENT_SUMMARY WHERE record_id = %s""",
            (rid,),
        )
        row = sf_cursor.fetchone()
        assert row[0] == "CORRECTED"
        assert row[1] == new_inv_num, f"invoice_number: {row[1]} != {new_inv_num}"
        assert str(row[2]) == new_due, f"due_date: {row[2]} != {new_due}"
        assert row[3] == new_terms, f"payment_terms: {row[3]} != {new_terms}"
        assert row[4] == new_recipient, f"recipient: {row[4]} != {new_recipient}"
        # Uncorrected fields should remain at original values
        assert str(row[5]) == str(t["VENDOR_NAME"]), (
            f"vendor_name should be unchanged: {row[5]} != {t['VENDOR_NAME']}"
        )
        assert float(row[6]) == float(t["TOTAL_AMOUNT"]), (
            f"total_amount should be unchanged: {row[6]} != {t['TOTAL_AMOUNT']}"
        )

    def test_05_second_edit_overwrites_first(self, sf_cursor, targets):
        """User re-edits invoice #2 — changes status to APPROVED and new total.

        The view must show the SECOND edit (latest reviewed_at wins).
        This proves the NOORDER autoincrement fix works with real data.
        """
        t = targets[1]
        rid, fname = t["RECORD_ID"], t["FILE_NAME"]
        final_total = 1234.56
        final_vendor = "Final Vendor LLC"
        notes = f"{TAG}re_edit_04"

        # Brief pause so reviewed_at is strictly later than test_03's insert
        sf_cursor.execute("SELECT SYSTEM$WAIT(1)")

        corrections = {
            "vendor_name": final_vendor,
            "total_amount": final_total,
        }

        sf_cursor.execute(
            f"""INSERT INTO {FQ}.INVOICE_REVIEW (
                record_id, file_name, review_status,
                corrected_vendor_name, corrected_total,
                reviewer_notes, corrections
            ) SELECT %s, %s, 'APPROVED', %s, %s, %s, PARSE_JSON(%s)""",
            (rid, fname, final_vendor, final_total, notes,
             json.dumps(corrections)),
        )

        # View must show the SECOND edit, not the first
        sf_cursor.execute(
            f"SELECT review_status, vendor_name, total_amount "
            f"FROM {FQ}.V_DOCUMENT_SUMMARY WHERE record_id = %s",
            (rid,),
        )
        row = sf_cursor.fetchone()
        assert row[0] == "APPROVED", (
            f"Status should be APPROVED (2nd edit), got {row[0]}"
        )
        assert row[1] == final_vendor, (
            f"Vendor should be '{final_vendor}' (2nd edit), got '{row[1]}'"
        )
        assert float(row[2]) == final_total, (
            f"Total should be {final_total} (2nd edit), got {row[2]}"
        )

    def test_06_other_records_untouched(self, sf_cursor, targets):
        """Records outside the 3 targets must not be affected."""
        target_ids = [t["RECORD_ID"] for t in targets]
        placeholders = ",".join(["%s"] * len(target_ids))

        sf_cursor.execute(
            f"""SELECT COUNT(*) FROM {FQ}.V_DOCUMENT_SUMMARY
                WHERE record_id NOT IN ({placeholders})
                  AND review_status IS NOT NULL
                  AND reviewed_at > DATEADD('minute', -5, CURRENT_TIMESTAMP())
            """,
            target_ids,
        )
        count = sf_cursor.fetchone()[0]
        # There might be pre-existing reviewed records — we just check that
        # no NEW reviews appeared in the last 5 minutes for non-target records
        # by verifying reviewer_notes doesn't contain our tag
        sf_cursor.execute(
            f"""SELECT COUNT(*) FROM {FQ}.INVOICE_REVIEW
                WHERE record_id NOT IN ({placeholders})
                  AND reviewer_notes LIKE %s
            """,
            target_ids + [TAG_LIKE],
        )
        leaked = sf_cursor.fetchone()[0]
        assert leaked == 0, (
            f"{leaked} test review rows leaked to non-target records"
        )

    def test_07_audit_trail_complete(self, sf_cursor, targets):
        """INVOICE_REVIEW has the full append-only audit trail.

        Invoice #1: 1 row (approved)
        Invoice #2: 2 rows (corrected, then re-approved)
        Invoice #3: 1 row (corrected)
        Total: 4 test rows
        """
        # Total test rows
        sf_cursor.execute(
            f"SELECT COUNT(*) FROM {FQ}.INVOICE_REVIEW "
            f"WHERE reviewer_notes LIKE '{TAG}%'"
        )
        total = sf_cursor.fetchone()[0]
        assert total == 4, f"Expected 4 audit rows, got {total}"

        # Per-record breakdown
        expected = {
            targets[0]["RECORD_ID"]: 1,  # approved once
            targets[1]["RECORD_ID"]: 2,  # corrected then re-approved
            targets[2]["RECORD_ID"]: 1,  # corrected once
        }
        for rid, expected_count in expected.items():
            sf_cursor.execute(
                f"SELECT COUNT(*) FROM {FQ}.INVOICE_REVIEW "
                f"WHERE record_id = %s AND reviewer_notes LIKE %s",
                (rid, TAG_LIKE),
            )
            actual = sf_cursor.fetchone()[0]
            assert actual == expected_count, (
                f"Record {rid}: expected {expected_count} audit rows, got {actual}"
            )

        # Verify chronological ordering: for invoice #2, the re-edit is later
        rid2 = targets[1]["RECORD_ID"]
        sf_cursor.execute(
            f"""SELECT reviewer_notes, review_status, reviewed_at
                FROM {FQ}.INVOICE_REVIEW
                WHERE record_id = %s AND reviewer_notes LIKE %s
                ORDER BY reviewed_at ASC""",
            (rid2, TAG_LIKE),
        )
        rows = sf_cursor.fetchall()
        assert rows[0][1] == "CORRECTED", "First edit should be CORRECTED"
        assert rows[1][1] == "APPROVED", "Second edit should be APPROVED"
        assert rows[1][2] > rows[0][2], (
            "Second edit reviewed_at must be later than first"
        )

    def test_08_rollback_and_verify_originals_restored(self, sf_cursor, targets):
        """Delete all test rows and verify originals are fully restored.

        After cleanup:
        - All 3 records should return to review_status=NULL
        - All original field values should be back (no corrections in view)
        - 0 leftover test rows in INVOICE_REVIEW
        """
        # --- Rollback ---
        sf_cursor.execute(
            f"DELETE FROM {FQ}.INVOICE_REVIEW "
            f"WHERE reviewer_notes LIKE '{TAG}%'"
        )
        deleted = sf_cursor.fetchone()[0]
        assert deleted == 4, f"Expected to delete 4 rows, deleted {deleted}"

        # --- Verify 0 leftover ---
        sf_cursor.execute(
            f"SELECT COUNT(*) FROM {FQ}.INVOICE_REVIEW "
            f"WHERE reviewer_notes LIKE '{TAG}%'"
        )
        remaining = sf_cursor.fetchone()[0]
        assert remaining == 0, f"{remaining} test rows still in INVOICE_REVIEW"

        # --- Verify all 3 records restored to original state ---
        for t in targets:
            rid = t["RECORD_ID"]
            sf_cursor.execute(
                f"""SELECT review_status, vendor_name, invoice_number,
                           total_amount, due_date, payment_terms, recipient
                    FROM {FQ}.V_DOCUMENT_SUMMARY
                    WHERE record_id = %s""",
                (rid,),
            )
            row = sf_cursor.fetchone()
            assert row is not None, f"Record {rid} missing from view after cleanup"

            # review_status should be NULL (back to unreviewed)
            assert row[0] is None, (
                f"Record {rid}: review_status should be NULL after rollback, "
                f"got '{row[0]}'"
            )

            # All original field values restored
            assert str(row[1]) == str(t["VENDOR_NAME"]), (
                f"Record {rid}: vendor_name not restored: "
                f"'{row[1]}' != '{t['VENDOR_NAME']}'"
            )
            assert str(row[2]) == str(t["INVOICE_NUMBER"]), (
                f"Record {rid}: invoice_number not restored: "
                f"'{row[2]}' != '{t['INVOICE_NUMBER']}'"
            )
            assert float(row[3]) == float(t["TOTAL_AMOUNT"]), (
                f"Record {rid}: total_amount not restored: "
                f"{row[3]} != {t['TOTAL_AMOUNT']}"
            )
