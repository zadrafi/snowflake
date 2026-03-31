#!/usr/bin/env python3
"""sync_streamlit_stage.py — Sync local Streamlit files to STREAMLIT_STAGE.

Compares local MD5 hashes against stage LIST metadata,
uploads only changed files, removes orphans.

Usage:
    python scripts/sync_streamlit_stage.py                    # sync
    python scripts/sync_streamlit_stage.py --dry-run          # preview
    python scripts/sync_streamlit_stage.py --no-remove        # skip orphan removal
    python scripts/sync_streamlit_stage.py --restart           # restart app after sync
    POC_CONNECTION=ci python scripts/sync_streamlit_stage.py  # CI connection

Environment variables:
    POC_CONNECTION   Snowflake connection name (default: 'default')
    POC_DB           Database (default: AI_EXTRACT_POC)
    POC_SCHEMA       Schema (default: DOCUMENTS)
    POC_WH           Warehouse (default: AI_EXTRACT_WH)
    POC_ROLE         Role (default: AI_EXTRACT_APP)
"""

import argparse
import hashlib
import json
import os
import sys
import time
from pathlib import Path

import snowflake.connector

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
CONNECTION = os.environ.get("POC_CONNECTION", "default")
DB         = os.environ.get("POC_DB", "AI_EXTRACT_POC")
SCHEMA     = os.environ.get("POC_SCHEMA", "DOCUMENTS")
WH         = os.environ.get("POC_WH", "AI_EXTRACT_WH")
ROLE       = os.environ.get("POC_ROLE", "AI_EXTRACT_APP")
FQ         = f"{DB}.{SCHEMA}"
STAGE      = f"@{FQ}.STREAMLIT_STAGE"

PROJECT_ROOT   = Path(__file__).resolve().parent.parent
STREAMLIT_DIR  = PROJECT_ROOT / "streamlit"

# ---------------------------------------------------------------------------
# File manifest — edit these lists when you add/remove app files
# ---------------------------------------------------------------------------
ROOT_FILES = [
    "streamlit_app.py",
    "config.py",
    "environment.yml",
    "field_highlighter.py",
    "validate_extraction.py",
    "writeback_pdf.py",
]

PAGE_FILES = [
    "0_Dashboard.py",
    "1_Document_Viewer.py",
    "2_Analytics.py",
    "3_Review.py",
    "4_Admin.py",
    "5_Cost.py",
    "6_Process_New.py",
    "7_Claude_PDF_Analysis.py",
    "8_Accuracy.py",
]


def _build_manifest() -> dict[str, Path]:
    """Build {stage_relative_path: local_absolute_path}."""
    manifest = {}
    for f in ROOT_FILES:
        local = STREAMLIT_DIR / f
        if local.exists():
            manifest[f] = local
        else:
            print(f"  WARN: {f} not found at {local}")
    for f in PAGE_FILES:
        local = STREAMLIT_DIR / "pages" / f
        if not local.exists():
            local = STREAMLIT_DIR / f
        if local.exists():
            manifest[f"pages/{f}"] = local
        else:
            print(f"  WARN: page {f} not found")
    return manifest


def _md5(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def _get_stage_checksums(cur) -> dict[str, str]:
    """Return {relative_path: md5} from LIST @STREAMLIT_STAGE."""
    checksums = {}
    try:
        cur.execute(f"LIST {STAGE}")
        for row in cur.fetchall():
            full_name, md5 = row[0], row[2] if len(row) > 2 else None
            parts = full_name.split("/", 1)
            rel = parts[1] if len(parts) > 1 else parts[0]
            if rel.endswith(".gz"):
                rel = rel[:-3]
            checksums[rel] = md5
    except Exception as e:
        print(f"  WARN: LIST failed: {e}")
    return checksums


def sync(dry_run=False, remove_orphans=True, restart_app=False):
    print(f"Connecting to {FQ} via '{CONNECTION}'...")
    conn = snowflake.connector.connect(connection_name=CONNECTION)
    cur = conn.cursor()
    cur.execute(f"USE ROLE {ROLE}")
    cur.execute(f"USE DATABASE {DB}")
    cur.execute(f"USE SCHEMA {SCHEMA}")
    cur.execute(f"USE WAREHOUSE {WH}")

    report = {"uploaded": [], "removed": [], "skipped": [], "errors": [], "dry_run": dry_run}

    try:
        manifest = _build_manifest()
        if not manifest:
            report["errors"].append("No files in manifest")
            return report

        print(f"\nLocal: {len(manifest)} files")
        stage_checksums = _get_stage_checksums(cur)
        print(f"Stage: {len(stage_checksums)} files\n")

        to_upload = []
        for stage_path, local_path in manifest.items():
            local_md5 = _md5(local_path)
            stage_md5 = stage_checksums.get(stage_path)
            if stage_md5 is None:
                to_upload.append((stage_path, local_path, "NEW"))
            elif stage_md5 != local_md5:
                to_upload.append((stage_path, local_path, "CHANGED"))
            else:
                report["skipped"].append(stage_path)

        manifest_paths = set(manifest.keys())
        orphans = [
            p for p in stage_checksums
            if p.endswith((".py", ".yml", ".css")) and p not in manifest_paths
        ]

        print(f"{'DRY RUN — ' if dry_run else ''}Plan:")
        print(f"  Upload: {len(to_upload)}")
        for sp, _, reason in to_upload:
            print(f"    {reason:10s} {sp}")
        print(f"  Remove: {len(orphans)}")
        for p in orphans:
            print(f"    ORPHAN    {p}")
        print(f"  Skip:   {len(report['skipped'])}")

        if dry_run:
            report["uploaded"] = [{"path": sp, "reason": r} for sp, _, r in to_upload]
            report["removed"] = orphans
            return report

        for stage_path, local_path, reason in to_upload:
            stage_dir = STAGE
            if "/" in stage_path:
                subdir = "/".join(stage_path.split("/")[:-1])
                stage_dir = f"{STAGE}/{subdir}"
            try:
                cur.execute(
                    f"PUT 'file://{local_path}' '{stage_dir}' "
                    f"AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
                )
                report["uploaded"].append(stage_path)
                print(f"  PUT {stage_path} ({reason}) ✓")
            except Exception as e:
                report["errors"].append(f"PUT {stage_path}: {e}")
                print(f"  PUT {stage_path} ✗ {e}")

        if remove_orphans:
            for stage_path in orphans:
                try:
                    cur.execute(f"REMOVE '{STAGE}/{stage_path}'")
                    report["removed"].append(stage_path)
                    print(f"  REMOVE {stage_path} ✓")
                except Exception as e:
                    report["errors"].append(f"REMOVE {stage_path}: {e}")

        if restart_app and (report["uploaded"] or report["removed"]):
            try:
                ts = time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime())
                n = len(report["uploaded"])
                cur.execute(
                    f"ALTER STREAMLIT {FQ}.AI_EXTRACT_DASHBOARD "
                    f"SET COMMENT = 'Synced {n} file(s) at {ts}'"
                )
                print(f"\n  App restarted ✓")
            except Exception as e:
                report["errors"].append(f"RESTART: {e}")

    finally:
        cur.close()
        conn.close()

    n_up  = len(report["uploaded"])
    n_rm  = len(report["removed"])
    n_sk  = len(report["skipped"])
    n_err = len(report["errors"])
    print(f"\n{'=' * 50}")
    print(f"Uploaded: {n_up}  Removed: {n_rm}  Skipped: {n_sk}  Errors: {n_err}")
    print(f"{'=' * 50}")
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync Streamlit files to STREAMLIT_STAGE")
    parser.add_argument("--dry-run",   action="store_true", help="Preview without uploading")
    parser.add_argument("--no-remove", action="store_true", help="Skip orphan removal")
    parser.add_argument("--restart",   action="store_true", help="Restart Streamlit app after sync")
    parser.add_argument("--json",      action="store_true", help="Output JSON report")
    args = parser.parse_args()

    report = sync(
        dry_run=args.dry_run,
        remove_orphans=not args.no_remove,
        restart_app=args.restart,
    )
    if args.json:
        print(json.dumps(report, indent=2))
    sys.exit(1 if report["errors"] else 0)
