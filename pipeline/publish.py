"""
Publication module — safe write-to-staging-then-commit pattern.

Protocol:
1. Write all Parquet files to output/staging/<run_id>/
2. Validate staged files (row count must match)
3. Move staged files to final lake partitions (atomic per-file on same filesystem)
4. Write commit manifest to output/manifests/<run_id>.json
5. Only after manifest is written is the run considered committed.

Atomicity note:
    os.replace() is atomic on POSIX within the same filesystem.
    On Windows it is NOT atomic if the destination exists — we handle this
    by writing to a unique staging path and using shutil.move which falls
    back to copy+delete. This means a crash between move and manifest
    could leave orphaned files. The manifest is the source of truth:
    if no manifest exists for a run_id, that run is not committed.

Readers should check for a committed manifest before querying a partition.
"""
import json
import os
import shutil
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import LAKE_DIR, STAGING_DIR, MANIFESTS_DIR, TARGET_ROWS_PER_FILE


def _partition_path(base: Path, year: int, month: int, day: int) -> Path:
    return base / f"year={year}" / f"month={month}" / f"day={day}"


def _write_partition_to_staging(
    partition_df: pd.DataFrame,
    staging_base: Path,
    year: int,
    month: int,
    day: int,
    run_id: str,
) -> list[Path]:
    """Write one partition's data to staging. Returns list of written file paths."""
    dest = _partition_path(staging_base, year, month, day)
    dest.mkdir(parents=True, exist_ok=True)

    cols_to_drop = {"year", "month", "day"}
    write_df = partition_df.drop(columns=[c for c in cols_to_drop if c in partition_df.columns])

    table = pa.Table.from_pandas(write_df, preserve_index=False)
    written = []

    # Split into chunks targeting TARGET_ROWS_PER_FILE rows per file
    total = len(write_df)
    n_files = max(1, (total + TARGET_ROWS_PER_FILE - 1) // TARGET_ROWS_PER_FILE)
    chunk_size = (total + n_files - 1) // n_files

    for i in range(n_files):
        chunk = table.slice(i * chunk_size, chunk_size)
        file_path = dest / f"run_{run_id}_part_{str(i).zfill(4)}.parquet"
        pq.write_table(chunk, file_path)
        written.append(file_path)

    return written


def _validate_staged(staged_files: list[Path], expected_rows: int) -> None:
    """Read staged files and verify total row count matches expected."""
    actual = sum(pq.read_metadata(f).num_rows for f in staged_files)
    if actual != expected_rows:
        raise ValueError(
            f"Staged validation failed: expected {expected_rows} rows, found {actual}"
        )


def _move_to_lake(staged_files: list[Path], staging_base: Path) -> list[Path]:
    """
    Move staged files from staging/<run_id>/ to lake/.
    Preserves the year=/month=/day= subdirectory structure.
    Returns list of final lake paths.
    """
    final_paths = []
    for src in staged_files:
        relative = src.relative_to(staging_base)
        dest = LAKE_DIR / relative
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(src), str(dest))
        final_paths.append(dest)
    return final_paths


def _write_manifest(
    run_id: str,
    source_file: str,
    source_fingerprint: str,
    partitions: list[str],
    newly_published_rows: int,
    total_published_rows: int,
) -> Path:
    MANIFESTS_DIR.mkdir(parents=True, exist_ok=True)
    manifest = {
        "run_id": run_id,
        "source_file": source_file,
        "source_fingerprint": source_fingerprint,
        "partitions_written": partitions,
        "newly_published_rows": newly_published_rows,
        "total_published_rows": total_published_rows,
        "status": "committed",
    }
    path = MANIFESTS_DIR / f"{run_id}.json"
    path.write_text(json.dumps(manifest, indent=2))
    return path


def get_existing_event_ids() -> dict[str, str]:
    """
    Scan the lake and return a dict of {event_id: payload_hash}.
    Used by validate.py to detect cross-run duplicates and conflicts.
    payload_hash is stored as a column in the Parquet files.
    """
    import pyarrow.dataset as ds
    if not LAKE_DIR.exists() or not any(LAKE_DIR.rglob("*.parquet")):
        return {}

    dataset = ds.dataset(str(LAKE_DIR), format="parquet", partitioning="hive")
    try:
        table = dataset.to_table(columns=["event_id", "payload_hash"])
        return dict(zip(table["event_id"].to_pylist(), table["payload_hash"].to_pylist()))
    except Exception:
        return {}


def get_total_published_rows() -> int:
    """Count total rows currently in the lake across all partitions."""
    import pyarrow.dataset as ds
    if not LAKE_DIR.exists() or not any(LAKE_DIR.rglob("*.parquet")):
        return 0
    dataset = ds.dataset(str(LAKE_DIR), format="parquet", partitioning="hive")
    return dataset.count_rows()


def publish(
    df: pd.DataFrame,
    run_id: str,
    source_file: str,
    source_fingerprint: str,
) -> dict:
    """
    Publish transformed records to the lake using write-to-staging-then-commit.

    Returns a dict with publication metrics.
    Raises on staged validation failure — lake is never touched in that case.
    """
    if df.empty:
        return {"newly_published_rows": 0, "partitions": []}

    # Add payload_hash for future duplicate detection
    import hashlib
    PAYLOAD_COLS = ["order_id", "customer_id", "event_type",
                    "event_timestamp", "product_id", "quantity", "unit_price", "region"]

    def _phash(row):
        raw = "|".join(str(row[c]) for c in PAYLOAD_COLS)
        return hashlib.sha256(raw.encode()).hexdigest()

    df = df.copy()
    df["payload_hash"] = df.apply(_phash, axis=1)

    staging_base = STAGING_DIR / run_id
    staging_base.mkdir(parents=True, exist_ok=True)

    staged_files: list[Path] = []
    partition_labels: list[str] = []

    try:
        for (year, month, day), part_df in df.groupby(["year", "month", "day"]):
            files = _write_partition_to_staging(part_df, staging_base, year, month, day, run_id)
            staged_files.extend(files)
            partition_labels.append(f"year={year}/month={month}/day={day}")

        # Validate before touching the lake
        _validate_staged(staged_files, len(df))

        # Move to lake
        _move_to_lake(staged_files, staging_base)

        # Clean up staging dir
        shutil.rmtree(staging_base, ignore_errors=True)

        total_rows = get_total_published_rows()

        _write_manifest(
            run_id=run_id,
            source_file=source_file,
            source_fingerprint=source_fingerprint,
            partitions=partition_labels,
            newly_published_rows=len(df),
            total_published_rows=total_rows,
        )

        return {
            "newly_published_rows": len(df),
            "partitions": partition_labels,
            "total_published_rows": total_rows,
        }

    except Exception:
        # Clean up staging on failure — lake is untouched
        shutil.rmtree(staging_base, ignore_errors=True)
        raise
