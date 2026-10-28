"""
Partition pruning benchmark.

Demonstrates that filtering with explicit Hive partition columns
(year, month) causes PyArrow to select fewer files than a full scan.

Metrics reported:
    - discovered_files  : total Parquet files in the dataset
    - selected_files    : files selected by PyArrow partition metadata
    - selected_size_mb  : sum of selected file sizes (from filesystem)
    - returned_rows     : rows in the query result
    - duration_seconds  : wall-clock time for to_table()

IMPORTANT DISTINCTION:
    selected_files = files chosen by partition directory metadata.
    physically_read_files = files actually opened by the OS.
    These are NOT the same. PyArrow may open fewer files internally
    due to row-group filtering, but we cannot measure that from Python
    without OS-level instrumentation. We report selected_files only
    and document this limitation clearly.

Usage:
    python pruning_demo.py
"""
import time
from pathlib import Path

import pyarrow.dataset as ds

import sys
sys.path.insert(0, str(Path(__file__).parent))
from config import LAKE_DIR, OUTPUT_DIR


def _file_sizes_mb(fragments) -> float:
    total = 0
    for frag in fragments:
        try:
            total += Path(frag.path).stat().st_size
        except Exception:
            pass
    return round(total / (1024 * 1024), 3)


def run_benchmark() -> dict:
    if not LAKE_DIR.exists() or not any(LAKE_DIR.rglob("*.parquet")):
        print("ERROR: No data in lake. Run the pipeline first.")
        print("  python run_pipeline.py --input sample_data/events_large.csv")
        return {}

    dataset = ds.dataset(str(LAKE_DIR), format="parquet", partitioning="hive")

    # ── Full scan ──────────────────────────────────────────────────────────
    all_fragments = list(dataset.get_fragments())
    discovered_files = len(all_fragments)

    t0 = time.perf_counter()
    full_result = dataset.to_table(
        columns=["event_id", "event_type", "total_value"]
    )
    full_duration = round(time.perf_counter() - t0, 4)
    full_size_mb = _file_sizes_mb(all_fragments)

    # ── Pruned scan — explicit partition column filter ─────────────────────
    # PyArrow prunes reliably on Hive partition columns (integers).
    # A filter on event_timestamp alone does NOT guarantee partition pruning.
    march_filter = (ds.field("year") == 2024) & (ds.field("month") == 3)

    selected_fragments = list(dataset.get_fragments(filter=march_filter))
    selected_files = len(selected_fragments)
    selected_size_mb = _file_sizes_mb(selected_fragments)

    t0 = time.perf_counter()
    pruned_result = dataset.to_table(
        filter=march_filter,
        columns=["event_id", "event_type", "total_value"],
    )
    pruned_duration = round(time.perf_counter() - t0, 4)

    # ── Correctness check ─────────────────────────────────────────────────
    # The pruned result must be a subset of the full result
    full_ids = set(full_result["event_id"].to_pylist())
    pruned_ids = set(pruned_result["event_id"].to_pylist())
    subset_ok = pruned_ids.issubset(full_ids)

    eliminated_pct = round((1 - selected_files / discovered_files) * 100, 1) if discovered_files else 0

    results = {
        "full_scan": {
            "discovered_files": discovered_files,
            "selected_files": discovered_files,
            "selected_size_mb": full_size_mb,
            "returned_rows": full_result.num_rows,
            "duration_seconds": full_duration,
        },
        "pruned_scan": {
            "filter": "year == 2024 AND month == 3",
            "discovered_files": discovered_files,
            "selected_files": selected_files,
            "selected_size_mb": selected_size_mb,
            "returned_rows": pruned_result.num_rows,
            "duration_seconds": pruned_duration,
        },
        "files_eliminated_pct": eliminated_pct,
        "pruned_is_subset_of_full": subset_ok,
        "note": (
            "selected_files = files chosen by partition directory metadata. "
            "physically_read_files may differ and is not measured here."
        ),
    }

    report = _format_report(results)
    print(report)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUTPUT_DIR / "pruning_benchmark.txt").write_text(report, encoding="utf-8")
    print(f"\nSaved to output/pruning_benchmark.txt")

    return results


def _format_report(r: dict) -> str:
    fs = r["full_scan"]
    ps = r["pruned_scan"]
    lines = [
        "=" * 60,
        "PARTITION PRUNING BENCHMARK",
        "=" * 60,
        "",
        "Dataset: output/lake  (Hive-partitioned Parquet)",
        f"Filter applied: {ps['filter']}",
        "",
        "--- FULL SCAN (no partition filter) ---",
        f"  Discovered files  : {fs['discovered_files']}",
        f"  Selected files    : {fs['selected_files']}",
        f"  Selected size     : {fs['selected_size_mb']} MB",
        f"  Returned rows     : {fs['returned_rows']:,}",
        f"  Duration          : {fs['duration_seconds']} s",
        "",
        "--- PRUNED SCAN (year=2024 AND month=3) ---",
        f"  Discovered files  : {ps['discovered_files']}",
        f"  Selected files    : {ps['selected_files']}",
        f"  Selected size     : {ps['selected_size_mb']} MB",
        f"  Returned rows     : {ps['returned_rows']:,}",
        f"  Duration          : {ps['duration_seconds']} s",
        "",
        f"Files eliminated by pruning : {r['files_eliminated_pct']}%",
        f"Pruned result is subset of full : {r['pruned_is_subset_of_full']}",
        "",
        "NOTE:",
        r["note"],
        "=" * 60,
    ]
    return "\n".join(lines)


if __name__ == "__main__":
    run_benchmark()
