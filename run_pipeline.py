"""
Main pipeline entry point.

Usage:
    python run_pipeline.py --input sample_data/events_large.csv
    python run_pipeline.py --input sample_data/events_small.csv

Steps:
    1. Ingest CSV + compute SHA-256 fingerprint
    2. File-schema check (reject whole file if incompatible)
    3. Load existing event_ids from lake (for cross-run duplicate detection)
    4. Record-level validation → good / rejects / replays
    5. Transform good records
    6. Publish via write-to-staging-then-commit
    7. Write rejects CSV
    8. Reconcile counts
    9. Log run metrics
"""
import argparse
import uuid
from pathlib import Path

from loguru import logger

import sys
sys.path.insert(0, str(Path(__file__).parent))

from pipeline.ingest import ingest
from pipeline.validate import check_file_schema, validate_records, FileSchemaError
from pipeline.transform import transform
from pipeline.publish import publish, get_existing_event_ids, get_total_published_rows
from pipeline.rejects import write_rejects
from utils.logger import RunLogger
from utils.reconcile import reconcile, ReconciliationError


def run(input_path: str) -> dict:
    run_id = str(uuid.uuid4())[:8]
    input_path = Path(input_path)

    run_logger = None

    try:
        # 1. Ingest
        df, fingerprint = ingest(input_path, run_id)
        input_rows = len(df)

        run_logger = RunLogger(run_id, input_path.name, fingerprint)
        run_logger.record(input_rows=input_rows)
        logger.info(f"Ingested {input_rows:,} rows from {input_path.name}")

        # 2. File-schema check
        check_file_schema(df)
        logger.info("File schema check passed")

        # 3. Load existing event_ids for cross-run duplicate detection
        existing_event_ids = get_existing_event_ids()
        logger.info(f"Loaded {len(existing_event_ids):,} existing event_ids from lake")

        # 4. Record-level validation
        good_df, rejects_df, replays_df = validate_records(df, existing_event_ids)

        conflicting_rows = int((rejects_df.get("reject_reason", pd.Series(dtype=str)) == "conflicting_event_id").sum()) if not rejects_df.empty else 0
        rejected_rows = len(rejects_df)
        duplicate_replay_rows = len(replays_df)

        run_logger.record(
            rejected_rows=rejected_rows,
            duplicate_replay_rows=duplicate_replay_rows,
            conflicting_rows=conflicting_rows,
        )
        logger.info(f"Validation: good={len(good_df)}, rejects={rejected_rows}, replays={duplicate_replay_rows}, conflicts={conflicting_rows}")

        # 5. Transform
        transformed_df = transform(good_df)

        # 6. Publish
        pub_result = publish(transformed_df, run_id, input_path.name, fingerprint)
        newly_published = pub_result["newly_published_rows"]
        total_published = pub_result["total_published_rows"]

        run_logger.record(
            newly_published_rows=newly_published,
            total_published_rows=total_published,
        )
        logger.info(f"Published {newly_published:,} rows. Total in lake: {total_published:,}")

        # 7. Write rejects
        write_rejects(rejects_df, run_id)

        # 8. Reconcile
        report = reconcile(
            run_id=run_id,
            input_rows=input_rows,
            newly_published_rows=newly_published,
            rejected_rows=rejected_rows,
            duplicate_replay_rows=duplicate_replay_rows,
            conflicting_rows=conflicting_rows,
            total_published_rows=total_published,
        )
        logger.info("Reconciliation passed")
        sys.stdout.buffer.write((report + "\n").encode("utf-8"))

        # 9. Log success
        log = run_logger.finish("success")
        return log

    except FileSchemaError as e:
        logger.error(f"File schema rejected: {e}")
        if run_logger:
            run_logger.finish("failed", failure_reason=str(e))
        raise

    except ReconciliationError as e:
        logger.error(f"Reconciliation failed: {e}")
        if run_logger:
            run_logger.finish("failed", failure_reason=str(e))
        raise

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        if run_logger:
            run_logger.finish("failed", failure_reason=str(e))
        raise


import pandas as pd  # noqa: E402 — needed for conflicting_rows count above

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the data lake pipeline")
    parser.add_argument("--input", required=True, help="Path to input CSV file")
    args = parser.parse_args()
    run(args.input)
