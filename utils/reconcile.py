"""
Reconciliation module.

Enforces:
    input_rows = newly_published_rows + rejected_rows + duplicate_replay_rows

conflicting_rows are included in rejected_rows (they are a diagnostic subset).
Raises ReconciliationError if the equation does not balance.
Saves a human-readable report to output/reconciliation/.
"""
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import RECONCILIATION_DIR


class ReconciliationError(Exception):
    pass


def reconcile(
    run_id: str,
    input_rows: int,
    newly_published_rows: int,
    rejected_rows: int,
    duplicate_replay_rows: int,
    conflicting_rows: int,
    total_published_rows: int,
) -> str:
    """
    Verify the reconciliation equation and save a report.
    Returns the report text.
    Raises ReconciliationError if counts do not balance.
    """
    expected = newly_published_rows + rejected_rows + duplicate_replay_rows
    balanced = (expected == input_rows)

    report_lines = [
        f"Reconciliation Report - run_id: {run_id}",
        "=" * 55,
        f"  input_rows              : {input_rows:>10,}",
        f"  newly_published_rows    : {newly_published_rows:>10,}",
        f"  rejected_rows           : {rejected_rows:>10,}  (includes conflicting)",
        f"    conflicting_rows      : {conflicting_rows:>10,}  (diagnostic subset)",
        f"  duplicate_replay_rows   : {duplicate_replay_rows:>10,}",
        "-" * 55,
        f"  check (pub+rej+replay)  : {expected:>10,}",
        f"  BALANCED                : {'YES (OK)' if balanced else 'NO (FAIL)'}",
        "=" * 55,
        f"  total_published_rows    : {total_published_rows:>10,}  (all runs, all time)",
    ]
    report = "\n".join(report_lines)

    RECONCILIATION_DIR.mkdir(parents=True, exist_ok=True)
    path = RECONCILIATION_DIR / f"{run_id}_reconciliation.txt"
    path.write_text(report, encoding="utf-8")

    if not balanced:
        raise ReconciliationError(
            f"Reconciliation failed for run {run_id}: "
            f"input={input_rows} != published={newly_published_rows} + "
            f"rejected={rejected_rows} + replays={duplicate_replay_rows} = {expected}"
        )

    return report
