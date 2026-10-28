"""
Writes rejected records to a run-specific CSV file.
Every rejected record retains all original fields plus reject_reason.
"""
from pathlib import Path
import pandas as pd
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import REJECTS_DIR


def write_rejects(df: pd.DataFrame, run_id: str) -> Path | None:
    if df.empty:
        return None
    REJECTS_DIR.mkdir(parents=True, exist_ok=True)
    path = REJECTS_DIR / f"{run_id}_rejects.csv"
    df.to_csv(path, index=False)
    return path
