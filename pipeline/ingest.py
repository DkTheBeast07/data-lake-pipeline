"""
Ingest module — reads a CSV file and attaches run metadata.
Returns a DataFrame with run_id, source_file, source_fingerprint attached.
"""
import hashlib
from pathlib import Path

import pandas as pd


def compute_fingerprint(path: Path) -> str:
    """SHA-256 fingerprint of the input file for idempotency tracking."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def ingest(path: Path, run_id: str) -> tuple[pd.DataFrame, str]:
    """
    Read CSV from path.
    Returns (dataframe, source_fingerprint).
    Raises FileNotFoundError if path does not exist.
    Raises ValueError if file is empty (no rows after header).
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")

    fingerprint = compute_fingerprint(path)
    df = pd.read_csv(path, dtype=str, keep_default_na=False)  # read all as str; keep_default_na=False preserves blank cells as "" instead of NaN

    if df.empty:
        raise ValueError(f"Input file is empty: {path}")

    df["run_id"] = run_id
    df["source_file"] = path.name
    df["source_fingerprint"] = fingerprint

    return df, fingerprint
