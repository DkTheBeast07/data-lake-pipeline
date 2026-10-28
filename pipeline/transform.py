"""
Transform module — clean and enrich validated records.

- Parse event_timestamp to UTC datetime
- Derive total_value = quantity × unit_price
- Extract integer partition keys: year, month, day
- Cast quantity and unit_price to correct types
- Attach ingested_at timestamp
"""
from pathlib import Path
import pandas as pd
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform a DataFrame of validated records.
    Returns enriched DataFrame ready for publication.
    """
    if df.empty:
        return df

    out = df.copy()

    out["event_timestamp"] = pd.to_datetime(out["event_timestamp"], utc=True)
    out["quantity"] = out["quantity"].astype(int)
    out["unit_price"] = out["unit_price"].astype(float)
    out["total_value"] = (out["quantity"] * out["unit_price"]).round(2)

    out["year"] = out["event_timestamp"].dt.year.astype(int)
    out["month"] = out["event_timestamp"].dt.month.astype(int)
    out["day"] = out["event_timestamp"].dt.day.astype(int)

    out["ingested_at"] = pd.Timestamp.utcnow().isoformat()

    # Normalise timestamp to ISO string for Parquet storage
    out["event_timestamp"] = out["event_timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    return out
