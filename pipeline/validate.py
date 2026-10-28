"""
Validation module — two-layer validation.

Layer 1 (file-level): Check that the CSV has exactly the expected columns.
    If not, raise FileSchemaError — the entire file is rejected, nothing is published.

Layer 2 (record-level): Validate each row against schema and business rules.
    Bad rows go to rejects with a reject_reason.
    Good rows continue to transform.

Duplicate / conflict handling (event_id based):
    - Same event_id + same canonical payload  → duplicate_replay (skip, count)
    - Same event_id + different payload        → conflicting_event_id (quarantine in rejects)
    - Different event_id, same order_id/type   → valid history, keep both
"""
import hashlib
from pathlib import Path

import pandas as pd
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import EXPECTED_COLUMNS, VALID_EVENT_TYPES, VALID_REGIONS


class FileSchemaError(Exception):
    """Raised when the input file has an incompatible column schema."""


CANONICAL_PAYLOAD_COLS = [
    "order_id", "customer_id", "event_type",
    "event_timestamp", "product_id", "quantity", "unit_price", "region",
]


def _payload_hash(row: pd.Series) -> str:
    """Deterministic hash of the canonical payload fields for conflict detection."""
    raw = "|".join(str(row[c]) for c in CANONICAL_PAYLOAD_COLS)
    return hashlib.sha256(raw.encode()).hexdigest()


def check_file_schema(df: pd.DataFrame) -> None:
    """
    Layer 1: Verify the DataFrame has exactly the expected columns.
    Raises FileSchemaError listing missing and unexpected columns.
    """
    actual = set(df.columns) - {"run_id", "source_file", "source_fingerprint"}
    missing = EXPECTED_COLUMNS - actual
    extra = actual - EXPECTED_COLUMNS
    if missing or extra:
        raise FileSchemaError(
            f"Incompatible file schema. Missing: {sorted(missing)}. Extra: {sorted(extra)}"
        )


def validate_records(
    df: pd.DataFrame,
    existing_event_ids: dict[str, str] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Layer 2: Validate each record.

    Parameters
    ----------
    df : DataFrame from ingest (all columns as str)
    existing_event_ids : dict mapping event_id → payload_hash already in the lake

    Returns
    -------
    good_df       : valid records ready for transform
    rejects_df    : invalid + conflicting records with reject_reason
    replays_df    : duplicate replay records (same id + same payload, already published)
    """
    if existing_event_ids is None:
        existing_event_ids = {}

    good_rows = []
    reject_rows = []
    replay_rows = []

    # Track event_ids seen within this batch to catch intra-batch duplicates
    batch_seen: dict[str, str] = {}

    for _, row in df.iterrows():
        reason = _record_reason(row)
        if reason:
            r = row.to_dict()
            r["reject_reason"] = reason
            reject_rows.append(r)
            continue

        payload_hash = _payload_hash(row)
        eid = str(row["event_id"]).strip()

        # Check against lake (cross-run)
        if eid in existing_event_ids:
            if existing_event_ids[eid] == payload_hash:
                replay_rows.append(row.to_dict())
            else:
                r = row.to_dict()
                r["reject_reason"] = "conflicting_event_id"
                reject_rows.append(r)
            continue

        # Check within this batch
        if eid in batch_seen:
            if batch_seen[eid] == payload_hash:
                replay_rows.append(row.to_dict())
            else:
                r = row.to_dict()
                r["reject_reason"] = "conflicting_event_id"
                reject_rows.append(r)
            continue

        batch_seen[eid] = payload_hash
        r = row.to_dict()
        r["payload_hash"] = payload_hash  # carry forward so publish.py reuses the same hash
        good_rows.append(r)

    base_cols = df.columns.tolist()
    good_df    = pd.DataFrame(good_rows)    if good_rows    else pd.DataFrame(columns=base_cols)
    rejects_df = pd.DataFrame(reject_rows)  if reject_rows  else pd.DataFrame(columns=base_cols + ["reject_reason"])
    replays_df = pd.DataFrame(replay_rows)  if replay_rows  else pd.DataFrame(columns=base_cols)

    return good_df, rejects_df, replays_df


def _record_reason(row: pd.Series) -> str | None:
    """Return a reject reason string, or None if the record is valid."""
    eid = str(row.get("event_id", "")).strip()
    if not eid:
        return "missing_event_id"

    order_id = str(row.get("order_id", "")).strip()
    if not order_id:
        return "missing_order_id"

    customer_id = str(row.get("customer_id", "")).strip()
    if not customer_id.startswith("cust_"):
        return "invalid_customer_id_format"

    product_id = str(row.get("product_id", "")).strip()
    if not product_id.startswith("prod_"):
        return "invalid_product_id_format"

    event_type = str(row.get("event_type", "")).strip()
    if event_type not in VALID_EVENT_TYPES:
        return f"invalid_event_type:{event_type}"

    region = str(row.get("region", "")).strip()
    if region not in VALID_REGIONS:
        return f"invalid_region:{region}"

    try:
        qty = int(float(str(row.get("quantity", 0))))
        if qty <= 0:
            return "quantity_not_positive"
    except (ValueError, TypeError):
        return "invalid_quantity"

    try:
        price = float(str(row.get("unit_price", -1)))
        if price < 0:
            return "unit_price_negative"
    except (ValueError, TypeError):
        return "invalid_unit_price"

    ts = str(row.get("event_timestamp", "")).strip()
    try:
        pd.Timestamp(ts)
    except Exception:
        return f"invalid_timestamp:{ts}"

    return None
