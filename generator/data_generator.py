"""
Synthetic data generator for the data lake pipeline.

event_id stability:
    Each event gets a deterministic UUID derived from
    SHA-256(order_id + event_type + event_timestamp).
    This means re-running the generator with the same seed
    always produces the same event_ids — reproducible and stable.

Usage:
    python -m generator.data_generator --rows 100000 --output sample_data/events_large.csv
    python -m generator.data_generator --rows 1000   --output sample_data/events_small.csv
"""
import argparse
import hashlib
import uuid
from pathlib import Path

import pandas as pd
import numpy as np
from faker import Faker

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import (
    RANDOM_SEED, LARGE_DATASET_ROWS, DATE_START, DATE_END,
    VALID_EVENT_TYPES, VALID_REGIONS, SAMPLE_DATA_DIR,
)

fake = Faker()
Faker.seed(RANDOM_SEED)
rng = np.random.default_rng(RANDOM_SEED)


def make_event_id(order_id: str, event_type: str, event_timestamp: str) -> str:
    """Deterministic event_id: SHA-256 of (order_id|event_type|event_timestamp) → UUID5 namespace."""
    raw = f"{order_id}|{event_type}|{event_timestamp}"
    digest = hashlib.sha256(raw.encode()).hexdigest()
    # Use first 32 hex chars to form a UUID
    return str(uuid.UUID(digest[:32]))


def generate(n_rows: int = LARGE_DATASET_ROWS, seed: int = RANDOM_SEED) -> pd.DataFrame:
    """
    Generate n_rows synthetic e-commerce order events.
    Covers DATE_START through DATE_END (90 days by default).
    Includes ~3% deliberately invalid records for testing.
    """
    Faker.seed(seed)
    local_rng = np.random.default_rng(seed)

    event_types = list(VALID_EVENT_TYPES)
    regions = list(VALID_REGIONS)

    # Generate base order and customer pools
    n_orders = max(n_rows // 4, 100)
    n_customers = max(n_rows // 10, 50)
    n_products = max(n_rows // 20, 20)

    order_ids = [f"ord_{fake.uuid4()[:8]}" for _ in range(n_orders)]
    customer_ids = [f"cust_{str(i).zfill(4)}" for i in range(n_customers)]
    product_ids = [f"prod_{str(i).zfill(4)}" for i in range(n_products)]

    date_range = pd.date_range(DATE_START, DATE_END, freq="s")

    rows = []
    for _ in range(n_rows):
        order_id = order_ids[local_rng.integers(0, len(order_ids))]
        customer_id = customer_ids[local_rng.integers(0, len(customer_ids))]
        product_id = product_ids[local_rng.integers(0, len(product_ids))]
        event_type = event_types[local_rng.integers(0, len(event_types))]
        ts = date_range[local_rng.integers(0, len(date_range))]
        event_timestamp = ts.strftime("%Y-%m-%dT%H:%M:%S")
        quantity = int(local_rng.integers(1, 20))
        unit_price = round(float(local_rng.uniform(1.0, 500.0)), 2)
        region = regions[local_rng.integers(0, len(regions))]
        event_id = make_event_id(order_id, event_type, event_timestamp)

        rows.append({
            "event_id": event_id,
            "order_id": order_id,
            "customer_id": customer_id,
            "event_type": event_type,
            "event_timestamp": event_timestamp,
            "product_id": product_id,
            "quantity": quantity,
            "unit_price": unit_price,
            "region": region,
        })

    df = pd.DataFrame(rows)

    # Inject ~3% invalid records (labelled separately — not mixed silently)
    n_invalid = max(int(n_rows * 0.03), 10)
    invalid_indices = local_rng.choice(len(df), size=n_invalid, replace=False)

    # Rotate through different fault types
    fault_types = ["missing_event_id", "bad_event_type", "negative_quantity",
                   "bad_timestamp", "bad_customer_id", "bad_product_id", "missing_region"]
    for i, idx in enumerate(invalid_indices):
        fault = fault_types[i % len(fault_types)]
        if fault == "missing_event_id":
            df.at[idx, "event_id"] = ""
        elif fault == "bad_event_type":
            df.at[idx, "event_type"] = "REFUNDED"
        elif fault == "negative_quantity":
            df.at[idx, "quantity"] = -1
        elif fault == "bad_timestamp":
            df.at[idx, "event_timestamp"] = "not-a-date"
        elif fault == "bad_customer_id":
            df.at[idx, "customer_id"] = "BADCUST"
        elif fault == "bad_product_id":
            df.at[idx, "product_id"] = "BADPROD"
        elif fault == "missing_region":
            df.at[idx, "region"] = ""

    return df


def save(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    print(f"Saved {len(df):,} rows to {path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic event data")
    parser.add_argument("--rows", type=int, default=LARGE_DATASET_ROWS)
    parser.add_argument("--output", type=str, default=str(SAMPLE_DATA_DIR / "events_large.csv"))
    parser.add_argument("--seed", type=int, default=RANDOM_SEED)
    args = parser.parse_args()
    df = generate(n_rows=args.rows, seed=args.seed)
    save(df, Path(args.output))
