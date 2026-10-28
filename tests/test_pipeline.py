"""
Full test suite for the data lake pipeline.

Covers:
    - Data generator determinism and fingerprints
    - File schema validation (incompatible file -> whole file rejected)
    - Record validation: valid, missing fields, bad types, bad formats
    - Duplicate replay (same event_id + same payload)
    - Conflicting event_id (same event_id + different payload)
    - Same order_id with different event_ids -> both kept
    - Empty input, all-invalid batch
    - Transform correctness
    - Publication and commit manifest
    - Publication failure recovery (staging cleaned up)
    - Cross-run duplicate detection
    - Late-arriving events
    - Reconciliation equation
    - Pruning: filtered result is subset of full result
    - Compaction equivalence
"""
import hashlib
from pathlib import Path

import pandas as pd
import pytest
import pyarrow.dataset as ds

import sys
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from config import EXPECTED_COLUMNS
from generator.data_generator import generate, make_event_id
from pipeline.ingest import ingest, compute_fingerprint
from pipeline.validate import check_file_schema, validate_records, FileSchemaError
from pipeline.transform import transform
from pipeline.rejects import write_rejects
from utils.reconcile import reconcile, ReconciliationError


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture()
def tmp_lake(tmp_path, monkeypatch):
    """Redirect all output dirs to tmp for test isolation."""
    import config as cfg
    monkeypatch.setattr(cfg, "LAKE_DIR", tmp_path / "lake")
    monkeypatch.setattr(cfg, "STAGING_DIR", tmp_path / "staging")
    monkeypatch.setattr(cfg, "REJECTS_DIR", tmp_path / "rejects")
    monkeypatch.setattr(cfg, "LOGS_DIR", tmp_path / "logs")
    monkeypatch.setattr(cfg, "MANIFESTS_DIR", tmp_path / "manifests")
    monkeypatch.setattr(cfg, "RECONCILIATION_DIR", tmp_path / "reconciliation")

    import pipeline.publish as pub_mod
    monkeypatch.setattr(pub_mod, "LAKE_DIR", tmp_path / "lake")
    monkeypatch.setattr(pub_mod, "STAGING_DIR", tmp_path / "staging")
    monkeypatch.setattr(pub_mod, "MANIFESTS_DIR", tmp_path / "manifests")

    import utils.reconcile as rec_mod
    monkeypatch.setattr(rec_mod, "RECONCILIATION_DIR", tmp_path / "reconciliation")

    return tmp_path


def _make_valid_row(
    event_id=None, order_id="ord_abc123", customer_id="cust_0001",
    event_type="placed", event_timestamp="2024-03-15T10:00:00",
    product_id="prod_0001", quantity="2", unit_price="49.99", region="NA",
):
    # Only generate event_id if not explicitly provided (including empty string)
    if event_id is None:
        eid = make_event_id(order_id, event_type, event_timestamp)
    else:
        eid = event_id
    return {
        "event_id": eid, "order_id": order_id, "customer_id": customer_id,
        "event_type": event_type, "event_timestamp": event_timestamp,
        "product_id": product_id, "quantity": quantity,
        "unit_price": unit_price, "region": region,
        "run_id": "test_run", "source_file": "test.csv",
        "source_fingerprint": "abc123",
    }


def _df(*rows):
    return pd.DataFrame(rows)


# ── Generator tests ───────────────────────────────────────────────────────────

def test_generator_determinism():
    df1 = generate(n_rows=100, seed=42)
    df2 = generate(n_rows=100, seed=42)
    assert list(df1["event_id"]) == list(df2["event_id"])


def test_generator_different_seeds_differ():
    df1 = generate(n_rows=100, seed=42)
    df2 = generate(n_rows=100, seed=99)
    assert list(df1["event_id"]) != list(df2["event_id"])


def test_event_id_determinism():
    eid1 = make_event_id("ord_1", "placed", "2024-01-01T00:00:00")
    eid2 = make_event_id("ord_1", "placed", "2024-01-01T00:00:00")
    assert eid1 == eid2


def test_event_id_changes_with_input():
    eid1 = make_event_id("ord_1", "placed", "2024-01-01T00:00:00")
    eid2 = make_event_id("ord_1", "shipped", "2024-01-01T00:00:00")
    assert eid1 != eid2


def test_generator_covers_90_days():
    df = generate(n_rows=1000, seed=42)
    # Filter out deliberately injected invalid timestamps before parsing
    df = df[df["event_timestamp"] != "not-a-date"]
    df["ts"] = pd.to_datetime(df["event_timestamp"], errors="coerce")
    df = df.dropna(subset=["ts"])
    assert df["ts"].dt.date.min() >= pd.Timestamp("2024-01-01").date()
    assert df["ts"].dt.date.max() <= pd.Timestamp("2024-03-31").date()


def test_source_fingerprint_stable(tmp_path):
    csv = tmp_path / "test.csv"
    generate(n_rows=100, seed=42).to_csv(csv, index=False)
    assert compute_fingerprint(csv) == compute_fingerprint(csv)
    assert len(compute_fingerprint(csv)) == 64


def test_source_fingerprint_changes_with_content(tmp_path):
    csv1 = tmp_path / "a.csv"
    csv2 = tmp_path / "b.csv"
    generate(n_rows=100, seed=42).to_csv(csv1, index=False)
    generate(n_rows=100, seed=99).to_csv(csv2, index=False)
    assert compute_fingerprint(csv1) != compute_fingerprint(csv2)


# ── File schema tests ─────────────────────────────────────────────────────────

def test_file_schema_valid():
    check_file_schema(_df(_make_valid_row()))  # must not raise


def test_file_schema_missing_column():
    df = _df(_make_valid_row()).drop(columns=["event_id"])
    with pytest.raises(FileSchemaError, match="Missing"):
        check_file_schema(df)


def test_file_schema_extra_column():
    df = _df(_make_valid_row())
    df["unexpected_col"] = "x"
    with pytest.raises(FileSchemaError, match="Extra"):
        check_file_schema(df)


def test_file_schema_renamed_column():
    df = _df(_make_valid_row()).rename(columns={"event_id": "evt_id"})
    with pytest.raises(FileSchemaError):
        check_file_schema(df)


# ── Record validation tests ───────────────────────────────────────────────────

def test_valid_record_passes():
    good, rejects, replays = validate_records(_df(_make_valid_row()))
    assert len(good) == 1 and len(rejects) == 0 and len(replays) == 0


def test_missing_event_id_rejected():
    _, rejects, _ = validate_records(_df(_make_valid_row(event_id="")))
    assert rejects.iloc[0]["reject_reason"] == "missing_event_id"


def test_invalid_event_type_rejected():
    _, rejects, _ = validate_records(_df(_make_valid_row(event_type="REFUNDED")))
    assert "invalid_event_type" in rejects.iloc[0]["reject_reason"]


def test_negative_quantity_rejected():
    _, rejects, _ = validate_records(_df(_make_valid_row(quantity="-5")))
    assert rejects.iloc[0]["reject_reason"] == "quantity_not_positive"


def test_invalid_timestamp_rejected():
    _, rejects, _ = validate_records(_df(_make_valid_row(event_timestamp="not-a-date")))
    assert "invalid_timestamp" in rejects.iloc[0]["reject_reason"]


def test_invalid_customer_id_format():
    _, rejects, _ = validate_records(_df(_make_valid_row(customer_id="BADCUST")))
    assert rejects.iloc[0]["reject_reason"] == "invalid_customer_id_format"


def test_invalid_product_id_format():
    _, rejects, _ = validate_records(_df(_make_valid_row(product_id="BADPROD")))
    assert rejects.iloc[0]["reject_reason"] == "invalid_product_id_format"


def test_invalid_region_rejected():
    _, rejects, _ = validate_records(_df(_make_valid_row(region="MOON")))
    assert "invalid_region" in rejects.iloc[0]["reject_reason"]


def test_empty_dataframe():
    df = pd.DataFrame(columns=list(EXPECTED_COLUMNS) + ["run_id", "source_file", "source_fingerprint"])
    good, rejects, replays = validate_records(df)
    assert len(good) == 0 and len(rejects) == 0 and len(replays) == 0


def test_all_invalid_batch():
    rows = [
        _make_valid_row(event_id=""),
        _make_valid_row(event_type="UNKNOWN"),
        _make_valid_row(quantity="-1"),
    ]
    good, rejects, _ = validate_records(_df(*rows))
    assert len(good) == 0 and len(rejects) == 3


def test_mixed_valid_invalid_batch():
    rows = [_make_valid_row(), _make_valid_row(event_id="")]
    good, rejects, replays = validate_records(_df(*rows))
    assert len(good) == 1 and len(rejects) == 1 and len(replays) == 0


# ── Duplicate and conflict tests ──────────────────────────────────────────────

def test_duplicate_replay_same_batch():
    row = _make_valid_row()
    good, rejects, replays = validate_records(_df(row, row))
    assert len(good) == 1 and len(replays) == 1 and len(rejects) == 0


def test_conflicting_event_id_same_batch():
    row1 = _make_valid_row(quantity="2")
    row2 = {**row1, "quantity": "99"}
    good, rejects, replays = validate_records(_df(row1, row2))
    assert len(good) == 1 and len(rejects) == 1
    assert rejects.iloc[0]["reject_reason"] == "conflicting_event_id"


def test_same_order_id_different_event_ids_both_kept():
    """Different event_ids must not be discarded just because order_id matches."""
    row1 = _make_valid_row(event_type="placed",  event_timestamp="2024-03-01T10:00:00")
    row2 = _make_valid_row(event_type="shipped", event_timestamp="2024-03-02T10:00:00")
    good, rejects, replays = validate_records(_df(row1, row2))
    assert len(good) == 2 and len(rejects) == 0


def test_cross_run_duplicate_replay():
    row = _make_valid_row()
    payload_hash = hashlib.sha256(
        "|".join(str(row[c]) for c in [
            "order_id", "customer_id", "event_type", "event_timestamp",
            "product_id", "quantity", "unit_price", "region",
        ]).encode()
    ).hexdigest()
    existing = {row["event_id"]: payload_hash}
    good, rejects, replays = validate_records(_df(row), existing_event_ids=existing)
    assert len(good) == 0 and len(replays) == 1


def test_cross_run_conflicting_event_id():
    row = _make_valid_row(quantity="2")
    existing = {row["event_id"]: "completely_different_hash"}
    good, rejects, replays = validate_records(_df(row), existing_event_ids=existing)
    assert len(rejects) == 1
    assert rejects.iloc[0]["reject_reason"] == "conflicting_event_id"


# ── Transform tests ───────────────────────────────────────────────────────────

def test_transform_total_value():
    good, _, _ = validate_records(_df(_make_valid_row(quantity="3", unit_price="10.00")))
    result = transform(good)
    assert result.iloc[0]["total_value"] == pytest.approx(30.0)


def test_transform_partition_keys():
    good, _, _ = validate_records(_df(_make_valid_row(event_timestamp="2024-03-15T10:00:00")))
    result = transform(good)
    assert result.iloc[0]["year"] == 2024
    assert result.iloc[0]["month"] == 3
    assert result.iloc[0]["day"] == 15


def test_transform_late_arriving_event():
    """Late event (Jan date) must go to year=2024/month=1/day=5 partition."""
    good, _, _ = validate_records(_df(_make_valid_row(event_timestamp="2024-01-05T08:00:00")))
    result = transform(good)
    assert result.iloc[0]["year"] == 2024
    assert result.iloc[0]["month"] == 1
    assert result.iloc[0]["day"] == 5


def test_transform_empty_df():
    assert transform(pd.DataFrame()).empty


# ── Publication tests ─────────────────────────────────────────────────────────

def test_publish_creates_parquet_and_manifest(tmp_lake):
    import pipeline.publish as pub_mod
    good, _, _ = validate_records(_df(_make_valid_row()))
    result = pub_mod.publish(transform(good), "testrun1", "test.csv", "fp123")
    assert result["newly_published_rows"] == 1
    assert any((tmp_lake / "lake").rglob("*.parquet"))
    assert (tmp_lake / "manifests" / "testrun1.json").exists()


def test_publish_staging_cleaned_on_success(tmp_lake):
    import pipeline.publish as pub_mod
    good, _, _ = validate_records(_df(_make_valid_row()))
    pub_mod.publish(transform(good), "run_clean", "test.csv", "fp")
    assert not (tmp_lake / "staging" / "run_clean").exists()


def test_publish_failure_leaves_lake_untouched(tmp_lake, monkeypatch):
    """Injected validation failure must not touch the lake."""
    import pipeline.publish as pub_mod

    monkeypatch.setattr(pub_mod, "_validate_staged", lambda *a, **k: (_ for _ in ()).throw(ValueError("Injected")))

    good, _, _ = validate_records(_df(_make_valid_row()))
    with pytest.raises(Exception):
        pub_mod.publish(transform(good), "run_fail", "test.csv", "fp")

    assert not any((tmp_lake / "lake").rglob("*.parquet"))
    assert not (tmp_lake / "staging" / "run_fail").exists()


# ── Reconciliation tests ──────────────────────────────────────────────────────

def test_reconciliation_passes(tmp_lake):
    report = reconcile("rec_ok", 100, 90, 7, 3, 2, 90)
    assert "YES" in report


def test_reconciliation_fails_on_mismatch(tmp_lake):
    with pytest.raises(ReconciliationError):
        reconcile("rec_fail", 100, 90, 5, 3, 0, 90)  # 90+5+3=98 != 100


def test_conflicting_rows_subset_of_rejected(tmp_lake):
    """conflicting_rows <= rejected_rows — it is a diagnostic subset."""
    report = reconcile("conf_test", 10, 7, 2, 1, 1, 7)
    assert "YES" in report


# ── Ingest tests ──────────────────────────────────────────────────────────────

def test_ingest_missing_file():
    with pytest.raises(FileNotFoundError):
        ingest(Path("/nonexistent/file.csv"), "run1")


def test_ingest_empty_file(tmp_path):
    csv = tmp_path / "empty.csv"
    csv.write_text("event_id,order_id\n")
    with pytest.raises(ValueError, match="empty"):
        ingest(csv, "run1")


# ── Pruning test ──────────────────────────────────────────────────────────────

def test_pruning_filtered_is_subset_of_full(tmp_lake):
    import pipeline.publish as pub_mod

    rows = []
    for month, day in [(1, 10), (2, 5), (3, 1), (3, 15)]:
        ts = f"2024-{month:02d}-{day:02d}T10:00:00"
        rows.append(_make_valid_row(
            event_id=make_event_id(f"ord_{month}_{day}", "placed", ts),
            order_id=f"ord_{month}_{day}",
            event_timestamp=ts,
        ))

    good, _, _ = validate_records(pd.DataFrame(rows))
    pub_mod.publish(transform(good), "prune_test", "test.csv", "fp")

    lake_path = tmp_lake / "lake"
    dataset = ds.dataset(str(lake_path), format="parquet", partitioning="hive")

    full_ids = set(dataset.to_table(columns=["event_id"])["event_id"].to_pylist())
    march_filter = (ds.field("year") == 2024) & (ds.field("month") == 3)
    pruned_ids = set(
        dataset.to_table(filter=march_filter, columns=["event_id"])["event_id"].to_pylist()
    )

    assert pruned_ids.issubset(full_ids)
    assert len(pruned_ids) < len(full_ids)


# ── Compaction equivalence test ───────────────────────────────────────────────

def test_compaction_equivalence(tmp_lake, monkeypatch):
    """After compaction, same query returns identical event_ids."""
    import pipeline.publish as pub_mod
    import compaction as comp_mod

    monkeypatch.setattr(comp_mod, "LAKE_DIR",     tmp_lake / "lake")
    monkeypatch.setattr(comp_mod, "STAGING_DIR",  tmp_lake / "staging")
    monkeypatch.setattr(comp_mod, "MANIFESTS_DIR",tmp_lake / "manifests")
    monkeypatch.setattr(comp_mod, "OUTPUT_DIR",   tmp_lake)

    for i in range(3):
        ts = f"2024-03-15T{10+i:02d}:00:00"
        row = _make_valid_row(
            event_id=make_event_id(f"ord_comp_{i}", "placed", ts),
            order_id=f"ord_comp_{i}",
            event_timestamp=ts,
        )
        good, _, _ = validate_records(pd.DataFrame([row]))
        pub_mod.publish(transform(good), f"run_comp_{i}", "test.csv", f"fp_{i}")

    lake_path = tmp_lake / "lake"
    pre_ids = sorted(
        ds.dataset(str(lake_path), format="parquet", partitioning="hive")
        .to_table(columns=["event_id"])["event_id"].to_pylist()
    )

    comp_mod.run_compaction()

    post_ids = sorted(
        ds.dataset(str(lake_path), format="parquet", partitioning="hive")
        .to_table(columns=["event_id"])["event_id"].to_pylist()
    )

    assert pre_ids == post_ids
