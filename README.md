# Data Lake Pipeline

A time-partitioned data lake pipeline built with Python, Pandas, and PyArrow.
Demonstrates partition pruning, avoids small files, and handles data quality end-to-end.

---

## Architecture

```
Raw CSV (landing zone)
        |
        v
  [ Ingest ]         reads CSV, computes SHA-256 fingerprint, attaches run_id
        |
        v
  [ File Schema ]    checks expected columns — rejects whole file if incompatible
        |
        v
  [ Validate ]       record-level checks — good / rejects / duplicate replays
        |
   _____|_____
  |           |
  v           v
[Good]     [Rejects] --> output/rejects/<run_id>_rejects.csv
  |
  v
[ Transform ]        parse timestamps, derive total_value, extract year/month/day
  |
  v
[ Publish ]          write-to-staging -> validate -> atomic move -> commit manifest
  |
  v
[ Reconcile ]        input = newly_published + rejected + duplicate_replays
  |
  v
[ Log Metrics ]      output/logs/<run_id>.json
```

---

## Project Structure

```
data_lake_pipeline/
├── config.py                   # all paths and parameters
├── run_pipeline.py             # main pipeline entry point
├── pruning_demo.py             # partition pruning benchmark
├── compaction.py               # small-file compaction
├── requirements.txt
├── .gitignore
├── generator/
│   └── data_generator.py       # synthetic data with fixed seed
├── pipeline/
│   ├── ingest.py               # read CSV + SHA-256 fingerprint
│   ├── validate.py             # file schema + record validation
│   ├── transform.py            # enrich + partition keys
│   ├── publish.py              # safe publication + commit manifest
│   └── rejects.py              # write reject CSV
├── utils/
│   ├── logger.py               # structured run metrics
│   └── reconcile.py            # reconciliation equation
├── tests/
│   └── test_pipeline.py        # full pytest suite
├── sample_data/                # generated CSV files (gitignored)
└── output/                     # pipeline outputs (gitignored except .gitkeep)
    ├── lake/                   # Hive-partitioned Parquet
    ├── rejects/
    ├── logs/
    ├── manifests/
    ├── reconciliation/
    └── staging/                # temporary — always empty after a successful run
```

---

## Setup

```bash
# 1. Clone / enter the project
cd data_lake_pipeline

# 2. Create and activate virtual environment
python -m venv venv

# Windows
venv\Scripts\activate

# macOS / Linux
source venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt
```

---

## Commands

### Generate data
```bash
# Small sample (1,000 rows) — quick review
python -m generator.data_generator --rows 1000 --output sample_data/events_small.csv

# Large dataset (100,000 rows) — full benchmark
python -m generator.data_generator --rows 100000 --output sample_data/events_large.csv
```

### Run the pipeline
```bash
python run_pipeline.py --input sample_data/events_large.csv
```

### Run tests
```bash
pytest tests/ -v
```

### Run pruning benchmark
```bash
# Requires the large dataset to be in the lake first
python run_pipeline.py --input sample_data/events_large.csv
python pruning_demo.py
# Results saved to output/pruning_benchmark.txt
```

### Run compaction
```bash
python compaction.py
# Results saved to output/compaction_report.txt

# Compact a specific partition only
python compaction.py --partition "year=2024/month=1/day=15"
```

---

## Data Dictionary

### Source Schema (input CSV)

| Column | Type | Nullable | Unique | Rule |
|--------|------|----------|--------|------|
| `event_id` | string | NO | YES | Deterministic UUID from SHA-256(order_id\|event_type\|event_timestamp) |
| `order_id` | string | NO | NO | One order can have many events |
| `customer_id` | string | NO | NO | Must match `cust_XXXX` format |
| `event_type` | string | NO | NO | One of: placed, shipped, delivered, cancelled |
| `event_timestamp` | ISO-8601 | NO | NO | Parsed to UTC |
| `product_id` | string | NO | NO | Must match `prod_XXXX` format |
| `quantity` | integer | NO | NO | Must be > 0 |
| `unit_price` | float | NO | NO | Must be >= 0 |
| `region` | string | NO | NO | One of: NA, EU, APAC, LATAM |

### Target Schema (Parquet lake)

All source columns plus:

| Column | Type | Description |
|--------|------|-------------|
| `total_value` | float | quantity x unit_price |
| `year` | integer | Partition key — from event_timestamp |
| `month` | integer | Partition key — from event_timestamp |
| `day` | integer | Partition key — from event_timestamp |
| `run_id` | string | Pipeline run that wrote this record |
| `source_file` | string | Input filename |
| `source_fingerprint` | string | SHA-256 of input file |
| `payload_hash` | string | SHA-256 of canonical payload fields — used for conflict detection |
| `ingested_at` | string | UTC timestamp when pipeline processed this record |

---

## Naming Conventions

| Convention | Choice | Reason |
|------------|--------|--------|
| Column names | `snake_case` | Python/SQL standard |
| Partition dirs | `year=`, `month=`, `day=` | Hive-style — native to PyArrow, Spark, Athena |
| Timestamps | ISO-8601 UTC | Unambiguous, sortable |
| Run ID | UUID4 (first 8 chars) | Unique, short enough for filenames |
| Source tracking | `source_file` + `source_fingerprint` | Enables idempotency and traceability |
| Reject files | `<run_id>_rejects.csv` | Tied to run — every reject is traceable |
| Log files | `<run_id>.json` | One JSON log per run, machine-readable |
| Manifests | `<run_id>.json` | Presence = run committed |

---

## Partitioning Strategy

Layout: `output/lake/year=2024/month=3/day=15/run_<id>_part_0000.parquet`

PyArrow prunes partitions reliably when filters use the actual Hive partition
columns (`year`, `month`, `day`) as integers — not `event_timestamp`.

```python
import pyarrow.dataset as ds

dataset = ds.dataset("output/lake", format="parquet", partitioning="hive")
march_filter = (ds.field("year") == 2024) & (ds.field("month") == 3)

selected_fragments = list(dataset.get_fragments(filter=march_filter))
result = dataset.to_table(filter=march_filter, columns=["event_id", "total_value"])
```

`selected_fragments` = files chosen by partition directory metadata.
Physically read files may differ and are not measured from Python alone.

### Query Pattern Trade-offs

| Query | Pruning | Note |
|-------|---------|------|
| `year=2024 AND month=3 AND day=15` | Excellent | 1 partition folder |
| `year=2024 AND month=3` | Good | All day folders in March |
| `year=2024` | Moderate | All 365 day folders |
| `region = 'NA'` | None | Not a partition key — column pruning only |
| `event_type = 'delivered'` | None | Not a partition key — column pruning only |
| Date range across days | Good | Opens only matching day folders |
| `ORDER BY timestamp DESC LIMIT N` | None | Must open all partitions |

---

## Idempotency and Safe Publication

**Protocol:**
1. Compute SHA-256 fingerprint of input file
2. Write all output to `output/staging/<run_id>/`
3. Validate staged files (row count check)
4. Move staged files to final lake partitions
5. Write commit manifest to `output/manifests/<run_id>.json`
6. Mark run `success` only after manifest is written

**Atomicity note:**
`os.replace()` is atomic on POSIX within the same filesystem.
On Windows, `shutil.move` is used which is copy+delete — not atomic.
The manifest is the source of truth: no manifest = run not committed.
A crash between move and manifest leaves orphaned files that are safe
to delete manually (they will not be read by any committed query).

**Duplicate handling:**
- Same `event_id` + same payload → duplicate replay (skipped, counted)
- Same `event_id` + different payload → conflicting_event_id (quarantined in rejects)
- Same `order_id`, different `event_id` → valid history, both kept

---

## Reconciliation

Every run enforces:
```
input_rows = newly_published_rows + rejected_rows + duplicate_replay_rows
```

`conflicting_rows` is included in `rejected_rows` — it is a diagnostic subset, not a separate category.

`total_published_rows` = all rows in the lake across all runs (reported separately).

---

## File Sizing

Target: ~128 MB per Parquet file. This is a guideline, not a guarantee.
Small partitions (few events on a given day) will produce smaller files — this is expected.
Actual file sizes are measured and reported in `output/compaction_report.txt`.

---

## Known Limitations

1. `shutil.move` on Windows is not atomic — a crash mid-publication can leave orphaned staging files. These are safe to delete; the manifest is the source of truth.
2. `get_existing_event_ids()` loads all event_ids into memory — will not scale beyond ~50M unique events on a typical laptop.
3. Reference validation checks `prod_XXX` / `cust_XXX` format only — not actual existence in a reference dataset (none available).
4. No orchestration — runs are triggered manually or via a simple script loop.
5. 128 MB file target is approximate — actual sizes depend on data volume per partition.

---
## Evaluation Checklist

| Criterion | Status |
|-----------|--------|
| Partitioning strategy documented | Done |
| Pruning demonstrated with measured numbers | Done — `output/pruning_benchmark.txt` |
| Small file avoidance | Done — compaction + file sizing |
| Query pattern trade-offs documented | Done — table above |
| Validation (file + record level) | Done |
| Duplicate replay detection | Done |
| Conflict quarantine | Done |
| Safe publication (staging + manifest) | Done |
| Idempotency | Done |
| Reconciliation equation | Done |
| Structured run logs | Done |
| Automated tests | Done — 30+ test cases |
| Reproducible synthetic data | Done — fixed seed |
| Compaction with equivalence check | Done |
| README + data dictionary | Done |


-----
## Benchmarking Results

The pipeline includes two benchmarks to demonstrate partition pruning and small-file compaction using Hive-partitioned Parquet data.

### 1. Partition Pruning Benchmark

**Test configuration**
- Dataset: `output/lake`
- Filter: `year = 2024 AND month = 3`
- Total files discovered: 90

| Metric | Full Scan | Pruned Scan |
|---|---:|---:|
| Selected files | 90 | 30 |
| Selected file size | 11.431 MB | 3.801 MB |
| Returned rows | 72,443 | 24,061 |
| Execution time | 0.0358 s | 0.0115 s |

**Results**
- Files eliminated by pruning: **66.7%**
- The filtered result was verified as a subset of the full-scan result.
- The pruned scan selected one-third of the files and approximately one-third of the data size.

Execution times are from a local benchmark run and may vary with the environment, filesystem cache, and dataset size. The scans return different numbers of rows, so the timings should not be interpreted as a strict like-for-like performance comparison.

The benchmark measures files selected using partition metadata; it does not directly measure the number of files physically read.

### 2. Small-File Compaction Benchmark

| Metric | Before | After |
|---|---:|---:|
| Files | 180 | 90 |
| Total file size | 14.565 MB | 13.497 MB |
| Rows | 87,045 | 87,045 |

**Results**
- File count reduced by **50%**.
- Total file size reduced by approximately **7.3%**.
- Data equivalence check: **PASS**.

Compaction merges small files within partitions while preserving the data. The 128 MB target is a guideline rather than a guarantee; small partitions may naturally produce smaller files.

### Reproducing the Benchmarks

Generate data and run the pipeline before executing the pruning benchmark:

```bash
python -m generator.data_generator --rows 100000 --output sample_data/events_large.csv
python run_pipeline.py --input sample_data/events_large.csv
python pruning_demo.py
```

Run compaction separately:

```bash
python compaction.py
```

The reports are generated under `output/`. Benchmark results depend on the dataset and execution environment.