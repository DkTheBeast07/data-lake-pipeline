"""
Central configuration — all paths and parameters in one place.
Change BASE_DIR or OUTPUT_DIR to relocate the entire pipeline.
"""
from pathlib import Path

BASE_DIR = Path(__file__).parent
OUTPUT_DIR = BASE_DIR / "output"

# Directories
LAKE_DIR = OUTPUT_DIR / "lake"
STAGING_DIR = OUTPUT_DIR / "staging"
REJECTS_DIR = OUTPUT_DIR / "rejects"
LOGS_DIR = OUTPUT_DIR / "logs"
MANIFESTS_DIR = OUTPUT_DIR / "manifests"
RECONCILIATION_DIR = OUTPUT_DIR / "reconciliation"
SAMPLE_DATA_DIR = BASE_DIR / "sample_data"

# Data generation
RANDOM_SEED = 42
SMALL_DATASET_ROWS = 1_000
LARGE_DATASET_ROWS = 100_000
DATE_START = "2024-01-01"
DATE_END = "2024-03-31"  # 90 days

# Validation
VALID_EVENT_TYPES = {"placed", "shipped", "delivered", "cancelled"}
VALID_REGIONS = {"NA", "EU", "APAC", "LATAM"}
CUSTOMER_ID_PREFIX = "cust_"
PRODUCT_ID_PREFIX = "prod_"

# File sizing — target rows per Parquet file (~128 MB guideline)
# Actual sizes depend on data; we measure and report, not guarantee.
TARGET_ROWS_PER_FILE = 50_000

# Expected CSV columns (file-level schema check)
EXPECTED_COLUMNS = {
    "event_id",
    "order_id",
    "customer_id",
    "event_type",
    "event_timestamp",
    "product_id",
    "quantity",
    "unit_price",
    "region",
}
