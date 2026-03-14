import json
import logging
from pathlib import Path

import duckdb
import pandas as pd

# ------------------------------------------------------------------
# Config
# ------------------------------------------------------------------

RAW_DIR = Path("data/raw")
DB_PATH = Path("data/finsight_dev.duckdb")

TICKERS = ["LYV", "SEAT", "EB", "MSGE", "STUB", "SPHR"]

# The financial metrics we care about for anomaly detection
TARGET_METRICS = [
    "Revenues",
    "NetIncomeLoss",
    "Assets",
    "Liabilities",
    "StockholdersEquity",
    "OperatingIncomeLoss",
    "CashAndCashEquivalentsAtCarryingValue",
    "NetCashProvidedByUsedInOperatingActivities",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Loaders
# ------------------------------------------------------------------

def load_submissions(ticker: str) -> dict | None:
    """Load submissions.json for a ticker."""
    path = RAW_DIR / ticker / "submissions.json"
    if not path.exists():
        logger.warning(f"[{ticker}] submissions.json not found")
        return None
    with open(path) as f:
        return json.load(f)


def load_company_facts(ticker: str) -> dict | None:
    """Load company_facts.json for a ticker."""
    path = RAW_DIR / ticker / "company_facts.json"
    if not path.exists():
        logger.warning(f"[{ticker}] company_facts.json not found")
        return None
    with open(path) as f:
        return json.load(f)


# ------------------------------------------------------------------
# Transformers
# ------------------------------------------------------------------

def extract_submissions_row(ticker: str, data: dict) -> dict:
    """Pull key fields out of submissions.json into a flat dict."""
    return {
        "ticker": ticker,
        "cik": data.get("cik"),
        "entity_name": data.get("name"),
        "sic": data.get("sic"),
        "sic_description": data.get("sicDescription"),
        "category": data.get("category"),
        "fiscal_year_end": data.get("fiscalYearEnd"),
        "state_of_incorporation": data.get("stateOfIncorporation"),
    }


def extract_facts_rows(ticker: str, cik: str, data: dict) -> list[dict]:
    """
    Extract rows for each target metric from company_facts.json.

    Returns a flat list of dicts, one row per reported value.
    """
    rows = []
    us_gaap = data.get("facts", {}).get("us-gaap", {})

    for metric in TARGET_METRICS:
        if metric not in us_gaap:
            logger.debug(f"[{ticker}] Metric '{metric}' not found in facts")
            continue

        units = us_gaap[metric].get("units", {})
        usd_values = units.get("USD", [])

        for entry in usd_values:
            rows.append({
                "ticker": ticker,
                "cik": cik,
                "metric": metric,
                "val": entry.get("val"),
                "start": entry.get("start"),
                "end": entry.get("end"),
                "accn": entry.get("accn"),
                "form": entry.get("form"),
                "filed": entry.get("filed"),
                "frame": entry.get("frame"),
            })

    return rows


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main():
    logger.info("=" * 50)
    logger.info("FinSight — Loading raw data into DuckDB")
    logger.info(f"Database: {DB_PATH}")
    logger.info("=" * 50 + "\n")

    # Connect to DuckDB (creates file if it doesn't exist)
    con = duckdb.connect(str(DB_PATH))

    # ---- Load submissions ----
    logger.info("Loading submissions...")
    submissions_rows = []
    for ticker in TICKERS:
        data = load_submissions(ticker)
        if data:
            row = extract_submissions_row(ticker, data)
            submissions_rows.append(row)
            logger.info(f"  [{ticker}] ✓ {row['entity_name']}")

    submissions_df = pd.DataFrame(submissions_rows)
    con.execute("DROP TABLE IF EXISTS raw_submissions")
    con.execute("""
        CREATE TABLE raw_submissions AS
        SELECT * FROM submissions_df
    """)
    logger.info(f"  → raw_submissions: {len(submissions_df)} rows\n")

    # ---- Load company facts ----
    logger.info("Loading company facts...")
    all_facts_rows = []
    for ticker in TICKERS:
        data = load_company_facts(ticker)
        if data:
            cik = data.get("cik", "")
            rows = extract_facts_rows(ticker, cik, data)
            all_facts_rows.extend(rows)
            logger.info(f"  [{ticker}] ✓ {len(rows)} rows extracted")

    facts_df = pd.DataFrame(all_facts_rows)
    con.execute("DROP TABLE IF EXISTS raw_company_facts")
    con.execute("""
        CREATE TABLE raw_company_facts AS
        SELECT * FROM facts_df
    """)
    logger.info(f"  → raw_company_facts: {len(facts_df)} total rows\n")

    # ---- Verify ----
    logger.info("Verifying tables...")
    tables = con.execute("SHOW TABLES").fetchall()
    for table in tables:
        count = con.execute(f"SELECT COUNT(*) FROM {table[0]}").fetchone()[0]
        logger.info(f"  {table[0]}: {count:,} rows")

    con.close()
    logger.info("\nDone! Database ready for dbt.")


if __name__ == "__main__":
    main()