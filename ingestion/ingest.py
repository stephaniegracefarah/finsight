# ingestion/ingest.py

import os
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from ingestion.edgar_client import EdgarClient

# ------------------------------------------------------------------
# Config
# ------------------------------------------------------------------

TICKERS = ["LYV", "SEAT", "EB", "MSGE", "STUB", "SPHR"]

RAW_DIR = Path("data/raw")
LOG_DIR = Path("data/logs")

# ------------------------------------------------------------------
# Logging setup
# ------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def save_json(data: dict, path: Path) -> None:
    """Write a dict to a JSON file, creating parent dirs if needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    logger.info(f"Saved → {path}  ({path.stat().st_size / 1024:.1f} KB)")


def write_log(log_entries: list[dict]) -> None:
    """Append a run summary to data/logs/ingestion_log.json."""
    log_path = LOG_DIR / "ingestion_log.json"
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    # Load existing log if it exists
    existing = []
    if log_path.exists():
        with open(log_path) as f:
            existing = json.load(f)

    existing.extend(log_entries)

    with open(log_path, "w") as f:
        json.dump(existing, f, indent=2)

    logger.info(f"Log updated → {log_path}")


# ------------------------------------------------------------------
# Core ingestion logic
# ------------------------------------------------------------------

def ingest_ticker(client: EdgarClient, ticker: str) -> dict:
    """
    Fetch and save all EDGAR data for one ticker.

    Returns a log entry dict describing what happened.
    """
    log_entry = {
        "ticker": ticker,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "status": None,
        "cik": None,
        "submissions_file": None,
        "facts_file": None,
        "facts_size_kb": None,
        "error": None,
    }

    try:
        # Step 1 — resolve ticker to CIK
        logger.info(f"[{ticker}] Resolving CIK...")
        cik = client.get_cik(ticker)
        log_entry["cik"] = cik
        logger.info(f"[{ticker}] CIK = {cik}")

        # Step 2 — fetch submissions (filing metadata)
        logger.info(f"[{ticker}] Fetching submissions...")
        submissions = client.get_submissions(cik)
        submissions_path = RAW_DIR / ticker / "submissions.json"
        save_json(submissions, submissions_path)
        log_entry["submissions_file"] = str(submissions_path)

        # Step 3 — fetch company facts (the actual financial numbers)
        logger.info(f"[{ticker}] Fetching company facts (this may take a moment)...")
        facts = client.get_company_facts(cik)
        facts_path = RAW_DIR / ticker / "company_facts.json"
        save_json(facts, facts_path)
        log_entry["facts_file"] = str(facts_path)
        log_entry["facts_size_kb"] = round(facts_path.stat().st_size / 1024, 1)

        log_entry["status"] = "success"
        logger.info(f"[{ticker}] ✓ Done\n")

    except Exception as e:
        log_entry["status"] = "error"
        log_entry["error"] = str(e)
        logger.error(f"[{ticker}] ✗ Failed: {e}\n")

    return log_entry


# ------------------------------------------------------------------
# Entry point
# ------------------------------------------------------------------

def main():
    logger.info("=" * 50)
    logger.info("FinSight EDGAR Ingestion — starting run")
    logger.info(f"Tickers: {TICKERS}")
    logger.info("=" * 50 + "\n")

    client = EdgarClient()
    log_entries = []

    for ticker in TICKERS:
        entry = ingest_ticker(client, ticker)
        log_entries.append(entry)

    write_log(log_entries)

    # Print summary
    success = [e for e in log_entries if e["status"] == "success"]
    errors = [e for e in log_entries if e["status"] == "error"]

    logger.info("=" * 50)
    logger.info(f"Run complete — {len(success)} succeeded, {len(errors)} failed")
    if errors:
        for e in errors:
            logger.warning(f"  ✗ {e['ticker']}: {e['error']}")
    logger.info("=" * 50)


if __name__ == "__main__":
    main()