"""
Public Data Pipeline — Socrata API → Local Storage
Demonstrates: API ingestion, incremental sync, structured output, error logging.

Uses Chicago's open building permits dataset (same Socrata API as Boston).
"""

import json
import logging
import os
from datetime import datetime, date
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

API_ENDPOINT = "https://data.cityofchicago.org/resource/ydr8-5enu.json"
STATE_FILE   = Path("last_sync.json")
OUTPUT_FILE  = Path("permits.json")
PAGE_SIZE    = 1000

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# State tracking — remember where we left off
# ---------------------------------------------------------------------------

def load_last_sync() -> str | None:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())["last_sync"]
    return None


def save_last_sync(ts: str):
    STATE_FILE.write_text(json.dumps({"last_sync": ts}))


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------

def fetch_permits(since: str | None) -> list[dict]:
    params = {
        "$limit":  PAGE_SIZE,
        "$order":  "issue_date ASC",
        "$select": "id,permit_type,issue_date,work_description,reported_cost,zip_code",
    }

    if since:
        params["$where"] = f"issue_date > '{since}'"
        log.info(f"Incremental pull — records after {since}")
    else:
        log.info("First run — pulling all records")

    all_records = []
    offset = 0

    while True:
        params["$offset"] = offset
        try:
            r = requests.get(API_ENDPOINT, params=params, timeout=15)
            r.raise_for_status()
        except requests.RequestException as e:
            log.error(f"API request failed: {e}")
            raise

        batch = r.json()
        if not batch:
            break

        all_records.extend(batch)
        log.info(f"  Fetched {len(all_records)} records so far...")

        if len(batch) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    return all_records


# ---------------------------------------------------------------------------
# Write (swap Supabase client here for the real job)
# ---------------------------------------------------------------------------

def write_records(records: list[dict]):
    existing = []
    if OUTPUT_FILE.exists():
        existing = json.loads(OUTPUT_FILE.read_text())

    existing.extend(records)
    OUTPUT_FILE.write_text(json.dumps(existing, indent=2))
    log.info(f"Wrote {len(records)} new records ({len(existing)} total)")


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

def run():
    log.info("=== Pipeline start ===")
    last_sync = load_last_sync()

    try:
        records = fetch_permits(since=last_sync)
    except Exception:
        log.error("Aborting — fetch failed")
        return

    if not records:
        log.info("No new records since last sync")
        return

    write_records(records)

    latest_date = max(r.get("issue_date", "") for r in records)
    save_last_sync(latest_date)
    log.info(f"Last sync updated to {latest_date}")
    log.info("=== Done ===")


if __name__ == "__main__":
    run()
