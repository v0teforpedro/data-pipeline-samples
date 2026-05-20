# Data Pipeline — Socrata API → Database

Pulls structured data from a Socrata open data API, stores it locally (or in a database), and syncs incrementally — each run only fetches records newer than the last.

## What it does

- Fetches paginated records from any Socrata API endpoint
- Tracks last sync date in a local state file
- On subsequent runs, only pulls new records (no full re-fetch)
- Logs all activity and errors to `pipeline.log`

## Stack

- Python 3.10+
- `requests` for API calls
- Drop-in ready for Supabase, PostgreSQL, or any database by swapping the `write_records` function

## Usage

```bash
pip install requests
python sample_pipeline.py
```

First run pulls all available records. Every run after that is incremental.

## Adapting to a new data source

1. Replace `API_ENDPOINT` with your Socrata dataset URL
2. Update `$select` with the fields you need
3. Replace `write_records()` with your database write logic
