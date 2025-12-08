# Logistics Mock API (Local CSV backend)

Simple FastAPI app that serves local CSV files as JSON endpoints. No S3 / MongoDB integrations included.

## Repo layout
- app/
  - main.py
  - data_loader.py
  - schemas.py
- data/              (place your CSVs here: <table_name>.csv)
- requirements.txt
- Dockerfile

## Default exposed tables
Set via env var `EXPOSED_TABLES` (comma-separated). Default:
`delivery_events,fuel_purchases,safety_incidents,maintenance_record`

## Local run (dev)`
1. Create virtualenv & install:
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
