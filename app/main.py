# app/main.py
import os
import logging
from typing import Optional

from fastapi import FastAPI, Query, HTTPException, Header, Depends
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
import pandas as pd

from app.data_loader import DataLoader

# Logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("logistics_api")

# API key header
API_KEY_HEADER = "x-api-key"
EXPECTED_API_KEY = os.getenv("API_KEY", "changeme")

app = FastAPI(title="Logistics Mock API (Local CSV backend)")

EXPOSED_TABLES = os.getenv(
    "EXPOSED_TABLES",
    "delivery_events,fuel_purchases,safety_incidents,maintenance_records"
).split(",")
DATA_DIR = os.getenv("DATA_DIR", "data")

loader = DataLoader(data_dir=DATA_DIR)


def verify_api_key(x_api_key: Optional[str] = Header(None, alias=API_KEY_HEADER)):
    if EXPECTED_API_KEY == "changeme":
        return True
    if not x_api_key or x_api_key != EXPECTED_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return True


@app.get("/health")
def health():
    return {"status": "ok", "data_dir": DATA_DIR}


@app.get("/api/tables")
def list_tables(_auth: bool = Depends(verify_api_key)):
    tables = loader.list_tables()
    return {"tables": tables, "exposed": EXPOSED_TABLES}


@app.get("/api/schema/{table_name}")
def table_schema(table_name: str, _auth: bool = Depends(verify_api_key)):
    tables = loader.list_tables()
    if table_name not in tables:
        raise HTTPException(status_code=404, detail="Table not found")
    schema = loader.get_schema(table_name)
    return {"table": table_name, "schema": schema}


@app.get("/api/{table_name}")
def query_table(
    table_name: str,
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    limit: int = Query(1000, ge=1, le=10000),
    offset: int = Query(0, ge=0),
    full: bool = Query(False, description="Return full dataset"),
    _auth: bool = Depends(verify_api_key)
):
    table_name = table_name.strip()
    if table_name not in EXPOSED_TABLES:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not exposed. Allowed: {EXPOSED_TABLES}")

    try:
        df = loader.load_table(table_name)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found in data directory '{DATA_DIR}'")
    except Exception as e:
        log.exception("Error loading table %s", table_name)
        raise HTTPException(status_code=500, detail=f"Error loading table: {str(e)}")

    # optional date filtering
    if start_date or end_date:
        try:
            df = loader.filter_by_date(df, start_date, end_date)
        except Exception:
            # if filtering fails, proceed with full df
            log.warning("Date filter failed for %s with start=%s end=%s", table_name, start_date, end_date)

    if full:
        df_page = df.copy()
    else:
        start = offset
        end = offset + limit
        df_page = df.iloc[start:end].copy()


    # Convert datetimes to ISO strings where present (defensive)
    for col in df_page.columns:
        try:
            if pd.api.types.is_datetime64_any_dtype(df_page[col]) or pd.api.types.is_datetime64tz_dtype(df_page[col]):
                df_page[col] = df_page[col].dt.strftime("%Y-%m-%dT%H:%M:%S")
                df_page[col] = df_page[col].where(df_page[col].notna(), None)
        except Exception:
            # ignore per-column conversion errors
            log.debug("datetime conversion skipped for column %s", col)

    # Replace pandas NA with None
    df_page = df_page.where(pd.notnull(df_page), None)

    # Convert to records
    records = df_page.to_dict(orient="records")

    # Use FastAPI's jsonable_encoder to ensure all types are JSON-serializable (handles numpy, datetime, etc.)
    out = {"count": len(records), "offset": offset, "limit": limit, "data": records}
    json_ready = jsonable_encoder(out)

    # Log for debugging
    log.info("Serving table=%s rows=%d offset=%d limit=%d", table_name, len(records), offset, limit)

    return JSONResponse(content=json_ready)


