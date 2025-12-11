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
    start_date: Optional[str] = Query(None, description="ISO start date (YYYY-MM-DD or ISO datetime)"),
    end_date: Optional[str] = Query(None, description="ISO end date"),
    limit: int = Query(1000, ge=1, le=10000),
    offset: int = Query(0, ge=0),
    page: Optional[int] = Query(None, ge=1, description="1-based page number (optional)"),
    page_size: Optional[int] = Query(None, ge=1, le=10000, description="Optional page size when using page"),
    _auth: bool = Depends(verify_api_key)
):
    table_name = table_name.strip()

    # Normalize pagination: if page is provided, convert to offset/limit
    if page is not None:
        if page_size is not None:
            limit = page_size
        offset = (page - 1) * limit

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
            log.warning("Date filter failed for %s with start=%s end=%s", table_name, start_date, end_date)

    # ---- deterministic ordering for pagination ----
    sort_candidates = [
        "incident_date_ts",
        "purchase_date_ts",
        "maintenance_date_ts",
        "dispatch_date_ts",
        "load_date_ts",
        "month_ts",
        "event_date_ts",
        "created_at_ts",
        "updated_at_ts",
        "incident_date",
        "purchase_date",
        "maintenance_date",
        "dispatch_date",
        "load_date",
        "month",
        "event_date",
        "created_at",
        "updated_at",
        "event_id",
        "trip_id",
        "load_id",
        "maintenance_id",
    ]

    sort_col = None
    for c in sort_candidates:
        if c in df.columns:
            sort_col = c
            break

    if sort_col:
        try:
            df = df.sort_values(by=sort_col, kind="mergesort", na_position="last")
        except Exception:
            # fallback: ignore sorting error but keep deterministic-ish behaviour
            log.warning("Sorting failed on column %s for table %s", sort_col, table_name)
    # ----------------------------------------------------

    # pagination slicing
    start = int(offset)
    end = start + int(limit)
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
    total_rows = len(df)

    next_offset = None
    if end < total_rows:
        next_offset = end

    out = {
        "count": len(records),
        "offset": offset,
        "limit": limit,
        "next_offset": next_offset,
        "total": total_rows,
        "data": records,
    }

    json_ready = jsonable_encoder(out)

    # Log for debugging
    log.info("Serving table=%s rows=%d offset=%d limit=%d total=%d", table_name, len(records), offset, limit, total_rows)

    return JSONResponse(content=json_ready)



