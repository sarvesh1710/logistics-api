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

# -------------------------
# Auth
# -------------------------
def verify_api_key(x_api_key: Optional[str] = Header(None, alias=API_KEY_HEADER)):
    if EXPECTED_API_KEY == "changeme":
        return True
    if not x_api_key or x_api_key != EXPECTED_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return True

# -------------------------
# Health
# -------------------------
@app.get("/health")
def health():
    return {"status": "ok", "data_dir": DATA_DIR}

# -------------------------
# Metadata
# -------------------------
@app.get("/api/tables")
def list_tables(_auth: bool = Depends(verify_api_key)):
    tables = loader.list_tables()
    return {"tables": tables, "exposed": EXPOSED_TABLES}

@app.get("/api/schema/{table_name}")
def table_schema(table_name: str, _auth: bool = Depends(verify_api_key)):
    tables = loader.list_tables()
    if table_name not in tables:
        raise HTTPException(status_code=404, detail="Table not found")
    return {"table": table_name, "schema": loader.get_schema(table_name)}

# -------------------------
# DATA ENDPOINT (PAGINATED)
# -------------------------
@app.get("/api/{table_name}")
def query_table(
    table_name: str,
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    limit: int = Query(1000, ge=1, le=5000),   # safe page size
    offset: int = Query(0, ge=0),
    _auth: bool = Depends(verify_api_key)
):
    table_name = table_name.strip()

    if table_name not in EXPOSED_TABLES:
        raise HTTPException(
            status_code=404,
            detail=f"Table '{table_name}' not exposed. Allowed: {EXPOSED_TABLES}"
        )

    try:
        df = loader.load_table(table_name)
    except FileNotFoundError:
        raise HTTPException(
            status_code=404,
            detail=f"Table '{table_name}' not found in data directory '{DATA_DIR}'"
        )
    except Exception as e:
        log.exception("Error loading table %s", table_name)
        raise HTTPException(status_code=500, detail=str(e))

    # Optional date filtering
    if start_date or end_date:
        try:
            df = loader.filter_by_date(df, start_date, end_date)
        except Exception:
            log.warning("Date filter failed for %s", table_name)

    total_rows = len(df)

    # Pagination window
    start = offset
    end = offset + limit
    df_page = df.iloc[start:end].copy()

    # Convert datetime columns safely
    for col in df_page.columns:
        if pd.api.types.is_datetime64_any_dtype(df_page[col]):
            df_page[col] = df_page[col].dt.strftime("%Y-%m-%dT%H:%M:%S")

    df_page = df_page.where(pd.notnull(df_page), None)
    records = df_page.to_dict(orient="records")

    response = {
        "table": table_name,
        "offset": offset,
        "limit": limit,
        "returned": len(records),
        "total_available": total_rows,
        "has_more": (offset + limit) < total_rows,
        "data": records
    }

    log.info(
        "Serving table=%s offset=%d limit=%d returned=%d",
        table_name, offset, limit, len(records)
