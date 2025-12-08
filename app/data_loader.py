# app/data_loader.py
import os
import pandas as pd
from typing import Optional, Dict
from datetime import datetime
import logging

logger = logging.getLogger("data_loader")
logger.setLevel(logging.INFO)


class DataLoader:
    """
    Local CSV loader with lightweight cache and sensible post-processing for the logistics CSVs.
    """

    DATE_CANDIDATES = [
        "load_date",
        "dispatch_date",
        "purchase_date",
        "incident_date",
        "month",
        "scheduled_datetime",
        "actual_datetime",
        "event_date",
        "created_at",
        "updated_at",
    ]

    BOOL_TRUE = {"true", "True", "TRUE", "1", "yes", "Yes", "Y"}
    BOOL_FALSE = {"false", "False", "FALSE", "0", "no", "No", "N"}

    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        self.cache: Dict[str, pd.DataFrame] = {}

    def _local_path(self, table_name: str) -> str:
        return os.path.join(self.data_dir, f"{table_name}.csv")

    def list_tables(self):
        """List CSV files in data_dir (names without .csv)."""
        files = []
        if not os.path.isdir(self.data_dir):
            return files
        for f in os.listdir(self.data_dir):
            if f.lower().endswith(".csv"):
                files.append(os.path.splitext(f)[0])
        return sorted(files)

    def get_schema(self, table_name: str) -> Dict[str, str]:
        """Return simple schema (column -> dtype) using pandas inference (may be approximate)."""
        df = self.load_table(table_name)
        return {c: str(dtype) for c, dtype in df.dtypes.items()}

    def load_table(self, table_name: str) -> pd.DataFrame:
        """Load CSV and run lightweight normalization. Caches result in memory."""
        if table_name in self.cache:
            return self.cache[table_name]

        path = self._local_path(table_name)
        if not os.path.exists(path):
            raise FileNotFoundError(path)

        # read as string first to avoid dtype surprises
        df = pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""])
        # normalize column names: strip whitespace
        df.columns = [c.strip() for c in df.columns]

        # basic trimming for all string columns to clean spacing
        for c in df.select_dtypes(include="object").columns:
            df[c] = df[c].astype(str).str.strip()

        # normalize boolean-like columns (example on_time_flag)
        if "on_time_flag" in df.columns:
            df["on_time_flag"] = df["on_time_flag"].apply(self._to_bool_str)

        # attempt numeric conversion for common numeric fields
        for num_col in [
            "detention_minutes",
            "gallons",
            "price_per_gallon",
            "total_cost",
            "total_miles",
            "average_mpg",
            "labor_cost",
            "parts_cost",
            "downtime_hours",
        ]:
            if num_col in df.columns:
                df[num_col] = pd.to_numeric(df[num_col], errors="coerce")

        # attempt to parse dates for known date columns and store parsed version with suffix _ts
        for dcol in self.DATE_CANDIDATES:
            if dcol in df.columns:
                parsed = pd.to_datetime(df[dcol], errors="coerce", infer_datetime_format=True)
                parsed_col = f"{dcol}_ts"
                if parsed.notna().sum() > 0:
                    df[parsed_col] = parsed

        # add ingest timestamp column for auditing
        df["_ingest_ts"] = datetime.utcnow().isoformat()

        logger.info(f"Loaded table '{table_name}' shape={df.shape} from {path}")
        self.cache[table_name] = df
        return df

    def _to_bool_str(self, val: Optional[str]) -> str:
        if val is None:
            return ""
        s = str(val).strip()
        if s in self.BOOL_TRUE:
            return "true"
        if s in self.BOOL_FALSE:
            return "false"
        return s  # leave original if unknown

    def filter_by_date(self, df: pd.DataFrame, start_date: Optional[str], end_date: Optional[str]) -> pd.DataFrame:
        """
        Filter using the first available parsed timestamp column (dcol_ts) or raw candidate date column.
        Accepts ISO strings for start_date/end_date.
        """
        parsed_candidates = [f"{c}_ts" for c in self.DATE_CANDIDATES if f"{c}_ts" in df.columns]
        raw_candidates = [c for c in self.DATE_CANDIDATES if c in df.columns]

        candidate = parsed_candidates[0] if parsed_candidates else (raw_candidates[0] if raw_candidates else None)
        if not candidate:
            return df

        ser = pd.to_datetime(df[candidate], errors="coerce", infer_datetime_format=True)
        mask = pd.Series(True, index=df.index)
        if start_date:
            start_dt = pd.to_datetime(start_date, errors="coerce")
            if pd.notna(start_dt):
                mask &= ser >= start_dt
        if end_date:
            end_dt = pd.to_datetime(end_date, errors="coerce")
            if pd.notna(end_dt):
                mask &= ser <= end_dt
        filtered = df[mask.fillna(False)]
        logger.info(f"Filtered on '{candidate}' between {start_date} and {end_date}: {len(filtered)} rows")
        return filtered
