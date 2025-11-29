#!/usr/bin/env python3
# scripts/00_ingest_all.py

"""
Ingest raw ShopZada files under ./raw -> push to Postgres staging tables.
Handles: CSV (auto-detect tab/comma), JSON (ndjson or list or dict-of-dicts),
HTML (first table), XLSX, PARQUET, PICKLE.

Output tables: stg_<filename_without_ext> (lowercased)
Example: raw/order_data_20211001-20220101.csv -> stg_order_data_20211001_20220101
"""

import os
import json
import pickle
import pandas as pd
from sqlalchemy import create_engine
from utils import safe_write_sql, normalize_cols

RAW_DIR = os.getenv('RAW_DIR', './raw')   # local folder where you put dataset files
DATABASE_URL = os.getenv(
    'DATABASE_URL',
    'postgresql+psycopg2://shopzada:12345@localhost:5432/shopzada'
)
engine = create_engine(DATABASE_URL, future=True)

def safe_table_name_from_path(path):
    base = os.path.basename(path)
    name, _ = os.path.splitext(base)
    # reuse your existing style: lowercase, replace spaces and dashes with underscore
    return name.lower().replace(' ', '_').replace('-', '_')

# ---------- JSON / nested helpers ----------
def load_json_flex(path):
    """
    Load JSON that may be:
      - newline-delimited JSON records (lines=True)
      - a JSON array of objects (list)
      - a dict-of-dicts (pandas orient='columns' output)
    Returns a pandas DataFrame.
    """
    # Try newline-delimited JSON first (very common)
    try:
        df = pd.read_json(path, lines=True)
        return df
    except ValueError:
        pass
    except Exception:
        # fallback to next approach
        pass

    # Try to load whole JSON and inspect structure
    with open(path, 'r', encoding='utf-8', errors='replace') as fh:
        obj = json.load(fh)

    # dict-of-dicts (orient='columns'): outer keys -> columns, inner keys -> index
    if isinstance(obj, dict) and all(isinstance(v, dict) for v in obj.values()):
        df = pd.DataFrame(obj)
        # reset index to simple 0..n-1
        df = df.reset_index(drop=True)
        return df

    # list of objects / records
    if isinstance(obj, list):
        df = pd.DataFrame(obj)
        return df

    # fallback: try pandas read_json default
    try:
        df = pd.read_json(path)
        return df
    except Exception as e:
        raise RuntimeError(f"Unsupported JSON structure in {path}: {e}")

def sanitize_nested_columns(df):
    """
    Convert columns that contain dict/list into JSON strings so psycopg2/SQLAlchemy won't fail.
    Returns sanitized DataFrame.
    """
    # work on a copy to avoid surprising callers
    df = df.copy()
    for col in df.columns:
        # skip entirely-null columns
        non_null = df[col].dropna()
        if non_null.empty:
            continue
        # If any element is a dict or list, encode entire column to JSON strings (preserving nulls)
        if non_null.apply(lambda x: isinstance(x, (dict, list))).any():
            df[col] = df[col].apply(lambda x: json.dumps(x) if pd.notna(x) else None)
    return df

# ---------- ingest functions ----------
def ingest_csv(path, table_base):
    # detect whether header line contains a tab to choose separator
    with open(path, 'r', encoding='utf-8', errors='replace') as fh:
        first = fh.readline()
    sep = '\t' if '\t' in first else ','
    df = pd.read_csv(path, sep=sep, engine='python')
    df = normalize_cols(df)
    df = sanitize_nested_columns(df)
    safe_write_sql(df, f"stg_{table_base}", engine)

def ingest_json(path, table_base):
    df = load_json_flex(path)
    # sometimes json_normalize is useful for nested JSON where columns are nested dicts;
    # only apply if there is at least one nested dict in cells (to avoid flattening simple tables).
    # But to keep behavior predictable, we'll sanitize nested columns rather than aggressively flatten.
    df = normalize_cols(df)
    df = sanitize_nested_columns(df)
    safe_write_sql(df, f"stg_{table_base}", engine)

def ingest_html(path, table_base):
    tables = pd.read_html(path)
    if len(tables) > 0:
        df = tables[0]
        df = normalize_cols(df)
        df = sanitize_nested_columns(df)
        safe_write_sql(df, f"stg_{table_base}", engine)
    else:
        print("[WARN] no table found in", path)

def ingest_xlsx(path, table_base):
    df = pd.read_excel(path, sheet_name=0)
    df = normalize_cols(df)
    df = sanitize_nested_columns(df)
    safe_write_sql(df, f"stg_{table_base}", engine)

def ingest_parquet(path, table_base):
    df = pd.read_parquet(path)
    df = normalize_cols(df)
    df = sanitize_nested_columns(df)
    safe_write_sql(df, f"stg_{table_base}", engine)

def ingest_pickle(path, table_base):
    # Pickles can contain DataFrames, dict-of-dicts, or arbitrary objects.
    with open(path, 'rb') as f:
        obj = pickle.load(f)
    if isinstance(obj, pd.DataFrame):
        df = obj
    elif isinstance(obj, dict):
        # Could be dict-of-dicts (orient='columns') or dict-of-records.
        # Try to create DataFrame directly (covers many cases)
        try:
            df = pd.DataFrame(obj)
            # if columns look like numeric-string indices (orient variations), reset index
            df = df.reset_index(drop=True)
        except Exception:
            # fallback: attempt json_normalize then wrap single row
            try:
                df = pd.json_normalize(obj)
            except Exception:
                df = pd.DataFrame([obj])
    else:
        # as a last resort, attempt to coerce to DataFrame, otherwise store repr
        try:
            df = pd.DataFrame(obj)
        except Exception:
            df = pd.DataFrame([{'pickle_repr': str(type(obj))}])

    df = normalize_cols(df)
    df = sanitize_nested_columns(df)
    safe_write_sql(df, f"stg_{table_base}", engine)

def ingest_file(path):
    base = os.path.basename(path)
    # keep underscore/dash style stable with previous behavior
    table_base = os.path.splitext(base)[0].lower().replace(' ', '_').replace('-', '_')
    ext = os.path.splitext(base)[1].lower()
    print("Ingesting:", base)
    try:
        if ext == '.csv' or ext == '.tsv':
            ingest_csv(path, table_base)
        elif ext == '.json':
            ingest_json(path, table_base)
        elif ext in ('.html', '.htm'):
            ingest_html(path, table_base)
        elif ext in ('.xlsx', '.xls'):
            ingest_xlsx(path, table_base)
        elif ext == '.parquet':
            ingest_parquet(path, table_base)
        elif ext in ('.pickle', '.pkl'):
            ingest_pickle(path, table_base)
        else:
            print("[SKIP] unsupported ext:", ext)
    except Exception as e:
        # Match your previous log format
        print(f"[ERROR] ingest failed for {path} : {e}")

def main():
    files = sorted([os.path.join(RAW_DIR, f) for f in os.listdir(RAW_DIR) if os.path.isfile(os.path.join(RAW_DIR, f))])
    if not files:
        print("No files found in raw dir:", RAW_DIR)
        return
    for p in files:
        ingest_file(p)
    print("Ingest complete.")

if __name__ == '__main__':
    main()
