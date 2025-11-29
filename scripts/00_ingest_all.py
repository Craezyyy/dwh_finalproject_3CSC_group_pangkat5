#!/usr/bin/env python3
# scripts/00_ingest_all.py



"""
Ingest raw ShopZada files under ./raw -> push to Postgres staging tables.
Handles: CSV (auto-detect tab/comma), JSON (ndjson or list), HTML (first table),
XLSX, PARQUET, PICKLE.

Output tables: stg_<filename_without_ext> (lowercased)
Example: raw/order_data_20211001-20220101.csv -> stg_order_data_20211001-20220101
"""

import os, json, pickle
import pandas as pd
from sqlalchemy import create_engine
from utils import safe_write_sql, normalize_cols

RAW_DIR = os.getenv('RAW_DIR', './raw')   # local folder where you put dataset files
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql+psycopg2://shopzada:shopzada_pass@localhost:5432/shopzada')

engine = create_engine(DATABASE_URL, future=True)

def ingest_csv(path, table_base):
    # detect whether header contains tabs
    with open(path, 'r', encoding='utf-8', errors='replace') as fh:
        first = fh.readline()
    sep = '\t' if '\t' in first else ','
    df = pd.read_csv(path, sep=sep, engine='python')
    safe_write_sql(df, f"stg_{table_base}", engine)

def ingest_json(path, table_base):
    try:
        # try ndjson first
        df = pd.read_json(path, lines=True)
    except ValueError:
        with open(path, 'r', encoding='utf-8', errors='replace') as fh:
            obj = json.load(fh)
        df = pd.json_normalize(obj)
    safe_write_sql(df, f"stg_{table_base}", engine)

def ingest_html(path, table_base):
    tables = pd.read_html(path)
    if len(tables) > 0:
        safe_write_sql(tables[0], f"stg_{table_base}", engine)
    else:
        print("[WARN] no table found in", path)

def ingest_xlsx(path, table_base):
    df = pd.read_excel(path)
    safe_write_sql(df, f"stg_{table_base}", engine)

def ingest_parquet(path, table_base):
    df = pd.read_parquet(path)
    safe_write_sql(df, f"stg_{table_base}", engine)

def ingest_pickle(path, table_base):
    with open(path, 'rb') as f:
        obj = pickle.load(f)
    if isinstance(obj, pd.DataFrame):
        df = obj
    elif isinstance(obj, dict):
        df = pd.json_normalize(obj)
    else:
        try:
            df = pd.DataFrame(obj)
        except Exception:
            df = pd.DataFrame([{'pickle_repr': str(type(obj))}])
    safe_write_sql(df, f"stg_{table_base}", engine)

def ingest_file(path):
    base = os.path.basename(path)
    table_base = os.path.splitext(base)[0].lower().replace(' ', '_').replace('-', '_')
    ext = os.path.splitext(base)[1].lower()
    print("Ingesting:", base)
    try:
        if ext == '.csv':
            ingest_csv(path, table_base)
        elif ext in ['.tsv']:
            ingest_csv(path, table_base)
        elif ext in ['.json']:
            ingest_json(path, table_base)
        elif ext in ['.html', '.htm']:
            ingest_html(path, table_base)
        elif ext in ['.xlsx', '.xls']:
            ingest_xlsx(path, table_base)
        elif ext == '.parquet':
            ingest_parquet(path, table_base)
        elif ext in ['.pickle', '.pkl']:
            ingest_pickle(path, table_base)
        else:
            print("[SKIP] unsupported ext:", ext)
    except Exception as e:
        print("[ERROR] ingest failed for", path, ":", e)

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
