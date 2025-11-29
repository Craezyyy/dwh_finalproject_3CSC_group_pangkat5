#!/usr/bin/env python3
# scripts/00_ingest_all.py

"""
Robust ingestion for ShopZada raw files -> Postgres staging tables.

Handles:
 - CSV/TSV (auto-detect tab/comma)
 - JSON (ndjson, list of objects, or dict-of-dicts where outer keys are columns)
 - HTML (first table)
 - XLSX
 - PARQUET
 - PICKLE (DataFrame, dict-of-dicts, dict-of-records, etc)

Outputs tables named stg_<filename_without_ext>.

Before running: ensure DATABASE_URL env var is set, or edit the default below.
"""

import os
import json
import pickle
import pandas as pd
from sqlalchemy import create_engine
from utils import safe_write_sql, normalize_cols

RAW_DIR = os.getenv('RAW_DIR', './raw')
DATABASE_URL = os.getenv(
    'DATABASE_URL',
    'postgresql+psycopg2://shopzada:12345@localhost:5432/shopzada'
)
engine = create_engine(DATABASE_URL, future=True)


# ------------------ Helpers ------------------

def safe_table_base(path):
    base = os.path.basename(path)
    return os.path.splitext(base)[0].lower().replace(' ', '_').replace('-', '_')


def sanitize_nested_columns(df):
    """
    Convert any dict/list cell into JSON strings so psycopg2 won't raise can't adapt type 'dict'.
    Return sanitized copy.
    """
    df = df.copy()
    for col in df.columns:
        non_null = df[col].dropna()
        if non_null.empty:
            continue
        # column-wise detection of nested types
        if non_null.apply(lambda x: isinstance(x, (dict, list))).any():
            df[col] = df[col].apply(lambda x: json.dumps(x) if pd.notna(x) else None)
    return df


def load_json_flex(path):
    """
    Deterministic JSON loader:
      - Reads raw JSON with json.load()
      - If dict-of-dicts (outer keys = columns, inner keys = row ids) -> build row-per-inner-key DataFrame
      - If list -> DataFrame(list)
      - Else fallback -> try pandas.read_json as last resort
    This avoids pandas mis-parsing ndjson as single dict-cells.
    """
    with open(path, 'r', encoding='utf-8', errors='replace') as fh:
        obj = json.load(fh)

    # dict-of-dicts -> rebuild rows
    if isinstance(obj, dict) and all(isinstance(v, dict) for v in obj.values()):
        inner_keys = set()
        for inner in obj.values():
            inner_keys.update(inner.keys())
        try:
            sorted_keys = sorted(inner_keys, key=lambda x: int(x) if str(x).isdigit() else x)
        except Exception:
            sorted_keys = sorted(inner_keys, key=lambda x: str(x))

        rows = []
        for k in sorted_keys:
            row = {}
            for col_name, inner in obj.items():
                if isinstance(inner, dict):
                    row[col_name] = inner.get(k)
                else:
                    row[col_name] = None
            rows.append(row)
        return pd.DataFrame(rows)

    # list of records -> direct
    if isinstance(obj, list):
        return pd.DataFrame(obj)

    # otherwise let pandas try (covers other oriented JSON)
    try:
        return pd.read_json(path)
    except Exception:
        # last resort: single-row with raw object
        return pd.DataFrame([obj])


# ------------------ Ingest functions ------------------

def ingest_csv(path, table_base):
    with open(path, 'r', encoding='utf-8', errors='replace') as fh:
        first = fh.readline()
    sep = '\t' if '\t' in first else ','
    df = pd.read_csv(path, sep=sep, engine='python')
    try:
        df = normalize_cols(df)
    except Exception:
        pass
    df = sanitize_nested_columns(df)
    safe_write_sql(df, f"stg_{table_base}", engine)


def ingest_json(path, table_base):
    df = load_json_flex(path)

    # Helpful debug: if df has only 1 row, print diagnostics (so you can see cause)
    if isinstance(df, pd.DataFrame) and df.shape[0] == 1:
        # quick heuristic to detect dict-cells
        nested_flag = False
        for col in df.columns:
            non_null = df[col].dropna()
            if not non_null.empty and non_null.apply(lambda x: isinstance(x, (dict, list))).any():
                nested_flag = True
                break
        if nested_flag:
            print(f"[DEBUG] {os.path.basename(path)} produced 1-row DataFrame with dict/list cells — converting if possible.")
            try:
                with open(path, 'r', encoding='utf-8', errors='replace') as fh:
                    raw = json.load(fh)
                if isinstance(raw, dict) and all(isinstance(v, dict) for v in raw.values()):
                    inner_keys = set()
                    for v in raw.values():
                        inner_keys.update(v.keys())
                    print(f"[DEBUG] {os.path.basename(path)} expected rows (union inner keys): {len(inner_keys)}")
            except Exception:
                pass

    try:
        df = normalize_cols(df)
    except Exception:
        pass
    df = sanitize_nested_columns(df)
    safe_write_sql(df, f"stg_{table_base}", engine)


def ingest_html(path, table_base):
    tables = pd.read_html(path)
    if not tables:
        print("[WARN] no table found in", path)
        return
    df = tables[0]
    try:
        df = normalize_cols(df)
    except Exception:
        pass
    df = sanitize_nested_columns(df)
    safe_write_sql(df, f"stg_{table_base}", engine)


def ingest_xlsx(path, table_base):
    df = pd.read_excel(path, sheet_name=0)
    try:
        df = normalize_cols(df)
    except Exception:
        pass
    df = sanitize_nested_columns(df)
    safe_write_sql(df, f"stg_{table_base}", engine)


def ingest_parquet(path, table_base):
    df = pd.read_parquet(path)
    try:
        df = normalize_cols(df)
    except Exception:
        pass
    df = sanitize_nested_columns(df)
    safe_write_sql(df, f"stg_{table_base}", engine)


def ingest_pickle(path, table_base):
    with open(path, 'rb') as f:
        obj = pickle.load(f)
    # If it's already a DataFrame, use it
    if isinstance(obj, pd.DataFrame):
        df = obj
    elif isinstance(obj, dict):
        # Try to treat dict-of-dicts as columns->inner-index first (same logic as JSON)
        if all(isinstance(v, dict) for v in obj.values()):
            inner_keys = set()
            for inner in obj.values():
                inner_keys.update(inner.keys())
            try:
                sorted_keys = sorted(inner_keys, key=lambda x: int(x) if str(x).isdigit() else x)
            except Exception:
                sorted_keys = sorted(inner_keys, key=lambda x: str(x))
            rows = []
            for k in sorted_keys:
                row = {}
                for col_name, inner in obj.items():
                    row[col_name] = inner.get(k) if isinstance(inner, dict) else None
                rows.append(row)
            df = pd.DataFrame(rows)
        else:
            # fallback: dict-of-records or simple mapping
            try:
                df = pd.DataFrame(obj)
                df = df.reset_index(drop=True)
            except Exception:
                df = pd.DataFrame([obj])
    else:
        try:
            df = pd.DataFrame(obj)
        except Exception:
            df = pd.DataFrame([{'pickle_repr': str(type(obj))}])

    try:
        df = normalize_cols(df)
    except Exception:
        pass
    df = sanitize_nested_columns(df)
    safe_write_sql(df, f"stg_{table_base}", engine)


# ------------------ Main loop ------------------

def ingest_file(path):
    base = os.path.basename(path)
    table_base = safe_table_base(path)
    ext = os.path.splitext(base)[1].lower()
    print("Ingesting:", base)
    try:
        if ext in ('.csv', '.tsv'):
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
        print(f"[ERROR] ingest failed for {path} : {e}")


def main():
    files = sorted([os.path.join(RAW_DIR, f) for f in os.listdir(RAW_DIR)
                    if os.path.isfile(os.path.join(RAW_DIR, f))])
    if not files:
        print("No files found in raw dir:", RAW_DIR)
        return

    # quick pre-check for the two JSONs that caused trouble previously
    trouble_files = {
        'order_data_20221201-20230601.json',
        'user_data.json'
    }
    for p in files:
        if os.path.basename(p) in trouble_files:
            try:
                # print an estimate of expected rows (helps you validate quickly)
                with open(p, 'r', encoding='utf-8', errors='replace') as fh:
                    raw = json.load(fh)
                if isinstance(raw, dict) and all(isinstance(v, dict) for v in raw.values()):
                    inner_keys = set()
                    for v in raw.values():
                        inner_keys.update(v.keys())
                    print(f"[ESTIMATE] {os.path.basename(p)} expected rows ~ {len(inner_keys)}")
            except Exception:
                pass

    for p in files:
        ingest_file(p)
    print("Ingest complete.")


if __name__ == '__main__':
    main()
