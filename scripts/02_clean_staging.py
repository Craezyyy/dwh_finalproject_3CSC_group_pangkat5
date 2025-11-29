#!/usr/bin/env python3
# scripts/02_clean_staging.py
"""
Cleaning pipeline for ShopZada staging -> curated tables.

Outputs (examples):
  - dim_date
  - cur_dim_users
  - cur_dim_products
  - cur_dim_merchants
  - cur_fact_orders
  - cur_fact_line_items
  - cur_order_delays
  - cur_transactional_campaign_data
  - cur_user_credit_card_summary

Usage:
  python scripts/02_clean_staging.py

Notes:
  - Uses DATABASE_URL envvar (same DB as ingestion).
  - Uses utils.safe_write_sql when available; falls back to pandas.to_sql.
  - Optional geo_fixes.csv (in repo root) with columns: wrong, correct
"""

import os
import re
import json
import csv
import unicodedata
from datetime import datetime, date
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

# Try to import ftfy for robust mojibake fixes; provide fallback if missing
try:
    import ftfy
    def fix_text_mojibake(s):
        if pd.isna(s) or s is None:
            return None
        return ftfy.fix_text(str(s))
except Exception:
    # fallback heuristics
    def fix_text_mojibake(s):
        if pd.isna(s) or s is None:
            return None
        s = str(s)
        # Try common fallback: decode latin1 bytes then utf-8
        try:
            # if it looks like mojibake with weird sequences, attempt roundtrip
            b = s.encode('latin-1', errors='ignore')
            candidate = b.decode('utf-8', errors='ignore')
            if _looks_more_unicode(candidate, s):
                return candidate
        except Exception:
            pass
        # last resort: attempt NFC normalization
        try:
            return unicodedata.normalize("NFKC", s)
        except Exception:
            return s

def _looks_more_unicode(candidate, original):
    """
    Tiny heuristic: candidate contains more characters above ASCII (accented), return True.
    """
    return sum(1 for ch in candidate if ord(ch) > 127) >= sum(1 for ch in original if ord(ch) > 127)

# Attempt to import user helper safe_write_sql; otherwise fallback
USE_SAFE_WRITE = False
try:
    from utils import safe_write_sql
    USE_SAFE_WRITE = True
except Exception:
    safe_write_sql = None

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql+psycopg2://shopzada:12345@localhost:5432/shopzada')
RAW_DIR = os.getenv('RAW_DIR', './raw')
REPORT_DIR = os.path.join("reports")
os.makedirs(REPORT_DIR, exist_ok=True)

engine = create_engine(DATABASE_URL, future=True)

# ---------- utility I/O ----------

def to_sql(df: pd.DataFrame, table: str, if_exists='replace'):
    """Write DataFrame to SQL, preferring safe_write_sql if available."""
    if df is None:
        return
    if USE_SAFE_WRITE:
        try:
            safe_write_sql(df, table, engine)
            print(f"[OK] wrote {table} ({len(df)} rows) via safe_write_sql")
            return
        except Exception as e:
            print(f"[WARN] safe_write_sql failed for {table}: {e} -> falling back to pandas.to_sql")
    try:
        df.to_sql(table, engine, index=False, if_exists=if_exists, method='multi', chunksize=10000)
        print(f"[OK] wrote {table} ({len(df)} rows) via pandas.to_sql")
    except Exception as e:
        print(f"[ERROR] failed writing {table}: {e}")

# ---------- text & encoding fixes ----------

def normalize_unicode_text(val):
    """Fix mojibake and normalize unicode punctuation/combining forms, trim whitespace."""
    if pd.isna(val) or val is None:
        return None
    try:
        s = str(val)
        s = fix_text_mojibake(s)
        s = unicodedata.normalize("NFKC", s)
        s = s.strip()
        # collapse multiple spaces
        s = re.sub(r'\s+', ' ', s)
        return s if s != '' else None
    except Exception:
        try:
            return str(val).strip()
        except Exception:
            return None

def clean_text_col(s):
    if pd.isna(s) or s is None:
        return None
    return str(s).strip()

def clean_lower(s):
    s = normalize_unicode_text(s)
    return s.lower() if isinstance(s, str) else None

# ---------- date handling ----------

def parse_datetime_safe(val):
    """Return a pandas.Timestamp or None (keeps timezone unaware)."""
    if pd.isna(val) or val is None:
        return None
    try:
        ts = pd.to_datetime(val, errors='coerce', utc=False)
        if pd.isna(ts):
            return None
        return ts
    except Exception:
        return None

def parse_date_safe(val):
    ts = parse_datetime_safe(val)
    if ts is None:
        return None
    # return python date (not Timestamp) for easier storage in DB date columns
    try:
        return ts.date()
    except Exception:
        return None

def clean_birthdate(val):
    d = parse_date_safe(val)
    if d is None:
        return None
    today = date.today()
    # Basic plausibility rules
    age = today.year - d.year - ((today.month, today.day) < (d.month, d.day))
    if age < 5 or age > 110:
        return None
    if d.year < 1900:
        return None
    return d

# ---------- numeric helpers ----------

def safe_numeric(val):
    if pd.isna(val) or val is None:
        return None
    # remove common currency and thousands separators
    s = str(val).strip()
    s = s.replace(',', '')
    s = re.sub(r'[^\d\.\-eE]', '', s)
    if s in ['', '.', '-', '+']:
        return None
    try:
        return float(s)
    except Exception:
        try:
            return None
        except Exception:
            return None

def extract_int_from_string(s):
    if pd.isna(s) or s is None:
        return None
    m = re.search(r'(-?\d+)', str(s))
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return None
    return None

# ---------- credit card masking ----------

def mask_credit_card(cc):
    if pd.isna(cc) or cc is None:
        return None
    s = re.sub(r'\D', '', str(cc))
    if len(s) >= 4:
        return '**** **** **** ' + s[-4:]
    return None

# ---------- geo fixes mapping (optional) ----------

GEO_FIX_CSV = Path("geo_fixes.csv")
_geo_map = {}

def load_geo_map():
    global _geo_map
    _geo_map = {}
    if GEO_FIX_CSV.exists():
        try:
            with open(GEO_FIX_CSV, newline='', encoding='utf-8') as fh:
                reader = csv.DictReader(fh)
                for r in reader:
                    wrong = r.get('wrong') or r.get('source') or r.get('bad')
                    correct = r.get('correct') or r.get('target') or r.get('fix')
                    if wrong and correct:
                        _geo_map[wrong.strip()] = correct.strip()
            print(f"[INFO] loaded geo_fixes.csv with {_geo_map and len(_geo_map) or 0} entries")
        except Exception as e:
            print(f"[WARN] failed to load geo_fixes.csv: {e}")

def apply_geo_fixes(val):
    if pd.isna(val) or val is None:
        return None
    s = str(val).strip()
    # exact mapping
    if s in _geo_map:
        return _geo_map[s]
    # case-insensitive mapping
    for k, v in _geo_map.items():
        if k.lower() == s.lower():
            return v
    return s

# ---------- helpers for reading staging tables ----------

def safe_read_table(table):
    try:
        return pd.read_sql(text(f"SELECT * FROM {table}"), engine)
    except Exception as e:
        print(f"[WARN] could not read {table}: {e}")
        return pd.DataFrame()

# ---------- builders for curated tables ----------

def build_dim_date(min_year=2018, max_year=None):
    if max_year is None:
        max_year = datetime.now().year + 1
    start = date(min_year, 1, 1)
    end = date(max_year, 12, 31)
    rng = pd.date_range(start=start, end=end, freq='D')
    rows = []
    for d in rng:
        rows.append({
            'date_sk': int(d.strftime('%Y%m%d')),
            'date': d.date(),
            'year': d.year,
            'quarter': (d.month - 1) // 3 + 1,
            'month': d.month,
            'day': d.day,
            'day_of_week': d.weekday() + 1,  # Monday=1
            'is_weekend': 1 if d.weekday() >= 5 else 0
        })
    df = pd.DataFrame(rows)
    to_sql(df, 'dim_date')
    return df

def build_dim_users():
    df = safe_read_table('stg_user_data')
    if df.empty:
        print("[WARN] stg_user_data empty or missing")
        return pd.DataFrame()
    # basic cleanup
    df['user_id'] = df['user_id'].astype(str).str.strip()
    # text fields
    for col in ['name', 'street', 'city', 'state', 'country']:
        if col in df.columns:
            df[col] = df[col].apply(normalize_unicode_text)
            df[col] = df[col].apply(apply_geo_fixes)
    # types
    if 'creation_date' in df.columns:
        df['creation_date'] = df['creation_date'].apply(parse_datetime_safe)
    if 'birthdate' in df.columns:
        df['birthdate'] = df['birthdate'].apply(clean_birthdate)
    # device address, user_type, gender
    if 'device_address' in df.columns:
        df['device_address'] = df['device_address'].apply(lambda x: normalize_unicode_text(x).lower() if pd.notna(x) else None)
    if 'user_type' in df.columns:
        df['user_type'] = df['user_type'].apply(lambda x: normalize_unicode_text(x).lower() if pd.notna(x) else None)
    if 'gender' in df.columns:
        df['gender'] = df['gender'].apply(lambda x: normalize_unicode_text(x).lower() if pd.notna(x) else None)

    # dedupe: keep latest creation_date if present, else keep first
    if 'creation_date' in df.columns:
        df = df.sort_values('creation_date', ascending=False)
    df = df.drop_duplicates(subset=['user_id'], keep='first').reset_index(drop=True)

    cur = df[['user_id', 'name', 'creation_date', 'birthdate', 'gender', 'user_type',
              'street', 'city', 'state', 'country', 'device_address']].copy()
    to_sql(cur, 'cur_dim_users')
    return cur

def build_dim_products():
    # prefer master product_list
    master = safe_read_table('stg_product_list')
    if master.empty:
        print("[WARN] stg_product_list missing or empty")
        master = pd.DataFrame(columns=['product_id','product_name','category','price'])
    # normalize
    if 'product_id' in master.columns:
        master['product_id'] = master['product_id'].astype(str).str.strip()
    for c in master.columns:
        if master[c].dtype == object:
            master[c] = master[c].apply(normalize_unicode_text)
    if 'price' in master.columns:
        master['price'] = master['price'].apply(safe_numeric)
    to_sql(master, 'cur_dim_products')
    return master

def build_dim_merchants():
    df = safe_read_table('stg_merchant_data')
    if df.empty:
        print("[WARN] stg_merchant_data missing or empty")
        df = pd.DataFrame()
    # Normalize merchant id and text
    if 'merchant_id' in df.columns:
        df['merchant_id'] = df['merchant_id'].astype(str).str.strip()
    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].apply(normalize_unicode_text)
    to_sql(df, 'cur_dim_merchants')
    return df

def build_fact_orders():
    # union order tables
    order_tables = [t for t in [
        'stg_order_data_20200101_20200701',
        'stg_order_data_20200701_20211001',
        'stg_order_data_20211001_20220101',
        'stg_order_data_20220101_20221201',
        'stg_order_data_20221201_20230601',
        'stg_order_data_20230601_20240101'
    ] if True]
    parts = []
    for t in order_tables:
        df = safe_read_table(t)
        if not df.empty:
            parts.append(df)
    if not parts:
        print("[WARN] no order tables found")
        return pd.DataFrame()
    orders = pd.concat(parts, ignore_index=True, sort=False)

    # normalize ids and text
    if 'order_id' in orders.columns:
        orders['order_id'] = orders['order_id'].astype(str).str.strip()
    if 'user_id' in orders.columns:
        orders['user_id'] = orders['user_id'].astype(str).str.strip()

    # parse transaction_date to datetime and to date
    if 'transaction_date' in orders.columns:
        orders['transaction_ts'] = orders['transaction_date'].apply(parse_datetime_safe)
        orders['transaction_date_clean'] = orders['transaction_ts'].apply(lambda x: x.date() if x is not None else None)

    # estimated arrival numeric
    if 'estimated_arrival' in orders.columns:
        orders['estimated_arrival_days'] = orders['estimated_arrival'].apply(extract_int_from_string)

    # dedupe: keep latest by transaction_ts, else first
    if 'transaction_ts' in orders.columns:
        orders = orders.sort_values('transaction_ts', ascending=False)
    orders = orders.drop_duplicates(subset=['order_id'], keep='first').reset_index(drop=True)

    # selected columns for curated fact
    cols = []
    for c in ['order_id', 'user_id', 'transaction_date_clean', 'transaction_ts', 'estimated_arrival_days']:
        if c in orders.columns:
            cols.append(c)
    cur = orders[cols].copy()
    # rename to consistent column names
    cur = cur.rename(columns={'transaction_date_clean': 'transaction_date', 'transaction_ts': 'transaction_timestamp'})
    to_sql(cur, 'cur_fact_orders')
    return cur

def build_fact_line_items():
    parts = []
    for t in ['stg_line_item_data_prices1', 'stg_line_item_data_prices2', 'stg_line_item_data_prices3',
              'stg_line_item_data_products1', 'stg_line_item_data_products2', 'stg_line_item_data_products3']:
        df = safe_read_table(t)
        if not df.empty:
            parts.append(df)
    if not parts:
        print("[WARN] no line item files found")
        return pd.DataFrame()
    li = pd.concat(parts, ignore_index=True, sort=False)

    # normalize
    if 'order_id' in li.columns:
        li['order_id'] = li['order_id'].astype(str).str.strip()
    if 'product_id' in li.columns:
        li['product_id'] = li['product_id'].astype(str).str.strip()

    # numeric
    for c in ['price', 'item_price', 'amount', 'quantity']:
        if c in li.columns:
            li[c] = li[c].apply(safe_numeric)

    # dedupe by line-item id if exists
    if 'line_item_id' in li.columns:
        li = li.sort_values('order_id').drop_duplicates(subset=['line_item_id'], keep='first').reset_index(drop=True)

    to_sql(li, 'cur_fact_line_items')
    return li

def clean_order_delays():
    df = safe_read_table('stg_order_delays')
    if df.empty:
        print("[INFO] no order_delays to clean")
        return pd.DataFrame()
    # try extract numeric
    for c in df.columns:
        if 'delay' in c.lower() or 'days' in c.lower():
            df['delay_days'] = df[c].apply(extract_int_from_string)
            break
    to_sql(df, 'cur_order_delays')
    return df

def clean_transactional_campaigns():
    df = safe_read_table('stg_transactional_campaign_data')
    if df.empty:
        print("[INFO] no transactional campaigns")
        return pd.DataFrame()
    if 'order_id' in df.columns:
        df['order_id'] = df['order_id'].astype(str).str.strip()
    to_sql(df, 'cur_transactional_campaign_data')
    return df

def clean_user_credit_card():
    df = safe_read_table('stg_user_credit_card')
    if df.empty:
        print("[INFO] no user_credit_card table")
        return pd.DataFrame()
    # mask card numbers
    if 'card_number' in df.columns:
        df['card_masked'] = df['card_number'].apply(mask_credit_card)
    # keep minimal columns
    keep = [c for c in ['user_id', 'card_masked', 'card_type', 'expiry'] if c in df.columns]
    out = df[keep].copy()
    to_sql(out, 'cur_user_credit_card_summary')
    return out

# ---------- orchestration ----------

def main():
    print("=== START cleaning run ===")
    load_geo_map()
    dd = build_dim_date(min_year=2015, max_year=datetime.now().year + 1)
    print(f"[INFO] dim_date rows: {len(dd)}")
    users = build_dim_users()
    print(f"[INFO] cur_dim_users rows: {len(users)}")
    products = build_dim_products()
    merchants = build_dim_merchants()
    orders = build_fact_orders()
    line_items = build_fact_line_items()
    delays = clean_order_delays()
    campaigns = clean_transactional_campaigns()
    cc = clean_user_credit_card()
    print("=== CLEANING RUN COMPLETE ===")

if __name__ == "__main__":
    main()
