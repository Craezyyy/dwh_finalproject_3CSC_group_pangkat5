# scripts/utils.py
# Small helper functions used by ingestion/transforms
# helpers (normalize column names, parse helpers, safe SQL write)

import re
import pandas as pd

def normalize_cols(df):
    """
    Normalize column names: strip, lower, replace whitespace with underscore.
    """
    df = df.copy()
    df.columns = [re.sub(r'\s+','_',str(c).strip().lower()) for c in df.columns]
    return df

def parse_days(s):
    """
    Parse strings like '13days' -> 13, return None for NA.
    """
    if pd.isna(s):
        return None
    import re
    m = re.search(r'(\d+)', str(s))
    return int(m.group(1)) if m else None

def parse_qty(s):
    """
    Parse quantity strings like '4PCs' -> 4.
    """
    if pd.isna(s):
        return None
    import re
    m = re.search(r'(\d+)', str(s))
    return int(m.group(1)) if m else None

def try_parse_dates(df, colname):
    """
    Try to parse a date column in-place, returning dataframe.
    """
    if colname in df.columns:
        try:
            df[colname] = pd.to_datetime(df[colname], errors='coerce').dt.date
        except Exception:
            pass
    return df

def safe_write_sql(df, table_name, engine, if_exists='replace'):
    """
    Normalize, drop common 'Unnamed' index cols, and write to SQL using pandas.to_sql.
    """
    df = normalize_cols(df)
    # Drop unnamed index columns often produced by Excel/CSV
    df = df.loc[:, ~df.columns.str.contains('^unnamed')]
    df.to_sql(table_name, engine, if_exists=if_exists, index=False)
    print(f"[OK] {len(df)} rows -> {table_name}")
