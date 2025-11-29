#!/usr/bin/env python3
"""
Profile staging tables (stg_*) and emit reports.

Outputs:
  - reports/staging_profile_summary.csv  (one row per table with basic stats)
  - reports/<table>_columns.csv         (column-level metrics)
  - reports/<table>_topvals.csv         (top 20 distinct values for suspicious columns)
"""

import os
import csv
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql+psycopg2://shopzada:12345@localhost:5432/shopzada')
REPORT_DIR = os.path.join("reports")
os.makedirs(REPORT_DIR, exist_ok=True)

engine = create_engine(DATABASE_URL, future=True)

def list_stg_tables():
    q = text("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type='BASE TABLE' AND table_name LIKE 'stg_%'
        ORDER BY table_name;
    """)
    with engine.connect() as conn:
        rows = conn.execute(q).fetchall()
    return [r[0] for r in rows]

def table_row_count(table):
    q = text(f"SELECT COUNT(*) FROM {table};")
    with engine.connect() as conn:
        return int(conn.execute(q).scalar_one())

def get_columns_info(table):
    q = text("""
      SELECT column_name, data_type
      FROM information_schema.columns
      WHERE table_schema='public' AND table_name = :t
      ORDER BY ordinal_position;
    """)
    with engine.connect() as conn:
        rows = conn.execute(q, {"t": table}).fetchall()
    return [(r[0], r[1]) for r in rows]

def null_and_distinct(table, column):
    q_null = text(f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL;")
    q_dist = text(f"SELECT COUNT(DISTINCT {column}) FROM {table};")
    with engine.connect() as conn:
        null_cnt = int(conn.execute(q_null).scalar_one())
        dist_cnt = int(conn.execute(q_dist).scalar_one())
    return null_cnt, dist_cnt

def top_values(table, column, limit=20):
    q = text(f"SELECT {column} AS val, COUNT(*) AS cnt FROM {table} GROUP BY {column} ORDER BY cnt DESC LIMIT :lim;")
    with engine.connect() as conn:
        try:
            rows = conn.execute(q, {"lim": limit}).fetchall()
            return [(r[0], int(r[1])) for r in rows]
        except Exception:
            return []

def detect_candidate_pks(table, cols, row_count):
    candidates = []
    for col, _ in cols:
        if col.lower() == "id" or col.lower().endswith("_id"):
            null_cnt, dist_cnt = null_and_distinct(table, col)
            if null_cnt == 0 and dist_cnt == row_count:
                candidates.append(col)
    return candidates

def fk_missing_count(child_table, child_col, parent_table, parent_col):
    q = text(f"""
      SELECT COUNT(*) FROM (
        SELECT DISTINCT c.{child_col} AS val
        FROM {child_table} c
        LEFT JOIN {parent_table} p ON c.{child_col} = p.{parent_col}
        WHERE c.{child_col} IS NOT NULL AND p.{parent_col} IS NULL
      ) x;
    """)
    with engine.connect() as conn:
        try:
            return int(conn.execute(q).scalar_one())
        except Exception:
            return None

def profile_table(table, tables_list):
    print(f"Profiling {table} ...", flush=True)
    row_count = table_row_count(table)
    cols = get_columns_info(table)
    col_records = []
    suspicious_topvals = []

    for col, dtype in cols:
        try:
            null_cnt, distinct_cnt = null_and_distinct(table, col)
            pct_null = (null_cnt / row_count * 100) if row_count > 0 else None
        except Exception as e:
            null_cnt, distinct_cnt, pct_null = None, None, None

        col_records.append({
            "table": table,
            "column": col,
            "data_type": dtype,
            "null_count": null_cnt,
            "null_pct": pct_null,
            "distinct_count": distinct_cnt
        })

        # capture top values for columns that look like IDs, low cardinality categorical columns, or high nulls
        if (col.lower().endswith("_id") or (distinct_cnt is not None and distinct_cnt < 100) or (pct_null and pct_null > 10)):
            top = top_values(table, col, limit=20)
            if top:
                suspicious_topvals.append((col, top))

    col_df = pd.DataFrame(col_records)
    col_df.to_csv(os.path.join(REPORT_DIR, f"{table}_columns.csv"), index=False)

    # write top values if any
    if suspicious_topvals:
        with open(os.path.join(REPORT_DIR, f"{table}_topvals.csv"), "w", newline='', encoding='utf-8') as fh:
            w = csv.writer(fh)
            w.writerow(["column", "value", "count"])
            for col, tops in suspicious_topvals:
                for val, cnt in tops:
                    w.writerow([col, val, cnt])

    # candidate PKs
    pk_candidates = detect_candidate_pks(table, cols, row_count)

    # FK checks for common keys
    fk_issues = []
    col_names = [c for c, _ in cols]
    if "user_id" in col_names and table != "stg_user_data":
        missing = fk_missing_count(table, "user_id", "stg_user_data", "user_id")
        fk_issues.append({"fk_col": "user_id", "parent": "stg_user_data", "missing": missing})
    if "product_id" in col_names and "stg_product_list" in tables_list:
        missing = fk_missing_count(table, "product_id", "stg_product_list", "product_id")
        fk_issues.append({"fk_col": "product_id", "parent": "stg_product_list", "missing": missing})
    if "merchant_id" in col_names and "stg_merchant_data" in tables_list:
        missing = fk_missing_count(table, "merchant_id", "stg_merchant_data", "merchant_id")
        fk_issues.append({"fk_col": "merchant_id", "parent": "stg_merchant_data", "missing": missing})

    return {
        "table": table,
        "row_count": row_count,
        "num_columns": len(cols),
        "pk_candidates": ";".join(pk_candidates),
        "fk_issues": fk_issues
    }

def main():
    tables = list_stg_tables()
    if not tables:
        print("No staging tables found.")
        return

    summary = []
    for t in tables:
        try:
            info = profile_table(t, tables)
            summary.append(info)
        except Exception as e:
            print(f"Error profiling {t}: {e}")

    rows = []
    for s in summary:
        fk_summary = "; ".join([f"{i['fk_col']}->{i['parent']} missing={i['missing']}" for i in s['fk_issues']]) if s['fk_issues'] else ""
        rows.append({
            "table": s["table"],
            "row_count": s["row_count"],
            "num_columns": s["num_columns"],
            "pk_candidates": s["pk_candidates"],
            "fk_issues": fk_summary
        })

    summary_df = pd.DataFrame(rows)
    out = os.path.join(REPORT_DIR, "staging_profile_summary.csv")
    summary_df.to_csv(out, index=False)
    print("\nWrote summary to", out)
    print(summary_df.to_string(index=False))

if __name__ == "__main__":
    main()
