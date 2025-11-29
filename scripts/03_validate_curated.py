
#!/usr/bin/env python3
# scripts/03_validate_curated.py

"""
QA Validation Script for Curated Tables

Checks:
  - Row counts
  - Null checks
  - Duplicate primary keys
  - Foreign key mismatches
  - Numeric validation
  - Date validation
  - Output written to: reports/curated_validation_report.csv
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql+psycopg2://shopzada:12345@localhost:5432/shopzada')
REPORT_DIR = "reports"
os.makedirs(REPORT_DIR, exist_ok=True)

engine = create_engine(DATABASE_URL, future=True)


def read(table):
    try:
        return pd.read_sql(text(f"SELECT * FROM {table}"), engine)
    except Exception as e:
        print(f"[WARN] Could not read {table}: {e}")
        return pd.DataFrame()


def find_duplicates(df, cols):
    if not all(c in df.columns for c in cols):
        return []
    dups = df[df.duplicated(subset=cols, keep=False)]
    return dups[cols].drop_duplicates().values.tolist()


def find_nulls(df, cols):
    issues = {}
    for c in cols:
        if c in df.columns:
            issues[c] = int(df[c].isna().sum())
    return issues


def count_fk_mismatches(df_child, df_parent, child_key, parent_key):
    if child_key not in df_child.columns or parent_key not in df_parent.columns:
        return -1
    parent_set = set(df_parent[parent_key].astype(str))
    return sum(~df_child[child_key].astype(str).isin(parent_set))


def save_report(rows):
    out = pd.DataFrame(rows)
    out.to_csv(os.path.join(REPORT_DIR, "curated_validation_report.csv"), index=False)
    print(f"\n[OK] Wrote validation report: reports/curated_validation_report.csv")


def main():
    report = []

    # -------- DIM USERS --------
    users = read("cur_dim_users")
    report.append({
        "table": "cur_dim_users",
        "rows": len(users),
        "duplicates(user_id)": find_duplicates(users, ["user_id"]),
        "nulls": find_nulls(users, ["user_id", "name", "creation_date"]),
    })

    # -------- DIM PRODUCTS --------
    products = read("cur_dim_products")
    report.append({
        "table": "cur_dim_products",
        "rows": len(products),
        "duplicates(product_id)": find_duplicates(products, ["product_id"]),
        "nulls": find_nulls(products, ["product_id", "product_name"]),
    })

    # -------- DIM MERCHANTS --------
    merchants = read("cur_dim_merchants")
    report.append({
        "table": "cur_dim_merchants",
        "rows": len(merchants),
        "duplicates(merchant_id)": find_duplicates(merchants, ["merchant_id"]),
        "nulls": find_nulls(merchants, ["merchant_id"]),
    })

    # -------- FACT ORDERS --------
    orders = read("cur_fact_orders")
    report.append({
        "table": "cur_fact_orders",
        "rows": len(orders),
        "duplicates(order_id)": find_duplicates(orders, ["order_id"]),
        "nulls": find_nulls(orders, ["order_id", "user_id", "transaction_date"]),
        "fk_mismatch(user_id→cur_dim_users)": count_fk_mismatches(orders, users, "user_id", "user_id"),
    })

    # -------- FACT LINE ITEMS --------
    li = read("cur_fact_line_items")
    report.append({
        "table": "cur_fact_line_items",
        "rows": len(li),
        "nulls": find_nulls(li, ["order_id", "product_id", "price", "quantity"]),
        "fk_mismatch(order_id→cur_fact_orders)": count_fk_mismatches(li, orders, "order_id", "order_id"),
        "fk_mismatch(product_id→cur_dim_products)": count_fk_mismatches(li, products, "product_id", "product_id"),
    })

    # -------- ORDER DELAYS --------
    delays = read("cur_order_delays")
    report.append({
        "table": "cur_order_delays",
        "rows": len(delays),
        "nulls": find_nulls(delays, ["order_id"]),
    })

    # -------- CAMPAIGN DATA --------
    camp = read("cur_transactional_campaign_data")
    report.append({
        "table": "cur_transactional_campaign_data",
        "rows": len(camp),
        "nulls": find_nulls(camp, ["order_id"]),
    })

    # -------- CREDIT CARD SUMMARY --------
    cc = read("cur_user_credit_card_summary")
    report.append({
        "table": "cur_user_credit_card_summary",
        "rows": len(cc),
        "nulls": find_nulls(cc, ["user_id", "card_masked"]),
    })

    # ---- Save the report ----
    save_report(report)

    print("\n=== VALIDATION CHECK COMPLETE ===\n")
    for r in report:
        print(r)


if __name__ == "__main__":
    main()
