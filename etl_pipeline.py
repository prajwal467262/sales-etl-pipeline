import pandas as pd
import numpy as np
import sqlite3
import os
from datetime import datetime

print("=" * 50)
print("Sales ETL Pipeline")
print(f"Run time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 50)

# ── EXTRACT ───────────────────────────────────────────────
def extract(filepath):
    print(f"[EXTRACT] Reading {filepath}...")
    df = pd.read_csv(filepath, parse_dates=['order_date'])
    print(f"[EXTRACT] Loaded {len(df):,} rows, {df.shape[1]} columns")
    return df

# ── TRANSFORM ─────────────────────────────────────────────
def transform(df):
    print("[TRANSFORM] Cleaning data...")

    # Remove duplicates
    before = len(df)
    df.drop_duplicates(inplace=True)
    print(f"[TRANSFORM] Removed {before - len(df)} duplicates")

    # Handle missing values
    df['quantity'].fillna(1, inplace=True)
    df['discount'].fillna(0, inplace=True)
    df.dropna(subset=['order_id','product_id','customer_id'], inplace=True)

    # Feature engineering
    df['revenue']       = df['unit_price'] * df['quantity'] * (1 - df['discount'])
    df['month']         = df['order_date'].dt.month
    df['year']          = df['order_date'].dt.year
    df['quarter']       = df['order_date'].dt.quarter
    df['day_of_week']   = df['order_date'].dt.day_name()

    # KPIs
    kpis = {
        'total_revenue':    round(df['revenue'].sum(), 2),
        'total_orders':     df['order_id'].nunique(),
        'avg_order_value':  round(df.groupby('order_id')['revenue'].sum().mean(), 2),
        'total_customers':  df['customer_id'].nunique(),
        'top_product':      df.groupby('product_id')['revenue'].sum().idxmax()
    }
    print("[TRANSFORM] KPIs computed:")
    for k, v in kpis.items():
        print(f"  {k}: {v}")

    return df, kpis

# ── LOAD ──────────────────────────────────────────────────
def load(df, db_path='outputs/sales.db'):
    print(f"[LOAD] Writing to {db_path}...")
    os.makedirs('outputs', exist_ok=True)
    conn = sqlite3.connect(db_path)
    df.to_sql('sales_clean', conn, if_exists='replace', index=False)

    # Also export clean CSV for BI tools
    csv_path = 'outputs/sales_clean.csv'
    df.to_csv(csv_path, index=False)

    conn.close()
    print(f"[LOAD] Done. {len(df):,} rows written.")
    print(f"[LOAD] CSV exported: {csv_path}")
    print(f"[LOAD] SQLite DB:    {db_path}")

# ── RUN ───────────────────────────────────────────────────
if __name__ == '__main__':
    raw = extract('data/raw_sales.csv')
    clean, kpis = transform(raw)
    load(clean)
    print("\n[DONE] ETL pipeline complete.")
