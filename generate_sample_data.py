"""Run this first to generate sample sales data for testing."""
import pandas as pd
import numpy as np
import os

np.random.seed(42)
n = 5000

df = pd.DataFrame({
    'order_id':    [f'ORD-{i:05d}' for i in np.random.randint(1, 3000, n)],
    'order_date':  pd.date_range('2023-01-01', periods=n, freq='3H'),
    'customer_id': [f'CUST-{i:04d}' for i in np.random.randint(1, 500, n)],
    'product_id':  [f'PROD-{i:03d}' for i in np.random.randint(1, 50, n)],
    'category':    np.random.choice(['Electronics','Clothing','Food','Home','Sports'], n),
    'region':      np.random.choice(['North','South','East','West'], n),
    'unit_price':  np.round(np.random.uniform(5, 500, n), 2),
    'quantity':    np.random.randint(1, 10, n),
    'discount':    np.round(np.random.choice([0, 0.05, 0.1, 0.15, 0.2], n), 2),
})

# Inject some nulls and duplicates for realism
df.loc[np.random.choice(df.index, 50), 'quantity'] = np.nan
df = pd.concat([df, df.sample(30)], ignore_index=True)

os.makedirs('data', exist_ok=True)
df.to_csv('data/raw_sales.csv', index=False)
print(f"Sample data saved: data/raw_sales.csv ({len(df):,} rows)")
