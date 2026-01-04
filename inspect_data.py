import pandas as pd

data_path = "/Users/zihao_/Documents/coding/dataset/formatted/order_full_data.parquet"
try:
    df = pd.read_parquet(data_path)
    print("Columns:", df.columns.tolist())
    print("\nFirst 5 rows:")
    print(df.head())
    print("\nData Types:")
    print(df.dtypes)
except Exception as e:
    print(f"Error reading parquet file: {e}")
