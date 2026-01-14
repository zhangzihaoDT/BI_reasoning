import pandas as pd
import os
import sys

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from runtime.context import DataManager

def inspect_data():
    dm = DataManager()
    # Load a sample to check columns
    dm.load_data()
    df = dm.get_data()
    print(f"Columns: {list(df.columns)}")
    
    # Check if license_city exists
    if 'license_city' in df.columns:
        print("\n'license_city' exists.")
        print(f"Unique values (first 10): {df['license_city'].dropna().unique()[:10]}")
        
        # Check specific value '上海'
        sh_count = df[df['license_city'] == '上海'].shape[0]
        sh_city_count = df[df['license_city'] == '上海市'].shape[0]
        print(f"\nCount for '上海': {sh_count}")
        print(f"Count for '上海市': {sh_city_count}")
    else:
        print("\n'license_city' NOT found in columns.")
        # Check for similar columns
        print([c for c in df.columns if 'city' in c.lower()])

    # Check LS6 Pure Electric count in Dec 2025
    # Filter: 2025-12, series=LS6, product_type=纯电, metric=开票量 (invoice_upload_time)
    print("\nChecking LS6 Pure Electric 2025-12 Invoice Count...")
    
    # Simulate basic filtering logic
    df['invoice_upload_time'] = pd.to_datetime(df['invoice_upload_time'], errors='coerce')
    
    mask = (
        (df['series'] == 'LS6') & 
        (df['product_type'] == '纯电') & 
        (df['invoice_upload_time'].dt.strftime('%Y-%m') == '2025-12')
    )
    base_count = df[mask].shape[0]
    print(f"Base count (LS6 + 纯电 + 2025-12): {base_count}")
    
    if 'license_city' in df.columns:
        sh_mask = mask & (df['license_city'] == '上海')
        sh_city_mask = mask & (df['license_city'] == '上海市')
        print(f"Count with license_city='上海': {df[sh_mask].shape[0]}")
        print(f"Count with license_city='上海市': {df[sh_city_mask].shape[0]}")

if __name__ == "__main__":
    inspect_data()
