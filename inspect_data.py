import pandas as pd

file_path = "/Users/zihao_/Documents/coding/dataset/formatted/order_full_data.parquet"

try:
    df = pd.read_parquet(file_path)
    print(f"Total rows: {len(df)}")
    
    if 'first_assign_time' in df.columns:
        missing_count = df['first_assign_time'].isna().sum()
        missing_pct = (missing_count / len(df)) * 100
        print(f"first_assign_time missing count: {missing_count}")
        print(f"first_assign_time missing percentage: {missing_pct:.2f}%")
        
        # Check specifically for yesterday's data (assuming yesterday is relative to dataset max or current time)
        # Let's check the distribution of first_assign_time around recent dates
        print("\nChecking recent lock_time records:")
        if 'lock_time' in df.columns:
            df['lock_time'] = pd.to_datetime(df['lock_time'])
            max_date = df['lock_time'].max()
            print(f"Max lock_time: {max_date}")
            
            recent_df = df[df['lock_time'].dt.date == max_date.date()]
            recent_missing = recent_df['first_assign_time'].isna().sum()
            print(f"Records on max date ({max_date.date()}): {len(recent_df)}")
            print(f"Missing first_assign_time on max date: {recent_missing}")
            
            # Show a few examples
            print("\nSample records on max date:")
            print(recent_df[['order_number', 'lock_time', 'first_assign_time']].head())
    else:
        print("Column 'first_assign_time' not found in dataset.")
        
except Exception as e:
    print(f"Error reading file: {e}")
