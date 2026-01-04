import pandas as pd
from typing import Optional
import datetime

class DataManager:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DataManager, cls).__new__(cls)
            cls._instance.data = None
            cls._instance.data_path = "/Users/zihao_/Documents/coding/dataset/formatted/order_full_data.parquet"
        return cls._instance

    def load_data(self):
        if self.data is None:
            print(f"Loading data from {self.data_path}...")
            self.data = pd.read_parquet(self.data_path)
            # Ensure date columns are datetime
            if 'order_create_date' in self.data.columns:
                self.data['order_create_date'] = pd.to_datetime(self.data['order_create_date'])
            
            self._apply_business_logic()
            print(f"Data loaded. Shape: {self.data.shape}")
    
    def _apply_business_logic(self):
        # Apply series_group logic
        def get_series_group(row):
            pname = str(row.get('product_name', ''))
            if '新一代' in pname and 'LS6' in pname:
                return 'CM2'
            elif '全新' in pname and 'LS6' in pname:
                return 'CM1'
            elif 'LS6' in pname and '全新' not in pname and '新一代' not in pname:
                return 'CM0'
            elif '全新' in pname and 'L6' in pname:
                return 'DM1'
            elif 'L6' in pname and '全新' not in pname:
                return 'DM0'
            elif 'LS9' in pname:
                return 'LS9'
            elif 'LS7' in pname:
                return 'LS7'
            elif 'L7' in pname:
                return 'L7'
            else:
                return '其他'
        
        if 'product_name' in self.data.columns:
            self.data['series_group'] = self.data.apply(get_series_group, axis=1)

    
    def get_data(self) -> pd.DataFrame:
        if self.data is None:
            self.load_data()
        return self.data

    def filter_data(self, date_range: Optional[str] = None) -> pd.DataFrame:
        df = self.get_data()
        if not date_range:
            return df
            
        # Use current system time as today
        today = pd.Timestamp.now().normalize()
        max_data_date = df['order_create_date'].max()
        
        if date_range == "yesterday":
            # For "yesterday", we might want just the previous day, or up to yesterday.
            # Assuming strictly the day before today
            target_date = today - pd.Timedelta(days=1)
            
            # Validation: check if target date exists in dataset
            if target_date > max_data_date:
                print(f"Warning: Requesting data for {target_date.date()} but dataset max date is {max_data_date.date()}. Data might be incomplete or missing.")
            
            # Filter for that specific day
            return df[df['order_create_date'].dt.date == target_date.date()]
        elif date_range == "last_30_days":
            start_date = today - pd.Timedelta(days=30)
            return df[df['order_create_date'] >= start_date]
        elif date_range == "last_7_days":
            start_date = today - pd.Timedelta(days=7)
            return df[df['order_create_date'] >= start_date]
            
        return df
