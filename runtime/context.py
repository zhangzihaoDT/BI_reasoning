import pandas as pd
from typing import Optional
import datetime
from pathlib import Path
import glob
import numpy as np
import re

class DataManager:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DataManager, cls).__new__(cls)
            cls._instance.data = None
            cls._instance.data_path = "/Users/zihao_/Documents/coding/dataset/formatted/order_full_data.parquet"
            cls._instance.assign_data = None
            cls._instance.assign_path = None
        return cls._instance

    @staticmethod
    def _parse_cn_date_static(val):
        if pd.isna(val):
            return pd.NaT
        s = str(val)
        # Try YYYY年M月D日
        m = re.match(r'(\d{4})年(\d{1,2})月(\d{1,2})日', s)
        if m:
            y, mo, d = m.groups()
            try:
                return pd.to_datetime(f"{int(y)}-{int(mo)}-{int(d)}", errors='coerce')
            except Exception:
                return pd.NaT
        # fallback: try direct to_datetime
        return pd.to_datetime(s, errors='coerce')

    def load_data(self):
        if self.data is None:
            print(f"Loading data from {self.data_path}...")
            self.data = pd.read_parquet(self.data_path)
            # Ensure date columns are datetime
            if 'order_create_date' in self.data.columns:
                self.data['order_create_date'] = pd.to_datetime(self.data['order_create_date'])
            if 'lock_time' in self.data.columns:
                self.data['lock_time'] = pd.to_datetime(self.data['lock_time'])
            if 'delivery_date' in self.data.columns:
                self.data['delivery_date'] = pd.to_datetime(self.data['delivery_date'])
            if 'first_assign_time' in self.data.columns:
                # Use custom parser for Chinese dates
                self.data['first_assign_time'] = self.data['first_assign_time'].apply(self._parse_cn_date_static)
            
            self._apply_business_logic()
            print(f"Data loaded. Shape: {self.data.shape}")
    
    def _apply_business_logic(self):
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

        def get_product_type(row):
            pname = str(row.get('product_name', ''))
            if '52' in pname or '66' in pname:
                return '增程'
            return '纯电'

        if 'product_name' in self.data.columns:
            self.data['series_group'] = self.data.apply(get_series_group, axis=1)
            self.data['product_type'] = self.data.apply(get_product_type, axis=1)

    
    def get_data(self) -> pd.DataFrame:
        if self.data is None:
            self.load_data()
        return self.data
    
    def load_assign_data(self):
        if self.assign_data is None:
            pattern = "/Users/zihao*/Documents/coding/dataset/original/assign_data.csv"
            matches = glob.glob(pattern)
            if matches:
                self.assign_path = matches[0]
            else:
                self.assign_path = None
            if self.assign_path:
                encodings = ['utf-8', 'utf-8-sig', 'utf-16', 'utf-16-le', 'utf-16-be', 'gbk', 'latin1']
                last_err = None
                for enc in encodings:
                    try:
                        df = pd.read_csv(self.assign_path, encoding=enc)
                        break
                    except Exception as e:
                        last_err = e
                        df = None
                if df is not None and df.shape[1] == 1:
                    for enc in encodings:
                        try:
                            df = pd.read_csv(self.assign_path, encoding=enc, sep='\t', engine='python')
                            break
                        except Exception:
                            pass
                if df is None:
                    raise last_err if last_err else RuntimeError("Failed to read assign_data.csv")
                
                def _normalize(s: str) -> str:
                    s = str(s).replace('\ufeff', '')
                    s = s.replace('\u00A0', ' ').replace('\u3000', ' ')
                    s = s.strip()
                    while '  ' in s:
                        s = s.replace('  ', ' ')
                    return s
                df.columns = [_normalize(c) for c in df.columns]
                
                required_cols = [
                    'Assign Time 年/月/日',
                    '下发线索数',
                    '下发线索当日试驾数',
                    '下发线索 7 日试驾数',
                    '下发线索 7 日锁单数',
                    '下发线索 30日试驾数',
                    '下发线索 30 日锁单数',
                    '下发门店数',
                    '下发线索当日锁单数 (门店)',
                    '下发线索数 (门店)'
                ]
                for c in required_cols:
                    if c not in df.columns:
                        raise ValueError(f"Missing required column: {c}")
                
                df['assign_date'] = df['Assign Time 年/月/日'].apply(self._parse_cn_date_static)
                for c in required_cols[1:]:
                    df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
                self.assign_data = df
            else:
                self.assign_data = pd.DataFrame()
    
    def get_assign_data(self) -> pd.DataFrame:
        if self.assign_data is None:
            self.load_assign_data()
        return self.assign_data

    def filter_data(self, date_range: Optional[str] = None, time_col: str = 'order_create_date') -> pd.DataFrame:
        df = self.get_data()
        if not date_range:
            return df
            
        # Ensure the time column exists and we filter out NaT if we are using it as time axis
        if time_col not in df.columns:
            return pd.DataFrame()
            
        # Use current system time as today
        today = pd.Timestamp.now().normalize()
        # Use dataset max date based on the specific time column
        # If time_col is lock_time, we should look at max lock_time? 
        # Or should we still anchor 'today' relative to system time?
        # The prompt implies system time.
        # But max_data_date validation should be against the time_col.
        max_data_date = df[time_col].max()
        
        if date_range == "yesterday":
            # For "yesterday", we might want just the previous day, or up to yesterday.
            # Assuming strictly the day before today
            target_date = today - pd.Timedelta(days=1)
            
            # Validation: check if target date exists in dataset
            if pd.isna(max_data_date) or target_date > max_data_date:
                print(f"Warning: Requesting data for {target_date.date()} but dataset max date for {time_col} is {max_data_date}. Data might be incomplete or missing.")
            
            # Filter for that specific day
            return df[df[time_col].dt.date == target_date.date()]
        elif date_range == "last_30_days":
            start_date = today - pd.Timedelta(days=30)
            return df[df[time_col] >= start_date]
        elif date_range == "last_7_days":
            start_date = today - pd.Timedelta(days=7)
            return df[df[time_col] >= start_date]
        
        # Try to parse specific date formats
        if date_range:
            # Check for date range format: YYYY-MM-DD/YYYY-MM-DD
            if '/' in date_range:
                parts = date_range.split('/')
                if len(parts) == 2:
                    try:
                        start = pd.to_datetime(parts[0]).normalize()
                        end = pd.to_datetime(parts[1]).normalize()
                        return df[(df[time_col] >= start) & (df[time_col] < end + pd.Timedelta(days=1))]
                    except Exception:
                        pass

            try:
                # YYYY (Year)
                if re.match(r'^\d{4}$', date_range):
                    year_val = int(date_range)
                    return df[df[time_col].dt.year == year_val]
                # YYYY-MM (Month)
                if re.match(r'^\d{4}-\d{2}$', date_range):
                    return df[df[time_col].dt.strftime('%Y-%m') == date_range]
                # YYYY-MM-DD (Day)
                if re.match(r'^\d{4}-\d{2}-\d{2}$', date_range):
                    return df[df[time_col].dt.strftime('%Y-%m-%d') == date_range]
            except Exception:
                pass
                
        return df
    
    def filter_assign_data(self, date_range: Optional[str] = None) -> pd.DataFrame:
        df = self.get_assign_data()
        if df.empty or not date_range:
            return df
        time_col = 'assign_date'
        if time_col not in df.columns:
            return pd.DataFrame()
        today = pd.Timestamp.now().normalize()
        max_data_date = df[time_col].max()
        if date_range == "yesterday":
            target_date = today - pd.Timedelta(days=1)
            if pd.isna(max_data_date) or target_date > max_data_date:
                pass
            return df[df[time_col].dt.date == target_date.date()]
        elif date_range == "last_30_days":
            start_date = today - pd.Timedelta(days=30)
            return df[df[time_col] >= start_date]
        elif date_range == "last_7_days":
            start_date = today - pd.Timedelta(days=7)
            return df[df[time_col] >= start_date]
        try:
            # Check for date range format: YYYY-MM-DD/YYYY-MM-DD
            if '/' in str(date_range):
                parts = str(date_range).split('/')
                if len(parts) == 2:
                    start = pd.to_datetime(parts[0]).normalize()
                    end = pd.to_datetime(parts[1]).normalize()
                    return df[(df[time_col] >= start) & (df[time_col] < end + pd.Timedelta(days=1))]

            if re.match(r'^\d{4}-\d{2}$', str(date_range)):
                return df[df[time_col].dt.strftime('%Y-%m') == str(date_range)]
            if re.match(r'^\d{4}-\d{2}-\d{2}$', str(date_range)):
                return df[df[time_col].dt.strftime('%Y-%m-%d') == str(date_range)]
        except Exception:
            pass
        return df
    
    def compute_assign_rates(self, date_range: Optional[str] = None) -> dict:
        df = self.filter_assign_data(date_range) if date_range else self.get_assign_data()
        if df.empty:
            return {
                "rate_same_day_test_drive": 0.0,
                "rate_7d_test_drive": 0.0,
                "rate_7d_lock": 0.0,
                "rate_30d_test_drive": 0.0,
                "rate_30d_lock": 0.0,
                "avg_daily_leads_per_store": 0.0,
            }
        leads = float(df['下发线索数'].sum())
        same_day = float(df['下发线索当日试驾数'].sum())
        d7_td = float(df['下发线索 7 日试驾数'].sum())
        d7_lock = float(df['下发线索 7 日锁单数'].sum())
        d30_td = float(df['下发线索 30日试驾数'].sum())
        d30_lock = float(df['下发线索 30 日锁单数'].sum())
        def _rate(n: float, d: float) -> float:
            return float(n / d) if d and d > 0 else 0.0
        with np.errstate(divide='ignore', invalid='ignore'):
            s = df['下发线索数'] / df['下发门店数']
            s = s.replace([np.inf, -np.inf], np.nan)
            avg_daily_leads_per_store = float(s.mean()) if len(s) > 0 else 0.0
        return {
            "rate_same_day_test_drive": _rate(same_day, leads),
            "rate_7d_test_drive": _rate(d7_td, leads),
            "rate_7d_lock": _rate(d7_lock, leads),
            "rate_30d_test_drive": _rate(d30_td, leads),
            "rate_30d_lock": _rate(d30_lock, leads),
            "avg_daily_leads_per_store": avg_daily_leads_per_store,
        }
