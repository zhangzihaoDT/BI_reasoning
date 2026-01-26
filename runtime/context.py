import pandas as pd
from typing import Optional
import datetime
from pathlib import Path
import glob
import numpy as np
import re
import json

class DataManager:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DataManager, cls).__new__(cls)
            cls._instance.data = None
            cls._instance.data_path = "/Users/zihao_/Documents/coding/dataset/formatted/order_full_data.parquet"
            cls._instance.assign_data = None
            cls._instance.assign_path = None
            cls._instance.business_definition = None
        return cls._instance

    def load_business_definition(self):
        if self.business_definition is None:
            path = Path("/Users/zihao_/Documents/github/W52_reasoning/world/business_definition.json")
            if path.exists():
                with open(path, 'r', encoding='utf-8') as f:
                    self.business_definition = json.load(f)
            else:
                self.business_definition = {}
        return self.business_definition

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
            for col in ['order_create_date', 'lock_time', 'delivery_date']:
                if col in self.data.columns:
                    self.data[col] = pd.to_datetime(self.data[col], errors='coerce')

            if 'first_assign_time' in self.data.columns:
                # Optimized date parsing: try vectorized format first, then fallback
                # Assuming format is mostly "YYYY年M月D日" or standard
                self.data['first_assign_time'] = pd.to_datetime(
                    self.data['first_assign_time'], 
                    format='%Y年%m月%d日', 
                    errors='coerce'
                )
            
            self._apply_business_logic()
            print(f"Data loaded. Shape: {self.data.shape}")
    
    def _apply_business_logic(self):
        if 'product_name' in self.data.columns:
            # Vectorized series_group logic
            pname = self.data['product_name'].astype(str)
            
            conditions = [
                pname.str.contains('新一代') & pname.str.contains('LS6'),
                pname.str.contains('全新') & pname.str.contains('LS6'),
                pname.str.contains('LS6') & ~pname.str.contains('全新') & ~pname.str.contains('新一代'),
                pname.str.contains('全新') & pname.str.contains('L6'),
                pname.str.contains('L6') & ~pname.str.contains('全新'),
                pname.str.contains('LS9'),
                pname.str.contains('LS7'),
                pname.str.contains('L7')
            ]
            choices = ['CM2', 'CM1', 'CM0', 'DM1', 'DM0', 'LS9', 'LS7', 'L7']
            
            self.data['series_group'] = np.select(conditions, choices, default='其他')
            # Alias series_group to series for easier filtering
            self.data['series'] = self.data['series_group']
            
            # Vectorized product_type logic
            # "52" or "66" in product name -> "增程" (as per original logic), else "纯电"
            mask_erev = pname.str.contains('52|66', regex=True)
            self.data['product_type'] = np.where(mask_erev, '增程', '纯电')

    
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

    def apply_filters(self, df: pd.DataFrame, filters: list) -> pd.DataFrame:
        """
        Apply a list of filters to the DataFrame.
        Each filter is a dict with: field, op, value.
        """
        if df.empty or not filters:
            return df

        if self.business_definition is None:
            self.load_business_definition()
        mapping = self.business_definition.get("model_series_mapping", {})

        if isinstance(filters, dict):
            filters = [{"field": k, "op": "=", "value": v} for k, v in filters.items()]

        if not isinstance(filters, list):
            return df

        for f in filters:
            if not isinstance(f, dict):
                continue
            field = f.get("field") or f.get("dimension")
            op = (f.get("op") or f.get("operator") or "=").lower()
            value = f.get("value") or f.get("values")
            
            if not field or field not in df.columns:
                continue

            # Auto-expand mapped values (e.g. LS6 -> [CM0, CM1, CM2])
            if field in ['series_group', 'series'] and op in ['=', '==', 'in']:
                # Handle single value
                if isinstance(value, str) and value in mapping:
                    op = 'in'
                    value = mapping[value]
                # Handle list of values (if any matches mapping)
                elif isinstance(value, list):
                    new_values = []
                    expanded = False
                    for v in value:
                        if isinstance(v, str) and v in mapping:
                            new_values.extend(mapping[v])
                            expanded = True
                        else:
                            new_values.append(v)
                    if expanded:
                        value = new_values
                        op = 'in'

            if op in ["=", "=="]:
                if isinstance(value, list):
                    if len(value) == 1:
                        df = df[df[field] == value[0]]
                    else:
                        # Auto-switch to 'in' if multiple values provided with '='
                        df = df[df[field].isin(value)]
                else:
                    df = df[df[field] == value]
            elif op in ["!=", "<>"]:
                df = df[df[field] != value]
            elif op == "in":
                values = value if isinstance(value, list) else [value]
                df = df[df[field].isin(values)]
            elif op == "contains":
                df = df[df[field].astype(str).str.contains(str(value), na=False)]
            elif op in ["not_null", "notna", "exists", "is not null", "not null", "is_not_null"]:
                df = df[df[field].notna()]
            elif op in [">", ">=", "<", "<="]:
                s = pd.to_numeric(df[field], errors="coerce")
                v = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
                if pd.isna(v):
                    continue
                if op == ">":
                    df = df[s > v]
                elif op == ">=":
                    df = df[s >= v]
                elif op == "<":
                    df = df[s < v]
                elif op == "<=":
                    df = df[s <= v]
        return df

    def filter_data(self, date_range: Optional[str] = None, time_col: str = 'order_create_date') -> pd.DataFrame:
        df = self.get_data()
        return self.filter_data_on_df(df, date_range, time_col)

    def filter_data_on_df(self, df: pd.DataFrame, date_range: Optional[str] = None, time_col: str = 'order_create_date') -> pd.DataFrame:
        if not date_range:
            return df
            
        # Ensure the time column exists and we filter out NaT if we are using it as time axis
        if time_col not in df.columns:
            return pd.DataFrame()
            
        # Use current system time as today
        today = pd.Timestamp.now().normalize()
        # Use dataset max date based on the specific time column
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
            # Support "至今" suffix (Chinese for "to present")
            if '至今' in str(date_range):
                start_str = str(date_range).replace('至今', '').strip()
                try:
                    start = pd.to_datetime(start_str).normalize()
                    return df[df[time_col] >= start]
                except Exception:
                    pass

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

            # Support " to " separator
            if ' to ' in date_range:
                parts = date_range.split(' to ')
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

        # Handle "launch_plus_Nd" relative format
        if date_range.startswith("launch_plus_"):
            try:
                days_match = re.search(r'(\d+)d', date_range)
                days = int(days_match.group(1)) if days_match else 7
                
                # Determine series from dataframe context or assume it's pre-filtered externally
                # But here we need to know WHICH series to look up. 
                # Strategy: Check if data is filtered by series, or check filters passed? 
                # Since filter_data doesn't get filters, we infer from data distribution or need a way to know.
                # Simplified approach: Look at 'series' column if unique.
                
                target_series = None
                if 'series' in df.columns:
                    unique_series = df['series'].dropna().unique()
                    if len(unique_series) == 1:
                        target_series = unique_series[0]
                    elif 'series_group' in df.columns:
                        unique_groups = df['series_group'].dropna().unique()
                        if len(unique_groups) == 1:
                            target_series = unique_groups[0]
                
                if target_series:
                    biz_def = self.load_business_definition()
                    launch_info = biz_def.get("time_periods", {}).get(target_series)
                    # Correctly use 'end' as Launch Date based on schema.md
                    if launch_info and "end" in launch_info:
                        launch_date = pd.to_datetime(launch_info["end"])
                        end_date = launch_date + pd.Timedelta(days=days - 1) # inclusive
                        return df[(df[time_col] >= launch_date) & (df[time_col] <= end_date)]
                    else:
                        print(f"Warning: No launch info (end date) found for series {target_series}")
                else:
                    print("Warning: Ambiguous series for launch date calculation. Ensure data is filtered by a single series.")
            except Exception as e:
                print(f"Error parsing launch date: {e}")
                
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
            # Support "至今" suffix (Chinese for "to present")
            if '至今' in str(date_range):
                start_str = str(date_range).replace('至今', '').strip()
                start = pd.to_datetime(start_str).normalize()
                return df[df[time_col] >= start]

            # Check for date range format: YYYY-MM-DD/YYYY-MM-DD
            if '/' in str(date_range):
                parts = str(date_range).split('/')
                if len(parts) == 2:
                    start = pd.to_datetime(parts[0]).normalize()
                    end = pd.to_datetime(parts[1]).normalize()
                    return df[(df[time_col] >= start) & (df[time_col] < end + pd.Timedelta(days=1))]
            
            # Support " to " separator
            if ' to ' in str(date_range):
                parts = str(date_range).split(' to ')
                if len(parts) == 2:
                    start = pd.to_datetime(parts[0]).normalize()
                    end = pd.to_datetime(parts[1]).normalize()
                    return df[(df[time_col] >= start) & (df[time_col] < end + pd.Timedelta(days=1))]

            if re.match(r'^\d{4}-\d{2}$', str(date_range)):
                return df[df[time_col].dt.strftime('%Y-%m') == str(date_range)]
            if re.match(r'^\d{4}-\d{2}-\d{2}$', str(date_range)):
                return df[df[time_col].dt.strftime('%Y-%m-%d') == str(date_range)]
                
            # Handle Chinese Date format
            m_day = re.match(r"(\d{4})年(\d{1,2})月(\d{1,2})日", str(date_range))
            if m_day:
                y, mo, d = m_day.groups()
                target_date = f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"
                return df[df[time_col].dt.strftime('%Y-%m-%d') == target_date]

        except Exception as e:
            print(f"Date parsing failed: {e}")
            pass
            
        print(f"Warning: date_range '{date_range}' provided but could not be parsed. Returning empty result to avoid returning full history.")
        return pd.DataFrame()
    
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
                "assign_store_leads_ratio": 0.0,
                "assign_store_structure": 0.0,
            }
        leads = float(df['下发线索数'].sum())
        same_day = float(df['下发线索当日试驾数'].sum())
        d7_td = float(df['下发线索 7 日试驾数'].sum())
        d7_lock = float(df['下发线索 7 日锁单数'].sum())
        d30_td = float(df['下发线索 30日试驾数'].sum())
        d30_lock = float(df['下发线索 30 日锁单数'].sum())
        
        # New metrics for store leads
        store_leads = float(df['下发线索数 (门店)'].sum())
        store_lock_same_day = float(df['下发线索当日锁单数 (门店)'].sum())
        
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
            "assign_store_leads_ratio": _rate(store_leads, leads),
            "assign_store_structure": _rate(store_lock_same_day, store_leads),
        }
