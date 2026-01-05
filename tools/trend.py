from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List
import pandas as pd

from tools.base import BaseTool
from runtime.context import DataManager


@dataclass
class TrendPoint:
    date: str
    value: float


class TrendTool(BaseTool):
    name = "trend"

    def can_handle(self, step: dict) -> bool:
        return step.get("tool") == "trend"

    def execute(self, step: dict, state: dict):
        params = step.get("parameters", {})
        metric = params.get("metric")
        time_grain = params.get("time_grain", "day")
        compare_type = params.get("compare_type")
        date_range = params.get("date_range")

        dm = DataManager()
        
        # Determine time column based on metric
        time_col = 'order_create_date'
        if metric in ['sales', '锁单量']:
            time_col = 'lock_time'
        elif metric in ['开票量']:
            time_col = 'invoice_upload_time'
            
        df = dm.filter_data(date_range, time_col=time_col)

        # Apply metric definition
        if metric in ['sales', '锁单量'] and 'lock_time' in df.columns:
            df = df[df['lock_time'].notna()]
        elif metric in ['开票量'] and 'invoice_upload_time' in df.columns and 'lock_time' in df.columns:
            df = df[df['invoice_upload_time'].notna() & df['lock_time'].notna()]

        if step.get("id") == "anomaly_check":
            if df.empty:
                 return {
                    "metric": metric,
                    "value": 0.0,
                    "mean": 0.0,
                    "std": 0.0,
                    "signals": [],
                }
            
            # Group by day to get daily stats (use time_col)
            daily = df.groupby(df[time_col].dt.date).size()
            # Current value is the last point
            value = float(daily.iloc[-1]) if not daily.empty else 0.0
            # Mean and std of the period
            mean = float(daily.mean())
            std = float(daily.std()) if len(daily) > 1 else 0.0
            
            return {
                "metric": metric,
                "time_grain": time_grain,
                "compare_type": compare_type,
                "date_range": date_range,
                "value": value,
                "mean": mean,
                "std": std,
                "signals": [],
            }

        if df.empty:
             return {
                "metric": metric,
                "series": [],
                "signals": [],
            }

        # Resample
        # Make sure index is datetime (use time_col)
        df_sorted = df.sort_values(time_col)
        df_sorted = df_sorted.set_index(time_col)
        
        rule = 'D'
        if time_grain == 'week':
            rule = 'W'
        elif time_grain == 'month':
            rule = 'ME' # 'M' is deprecated for month end
            
        trend_series = df_sorted.resample(rule).size()
        
        series = [
            TrendPoint(date=str(d.date()), value=float(v))
            for d, v in trend_series.items()
        ]

        # Calculate simple change for display
        change = 0.0
        change_pct = 0.0
        
        # Special handling for single-point comparisons (e.g. "yesterday")
        if date_range == "yesterday":
            today = pd.Timestamp.now().normalize()
            target_date = today - pd.Timedelta(days=1)
            
            # Determine comparison date
            compare_date = None
            if compare_type == 'wow':
                compare_date = target_date - pd.Timedelta(days=7)
            else: # Default to mom (day-over-day for daily grain)
                compare_date = target_date - pd.Timedelta(days=1)
                
            # Fetch comparison data (using raw df from DataManager but manually filtering)
            full_df = dm.get_data()
            
            # Filter prev_df using time_col
            if time_col in full_df.columns:
                prev_df = full_df[full_df[time_col].dt.date == compare_date.date()]
            else:
                prev_df = pd.DataFrame()
            
            # Apply metric definition to prev_df
            if metric in ['sales', '锁单量'] and 'lock_time' in prev_df.columns:
                prev_df = prev_df[prev_df['lock_time'].notna()]
            elif metric in ['开票量'] and 'invoice_upload_time' in prev_df.columns and 'lock_time' in prev_df.columns:
                prev_df = prev_df[prev_df['invoice_upload_time'].notna() & prev_df['lock_time'].notna()]
                
            prev_val = len(prev_df)
            curr_val = len(df) # df is already filtered and metric-applied
            
            change = float(curr_val - prev_val)
            if prev_val != 0:
                change_pct = change / prev_val
                
        elif len(series) >= 2:
            prev = series[-2].value
            curr = series[-1].value
            change = curr - prev
            if prev != 0:
                change_pct = change / prev

        return {
            "metric": metric,
            "time_grain": time_grain,
            "compare_type": compare_type,
            "date_range": date_range,
            "series": series,
            "signals": [],
            "change": change,
            "change_pct": change_pct
        }
