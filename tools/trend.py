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
        
        assign_metric_map = {
            "assign_leads": "下发线索数",
            "assign_testdrive_d1": "下发线索当日试驾数",
            "assign_testdrive_d7": "下发线索 7 日试驾数",
            "assign_lock_d7": "下发线索 7 日锁单数",
            "assign_testdrive_d30": "下发线索 30日试驾数",
            "assign_lock_d30": "下发线索 30 日锁单数",
            "assign_store_count": "下发门店数",
        }
        assign_rate_map = {
            "assign_rate_same_day_test_drive": ("下发线索当日试驾数", "下发线索数"),
            "assign_rate_7d_test_drive": ("下发线索 7 日试驾数", "下发线索数"),
            "assign_rate_7d_lock": ("下发线索 7 日锁单数", "下发线索数"),
            "assign_rate_30d_test_drive": ("下发线索 30日试驾数", "下发线索数"),
            "assign_rate_30d_lock": ("下发线索 30 日锁单数", "下发线索数"),
            "assign_avg_daily_leads_per_store": ("下发线索数", "下发门店数"),
        }
        
        if metric in assign_rate_map:
            time_col = 'assign_date'
            df = dm.filter_assign_data(date_range)
            num_col, den_col = assign_rate_map[metric]
            if df.empty or time_col not in df.columns or num_col not in df.columns or den_col not in df.columns:
                return {
                    "metric": metric,
                    "time_grain": time_grain,
                    "compare_type": compare_type,
                    "date_range": date_range,
                    "series": [],
                    "signals": [],
                    "change": 0.0,
                    "change_pct": 0.0
                }
            df_sorted = df.sort_values(time_col).set_index(time_col)
            rule = 'D'
            if time_grain == 'week':
                rule = 'W'
            elif time_grain == 'month':
                rule = 'ME'
            num_series = df_sorted.resample(rule)[num_col].sum()
            den_series = df_sorted.resample(rule)[den_col].sum()
            rate_series = num_series / den_series.replace(0, pd.NA)
            rate_series = rate_series.fillna(0.0)
            series = [
                TrendPoint(date=str(d.date()), value=float(v))
                for d, v in rate_series.items()
            ]
            change = 0.0
            change_pct = 0.0
            if date_range == "yesterday":
                today = pd.Timestamp.now().normalize()
                target_date = today - pd.Timedelta(days=1)
                compare_date = target_date - (pd.Timedelta(days=7) if compare_type == 'wow' else pd.Timedelta(days=1))
                full = dm.get_assign_data()
                if time_col in full.columns and num_col in full.columns and den_col in full.columns:
                    curr_df = full[full[time_col].dt.date == target_date.date()]
                    prev_df = full[full[time_col].dt.date == compare_date.date()]
                    curr_num = float(curr_df[num_col].sum())
                    curr_den = float(curr_df[den_col].sum())
                    prev_num = float(prev_df[num_col].sum())
                    prev_den = float(prev_df[den_col].sum())
                    curr_val = float(curr_num / curr_den) if curr_den else 0.0
                    prev_val = float(prev_num / prev_den) if prev_den else 0.0
                    change = curr_val - prev_val
                    if prev_val != 0:
                        change_pct = change / prev_val
            elif len(series) >= 2:
                prev = series[-2].value
                curr = series[-1].value
                change = curr - prev
                if prev != 0:
                    change_pct = change / prev
            if step.get("id") == "anomaly_check":
                if len(series) == 0:
                    return {
                        "metric": metric,
                        "value": 0.0,
                        "mean": 0.0,
                        "std": 0.0,
                        "signals": [],
                    }
                values = pd.Series([p.value for p in series])
                value = float(values.iloc[-1])
                mean = float(values.mean())
                std = float(values.std()) if len(values) > 1 else 0.0
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
        
        if metric in assign_metric_map:
            time_col = 'assign_date'
            df = dm.filter_assign_data(date_range)
            target_col = assign_metric_map[metric]
            
            if step.get("id") == "anomaly_check":
                if df.empty or target_col not in df.columns:
                    return {
                        "metric": metric,
                        "value": 0.0,
                        "mean": 0.0,
                        "std": 0.0,
                        "signals": [],
                    }
                daily = df.groupby(df[time_col].dt.date)[target_col].sum()
                value = float(daily.iloc[-1]) if not daily.empty else 0.0
                mean = float(daily.mean()) if not daily.empty else 0.0
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
            
            if df.empty or target_col not in df.columns:
                 return {
                    "metric": metric,
                    "series": [],
                    "signals": [],
                }
            
            df_sorted = df.sort_values(time_col).set_index(time_col)
            rule = 'D'
            if time_grain == 'week':
                rule = 'W'
            elif time_grain == 'month':
                rule = 'ME'
            trend_series = df_sorted.resample(rule)[target_col].sum()
            series = [
                TrendPoint(date=str(d.date()), value=float(v))
                for d, v in trend_series.items()
            ]
            change = 0.0
            change_pct = 0.0
            if date_range == "yesterday":
                today = pd.Timestamp.now().normalize()
                target_date = today - pd.Timedelta(days=1)
                compare_date = target_date - (pd.Timedelta(days=7) if compare_type == 'wow' else pd.Timedelta(days=1))
                full_df = dm.get_assign_data()
                prev_df = full_df[full_df[time_col].dt.date == compare_date.date()] if time_col in full_df.columns else pd.DataFrame()
                prev_val = float(prev_df[target_col].sum()) if target_col in prev_df.columns else 0.0
                curr_val = float(df[target_col].sum())
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
        
        # Determine time column based on metric
        time_col = 'order_create_date'
        if metric in ['sales', '锁单量']:
            time_col = 'lock_time'
        elif metric in ['开票量', '开票数']:
            time_col = 'invoice_upload_time'
            
        df = dm.filter_data(date_range, time_col=time_col)

        # Apply metric definition
        if metric in ['sales', '锁单量'] and 'lock_time' in df.columns:
            df = df[df['lock_time'].notna()]
        elif metric in ['开票量', '开票数'] and 'invoice_upload_time' in df.columns and 'lock_time' in df.columns:
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
            elif metric in ['开票量', '开票数'] and 'invoice_upload_time' in prev_df.columns and 'lock_time' in prev_df.columns:
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
