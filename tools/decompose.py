from __future__ import annotations

from typing import Any, Dict, List
import pandas as pd

from tools.base import BaseTool
from runtime.context import DataManager


class AdditiveTool(BaseTool):
    name = "additive"

    def can_handle(self, step: dict) -> bool:
        return step.get("tool") == "additive"

    def execute(self, step: dict, state: dict):
        params = step.get("parameters", {})
        metric = params.get("metric") or params.get("total_metric")
        dimensions = params.get("dimensions") or params.get("components") or []
        date_range = params.get("date_range")

        dm = DataManager()
        
        # Determine time column based on metric
        check_metric = metric or params.get("total_metric")
        time_col = 'order_create_date'
        if check_metric in ['sales', '锁单量']:
            time_col = 'lock_time'
        elif check_metric in ['开票量', '开票数']:
            time_col = 'invoice_upload_time'
            
        df = dm.filter_data(date_range, time_col=time_col)
        
        # Apply metric definition (check metric or total_metric)
        if check_metric in ['sales', '锁单量'] and 'lock_time' in df.columns:
            df = df[df['lock_time'].notna()]
        elif check_metric in ['开票量', '开票数'] and 'invoice_upload_time' in df.columns and 'lock_time' in df.columns:
            df = df[df['invoice_upload_time'].notna() & df['lock_time'].notna()]
            
        total_val = len(df)

        contributions: List[Dict[str, Any]] = []
        
        # Pick the first valid dimension to decompose by
        target_dim = None
        for dim in dimensions:
            if dim in df.columns:
                target_dim = dim
                break
        
        if target_dim:
            grouped = df.groupby(target_dim).size().sort_values(ascending=False)
            contributions = [
                {
                    "dimension": str(k), 
                    "value": int(v),
                    "percent": float(v / total_val) if total_val > 0 else 0.0
                }
                for k, v in grouped.items()
            ]
        else:
             contributions = [
                {"dimension": "All", "value": total_val, "percent": 1.0},
            ]

        return {
            "metric": metric,
            "dimensions": dimensions,
            "date_range": date_range,
            "contributions": contributions,
            "signals": [],
        }


class RatioTool(BaseTool):
    name = "ratio"

    def can_handle(self, step: dict) -> bool:
        return step.get("tool") == "ratio"

    def execute(self, step: dict, state: dict):
        params = step.get("parameters", {})
        date_range = params.get("date_range")
        metrics = params.get("metrics") or []

        dm = DataManager()
        
        # Determine time column based on metric (though Ratio tool handles multiple metrics, usually primary drives date filter)
        # Defaulting to order_create_date as ratio usually implies a relationship over a common baseline (like orders created)
        # But if specifically asking for "sales" (lock) ratio, maybe lock_time is better?
        # Let's stick to order_create_date for Ratio unless strictly single metric 'sales'.
        # Actually, for RatioTool, we often calculate rates (lock rate, delivery rate) on the base of Created Orders.
        # So filtering by lock_time would be WRONG for "lock rate" (which is locks / created).
        # Thus, we KEEP order_create_date as the time axis for RatioTool.
        df = dm.filter_data(date_range)
        
        # Note: CompositionTool usually breaks down the same metric
        # Wait, this is RatioTool...
        
        # If metric is 'sales' (unlikely for RatioTool which calculates rates), but let's see.
        # RatioTool doesn't usually take a single 'metric' param that filters the whole dataset.
        # It takes 'metrics' list (e.g. ['lock_rate']).
        
        # So we do NOT apply sales filter here.
        
        total = len(df)
        
        ratios = []
        
        if total > 0:
            if "lock_rate" in metrics or (not metrics and "lock_time" in df.columns):
                lock_count = df['lock_time'].notna().sum()
                ratios.append({"name": "lock_rate", "value": float(lock_count / total)})
                
            if "delivery_rate" in metrics or (not metrics and "delivery_date" in df.columns):
                delivery_count = df['delivery_date'].notna().sum()
                ratios.append({"name": "delivery_rate", "value": float(delivery_count / total)})
        else:
            if "lock_rate" in metrics:
                ratios.append({"name": "lock_rate", "value": 0.0})
            if "delivery_rate" in metrics:
                ratios.append({"name": "delivery_rate", "value": 0.0})

        # Handle numerator/denominator if provided
        if "numerator" in params and "denominator" in params:
             # Placeholder for custom ratio
             return {
                "date_range": date_range,
                "ratio": 0.0, 
                "numerator": params["numerator"],
                "denominator": params["denominator"],
                "signals": [],
            }

        return {
            "date_range": date_range,
            "ratios": ratios,
            "signals": [],
        }


class CompositionTool(BaseTool):
    name = "composition"

    def can_handle(self, step: dict) -> bool:
        return step.get("tool") == "composition"

    def execute(self, step: dict, state: dict):
        params = step.get("parameters", {})
        metric = params.get("metric")
        dimension = params.get("dimension")
        date_range = params.get("date_range")

        dm = DataManager()
        
        # Determine time column based on metric
        time_col = 'order_create_date'
        if metric in ['sales', '锁单量']:
            time_col = 'lock_time'
        elif metric in ['开票量', '开票数']:
            time_col = 'invoice_upload_time'
            
        df = dm.filter_data(date_range, time_col=time_col)
        
        if metric in ['sales', '锁单量'] and 'lock_time' in df.columns:
            df = df[df['lock_time'].notna()]
        elif metric in ['开票量', '开票数'] and 'invoice_upload_time' in df.columns and 'lock_time' in df.columns:
            df = df[df['invoice_upload_time'].notna() & df['lock_time'].notna()]
            
        total = len(df)
        
        items = []
        if dimension and dimension in df.columns:
            grouped = df.groupby(dimension).size().sort_values(ascending=False)
            items = [
                {
                    "dimension": str(k), 
                    "value": int(v),
                    "percent": float(v / total) if total > 0 else 0.0
                }
                for k, v in grouped.items()
            ]

        return {
            "metric": metric,
            "dimension": dimension,
            "date_range": date_range,
            "items": items,
            "signals": [],
        }


class ParetoTool(BaseTool):
    name = "pareto"

    def can_handle(self, step: dict) -> bool:
        return step.get("tool") == "pareto"

    def execute(self, step: dict, state: dict):
        params = step.get("parameters", {})
        metric = params.get("metric")
        dimension = params.get("dimension")
        date_range = params.get("date_range")

        dm = DataManager()
        
        # Determine time column based on metric
        time_col = 'order_create_date'
        if metric in ['sales', '锁单量']:
            time_col = 'lock_time'
        elif metric in ['开票量']:
            time_col = 'invoice_upload_time'
            
        df = dm.filter_data(date_range, time_col=time_col)
        total = len(df)
        
        ranked = []
        if dimension and dimension in df.columns:
            grouped = df.groupby(dimension).size().sort_values(ascending=False)
            cumulative = 0
            for k, v in grouped.items():
                cumulative += v
                ranked.append({
                    "dimension": str(k), 
                    "value": int(v),
                    "cumulative_percent": float(cumulative / total) if total > 0 else 0.0
                })

        return {
            "metric": metric,
            "dimension": dimension,
            "date_range": date_range,
            "ranked": ranked,
            "signals": [],
        }


class DualAxisTool(BaseTool):
    name = "dual_axis"

    def can_handle(self, step: dict) -> bool:
        return step.get("tool") == "dual_axis"

    def execute(self, step: dict, state: dict):
        params = step.get("parameters", {})
        left_metric = params.get("left_metric")
        right_metric = params.get("right_metric")
        time_grain = params.get("time_grain", "week")
        date_range = params.get("date_range")

        dm = DataManager()
        
        # DualAxis is tricky because it involves TWO metrics potentially with different time axes.
        # However, usually we align on a primary time axis (order_create_date) or the time axis of the left metric.
        # If left_metric is sales, we might want to use lock_time as the axis.
        # But right_metric might be something else.
        # Simplification: Use order_create_date as the common timeline for now, 
        # unless both imply lock_time.
        # Or, we fetch full data and resample separately.
        
        # Let's fetch FULL data for the range (using broad filter if possible, or just all data and filter in memory)
        # But filter_data applies date_range on ONE column.
        # If we use order_create_date, we might miss orders created earlier but locked in range.
        
        # Better approach for DualAxis: Get all data for the broad period relative to TODAY, 
        # then resample each metric on its own appropriate time column.
        
        # But filter_data is designed for single date_range.
        # Let's try to infer a "safe" range. 
        # If date_range is relative (last_12_weeks), we can calculate start date.
        
        # Actually, let's stick to order_create_date for the *Timeline Axis* of the chart.
        # In BI, usually "Weekly Sales" chart uses the date the order was booked (locked) or created.
        # If users want to see "Sales by Lock Date", the x-axis is Lock Date.
        # If users want "Sales by Create Date", x-axis is Create Date.
        
        # Given "Sales" = "Locked Orders", the X-Axis should likely be Lock Time.
        # If Left Metric is Sales, let's assume X-Axis is Lock Time.
        
        time_col = 'order_create_date'
        if left_metric in ['sales', '锁单量']:
            time_col = 'lock_time'
            
        df = dm.filter_data(date_range, time_col=time_col)
        
        # Note: DualAxis might use different metrics for left/right
        # If left_metric is sales, apply filter for left series calculation
        
        series = []
        if not df.empty:
            base_df = df.copy()
            rule = 'W'
            if time_grain == 'day':
                rule = 'D'
            elif time_grain == 'month':
                rule = 'ME'

            resampled_all = base_df.set_index(time_col).resample(rule)
                        
            # Left metric logic
            if left_metric in ['sales', '锁单量']:
                 df_left = base_df[base_df['lock_time'].notna()].set_index(time_col)
                 left_series = df_left.resample(rule).size()
            else:
                 left_series = resampled_all.size()
            
            # Right metric = lock count (example) or something else
            # If right metric is different, we might need to re-fetch or re-index?
            # For now, assuming right metric can be calculated from the SAME dataset and SAME time axis.
            # If right metric is "traffic", and we index by "lock_time", it might be weird.
            # But usually Dual Axis compares correlated metrics on SAME time axis.
            
            if right_metric in ['sales', '锁单量']:
                df_right = base_df[base_df['lock_time'].notna()].set_index(time_col)
                right_series = df_right.resample(rule).size()
            elif right_metric in ['开票量', '开票数']:
                df_right = base_df[base_df['invoice_upload_time'].notna() & base_df['lock_time'].notna()].set_index(time_col)
                right_series = df_right.resample(rule).size()
            else:
                right_series = resampled_all.size()
            
            # Align indexes (use left_series index as base)
            for d in left_series.index:
                # Handle potential missing index in right_series if dates don't align perfectly (though resample usually aligns them)
                r_val = right_series.get(d, 0)
                
                series.append({
                    "time": str(d.date()),
                    "left_value": int(left_series[d]),
                    "right_value": int(r_val)
                })

        return {
            "left_metric": left_metric,
            "right_metric": right_metric,
            "time_grain": time_grain,
            "date_range": date_range,
            "series": series,
            "signals": [],
        }
