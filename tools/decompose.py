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
        elif check_metric in ['开票量', '开票数'] and 'invoice_upload_time' in df.columns:
            df = df[df['invoice_upload_time'].notna()]
            
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
             num_metric = params["numerator"]
             den_metric = params["denominator"]
             filters = params.get("filters", [])
             
             # Helper to get value for a metric
             def get_metric_value(metric_name, filters):
                 # Check assign metrics
                 assign_metrics = [
                    "下发线索数", "下发线索当日试驾数", "下发线索 7 日试驾数", "下发线索 7 日锁单数",
                    "下发线索 30日试驾数", "下发线索 30 日锁单数", "下发门店数",
                    "下发线索当日锁单数 (门店)", "下发线索数 (门店)"
                 ]
                 if metric_name in assign_metrics:
                     adf = dm.get_assign_data()
                     adf = dm.filter_assign_data(date_range)
                     # Apply filters if possible (assign data usually doesn't have series/product)
                     # But we try anyway using dm.apply_filters
                     adf = dm.apply_filters(adf, filters)
                     return int(adf[metric_name].sum()) if not adf.empty and metric_name in adf.columns else 0
                 
                 # Order metrics
                 odf = dm.get_data()
                 time_col = "order_create_date"
                 if metric_name in ["sales", "锁单量", "锁单数"]:
                     time_col = "lock_time"
                 elif metric_name in ["交付数", "交付量"]:
                     time_col = "delivery_date"
                 elif metric_name in ["开票量", "开票数", "开票金额", "invoice_amount"]:
                     time_col = "invoice_upload_time"
                 
                 odf = dm.filter_data_on_df(odf, date_range, time_col)
                 odf = dm.apply_filters(odf, filters)
                 
                 if metric_name in ["sales", "锁单量", "锁单数"]:
                     return int(odf['lock_time'].notna().sum())
                 elif metric_name in ["交付数", "交付量"]:
                     return int(odf['delivery_date'].notna().sum())
                 else:
                     return len(odf) # Default count

             num_val = get_metric_value(num_metric, filters)
             den_val = get_metric_value(den_metric, filters)
             
             ratio = float(num_val / den_val) if den_val > 0 else 0.0
             
             return {
                "date_range": date_range,
                "ratio": ratio, 
                "numerator": num_metric,
                "denominator": den_metric,
                "numerator_value": num_val,
                "denominator_value": den_val,
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
        interval = params.get("interval")  # New: Support interval
        filters = params.get("filters") # New: Support filters

        dm = DataManager()
        
        # Determine time column based on metric
        time_col = 'order_create_date'
        if metric in ['sales', '锁单量']:
            time_col = 'lock_time'
        elif metric in ['开票量', '开票数']:
            time_col = 'invoice_upload_time'
            
        # Strategy: Apply filters first (for series/launch date context), then date filter
        df = dm.get_data()
        
        # Apply filters
        df = dm.apply_filters(df, filters)
        
        # Apply date range (using pre-filtered DF to support launch_plus logic)
        df = dm.filter_data_on_df(df, date_range, time_col=time_col)
        
        if metric in ['sales', '锁单量'] and 'lock_time' in df.columns:
            df = df[df['lock_time'].notna()]
        elif metric in ['开票量', '开票数'] and 'invoice_upload_time' in df.columns and 'lock_time' in df.columns:
            df = df[df['invoice_upload_time'].notna() & df['lock_time'].notna()]
            
        rows = []
        
        if not df.empty and dimension and dimension in df.columns:
            if interval:
                # Time-series composition
                rule = 'D'
                if interval == 'week': rule = 'W'
                elif interval == 'month': rule = 'ME'
                elif interval == 'year': rule = 'YE'
                
                # Group by Time and Dimension
                # We need to set index to time_col for Grouper to work
                df_indexed = df.set_index(time_col)
                grouped = df_indexed.groupby([pd.Grouper(freq=rule), dimension]).size()
                
                # Calculate totals per time bucket for percentage
                totals = df_indexed.groupby(pd.Grouper(freq=rule)).size()
                
                for (time_val, dim_val), count in grouped.items():
                    total_in_bucket = totals.get(time_val, 0)
                    rows.append({
                        "date": str(time_val.date()),
                        dimension: str(dim_val),
                        "value": int(count),
                        "percent": float(count / total_in_bucket) if total_in_bucket > 0 else 0.0
                    })
            else:
                # Static composition (existing logic)
                total = len(df)
                grouped = df.groupby(dimension).size().sort_values(ascending=False)
                rows = [
                    {
                        dimension: str(k), 
                        "value": int(v),
                        "percent": float(v / total) if total > 0 else 0.0
                    }
                    for k, v in grouped.items()
                ]
        elif not df.empty and not dimension:
             # Fallback if no dimension provided (just total)
             total = len(df)
             rows = [{"value": total, "percent": 1.0}]

        return {
            "metric": metric,
            "dimension": dimension,
            "date_range": date_range,
            "interval": interval,
            "rows": rows, # Standardized output key
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
        
        filters_left = params.get("filters_left") or params.get("filters")
        filters_right = params.get("filters_right")

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

            # --- Left Series Calculation ---
            # Apply left filters
            df_left_filtered = dm.apply_filters(base_df, filters_left)
            
            if left_metric in ['sales', '锁单量']:
                 # Ensure lock_time exists for sales
                 if 'lock_time' in df_left_filtered.columns:
                     df_left_filtered = df_left_filtered[df_left_filtered['lock_time'].notna()]
            
            # Resample left
            if not df_left_filtered.empty:
                left_series = df_left_filtered.set_index(time_col).resample(rule).size()
            else:
                left_series = pd.Series(dtype=int)

            # --- Right Series Calculation ---
            # Apply right filters
            df_right_filtered = dm.apply_filters(base_df, filters_right)
            
            if right_metric in ['sales', '锁单量']:
                if 'lock_time' in df_right_filtered.columns:
                    df_right_filtered = df_right_filtered[df_right_filtered['lock_time'].notna()]
            elif right_metric in ['开票量', '开票数']:
                if 'invoice_upload_time' in df_right_filtered.columns and 'lock_time' in df_right_filtered.columns:
                    df_right_filtered = df_right_filtered[
                        df_right_filtered['invoice_upload_time'].notna() & 
                        df_right_filtered['lock_time'].notna()
                    ]
            
            # Resample right
            if not df_right_filtered.empty:
                right_series = df_right_filtered.set_index(time_col).resample(rule).size()
            else:
                right_series = pd.Series(dtype=int)
            
            # --- Align and Merge ---
            # Union of indexes to ensure we cover both ranges
            all_dates = sorted(list(set(left_series.index) | set(right_series.index)))
            
            for d in all_dates:
                l_val = left_series.get(d, 0)
                r_val = right_series.get(d, 0)
                
                series.append({
                    "time": str(d.date()),
                    "left_value": int(l_val),
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
