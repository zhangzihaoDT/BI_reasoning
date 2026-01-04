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
        df = dm.filter_data(date_range)
        
        # Apply metric definition (check metric or total_metric)
        check_metric = metric or params.get("total_metric")
        if check_metric == 'sales' and 'lock_time' in df.columns:
            df = df[df['lock_time'].notna()]
            
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
        df = dm.filter_data(date_range)
        
        # Note: CompositionTool usually breaks down the same metric
        if metric == 'sales' and 'lock_time' in df.columns:
            df = df[df['lock_time'].notna()]
            
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
        df = dm.filter_data(date_range)
        
        if metric == 'sales' and 'lock_time' in df.columns:
            df = df[df['lock_time'].notna()]
            
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
        df = dm.filter_data(date_range)
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
        df = dm.filter_data(date_range)
        
        # Note: DualAxis might use different metrics for left/right
        # If left_metric is sales, apply filter for left series calculation
        
        series = []
        if not df.empty:
            df = df.set_index('order_create_date')
            rule = 'W'
            if time_grain == 'day':
                rule = 'D'
            elif time_grain == 'month':
                rule = 'ME'
                
            resampled = df.resample(rule)
            
            # Left metric logic
            if left_metric == 'sales':
                 # Custom handling for sales = lock_time notna
                 # We can't easily resample filtered and unfiltered in one pass without separate dataframes or columns
                 # Simpler approach: calculate left series from filtered data
                 
                 # Create a copy for sales metric
                 df_sales = df[df['lock_time'].notna()]
                 left_series = df_sales.resample(rule).size()
            else:
                 left_series = resampled.size()
            
            # Right metric = lock count (example)
            right_series = resampled['lock_time'].count()
            
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
