from __future__ import annotations

from typing import Any, Dict, List, Optional
import pandas as pd
import numpy as np
import re

from tools.base import BaseTool
from runtime.context import DataManager

class HistogramTool(BaseTool):
    name = "histogram"

    def can_handle(self, step: dict) -> bool:
        return step.get("tool") == "histogram"

    def execute(self, step: dict, state: dict):
        params = step.get("parameters", {})
        metric = params.get("metric")
        # Increase default bins to 30 for better resolution
        bins_param = params.get("bins", 30)
        date_range = params.get("date_range")
        compare_date_range = params.get("compare_date_range")
        
        # Determine time column
        time_col = 'order_create_date'
        if 'lock' in str(metric) or 'lock_time' in str(metric):
            time_col = 'lock_time'

        dm = DataManager()
        
        # Helper to parse dates robustly including Chinese format
        def parse_dates(series: pd.Series) -> pd.Series:
            if series.empty:
                return pd.to_datetime(series)
            
            # First try standard parsing
            parsed = pd.to_datetime(series, errors='coerce')
            
            # If we have NaT and input was string, try Chinese format
            # Chinese format: YYYY年M月D日
            if parsed.isna().all() and series.dtype == 'object':
                try:
                    # Clean the string to standard format: YYYY-M-D
                    cleaned = series.astype(str).str.replace('年', '-').str.replace('月', '-').str.replace('日', '')
                    parsed = pd.to_datetime(cleaned, errors='coerce')
                except Exception:
                    pass
            
            # If mixed (some parsed, some not), try to fill gaps? 
            # For now, let's assume consistent format per column mostly.
            # But let's handle the case where some are NaT after first pass but could be Chinese
            if parsed.isna().any() and series.dtype == 'object':
                 # Fallback for remaining NaTs
                 mask = parsed.isna()
                 try:
                     cleaned = series[mask].astype(str).str.replace('年', '-').str.replace('月', '-').str.replace('日', '')
                     parsed.loc[mask] = pd.to_datetime(cleaned, errors='coerce')
                 except Exception:
                     pass
            
            return parsed

        # Helper to get series
        def get_metric_series(df: pd.DataFrame, metric_expr: str) -> pd.Series:
            if not isinstance(metric_expr, str):
                return pd.Series()
                
            # Check for datediff('day', start, end)
            match = re.search(r"datediff\('day',\s*([a-zA-Z0-9_]+),\s*([a-zA-Z0-9_]+)\)", metric_expr)
            if match:
                start_col, end_col = match.groups()
                if start_col in df.columns and end_col in df.columns:
                    # Ensure datetime with robust parsing
                    s = parse_dates(df[start_col])
                    e = parse_dates(df[end_col])
                    diff = (e - s).dt.days
                    return diff.dropna()
            
            # Normal metric
            if metric_expr in df.columns:
                return pd.to_numeric(df[metric_expr], errors='coerce').dropna()
                
            return pd.Series()

        # Get data for primary range
        df_primary = dm.filter_data(date_range, time_col=time_col)
        series_primary = get_metric_series(df_primary, metric)
        
        result = {
            "metric": metric,
            "date_range": date_range,
            "bins": [],
            "signals": []
        }
        
        if series_primary.empty:
            result["signals"].append({
                "type": "data_quality_signal",
                "status": "warning",
                "message": f"Insufficient data to calculate distribution for {metric} in {date_range}. (Sample size: 0)"
            })
            return result

        # Prepare for comparison
        series_compare = pd.Series()
        if compare_date_range:
            df_compare = dm.filter_data(compare_date_range, time_col=time_col)
            series_compare = get_metric_series(df_compare, metric)

        # Compute Bins
        combined = series_primary
        if not series_compare.empty:
            combined = pd.concat([series_primary, series_compare])
            
        if combined.empty:
             return result

        # Smart Binning: Handle outliers by clipping to P99
        # This prevents long-tail outliers from compressing the main distribution
        upper_bound = np.percentile(combined, 99)
        # Ensure we have at least a small range if all values are identical
        if upper_bound == combined.min():
            upper_bound = combined.max()
        
        # Define range for histogram (min to P99)
        # Note: Values > P99 will be excluded from the bin counts unless we clip them.
        # Clipping is better to account for "overflow"
        hist_range = (combined.min(), upper_bound)
        
        # Calculate histogram with fixed range
        # Note: We don't clip data, we just set the range. Outliers are ignored in bin counts.
        # To include them in the last bin, we should clip.
        combined_clipped = np.clip(combined, hist_range[0], hist_range[1])
        
        hist_values, bin_edges = np.histogram(combined_clipped, bins=bins_param, range=hist_range)
        
        # Calculate distributions
        def calc_dist(series, edges, clip_max):
            # Clip series to match the histogram range logic
            series_clipped = np.clip(series, edges[0], clip_max)
            counts, _ = np.histogram(series_clipped, bins=edges)
            total = len(series)
            if total == 0:
                return [0.0] * len(counts)
            return [float(c)/total for c in counts]

        dist_primary = calc_dist(series_primary, bin_edges, upper_bound)
        
        # Build bin descriptions
        bins_data = []
        for i in range(len(bin_edges)-1):
            bins_data.append({
                "range": f"[{bin_edges[i]:.1f}, {bin_edges[i+1]:.1f})",
                "min": float(bin_edges[i]),
                "max": float(bin_edges[i+1]),
                "primary_pct": dist_primary[i],
                "compare_pct": 0.0
            })
            
        # Comparison logic
        if not series_compare.empty:
            dist_compare = calc_dist(series_compare, bin_edges, upper_bound)
            for i, d in enumerate(bins_data):
                d["compare_pct"] = dist_compare[i]
            
            # Calculate distance (Sum of Absolute Differences)
            sad = sum(abs(p - c) for p, c in zip(dist_primary, dist_compare))
            
            result["comparison"] = {
                "compare_date_range": compare_date_range,
                "distance": sad,
                "threshold": 0.3
            }
            
            # Generate signal
            threshold = 0.3
            is_abnormal = sad > threshold
            
            result["signals"].append({
                "type": "distribution_signal",
                "metric": metric,
                "status": "abnormal" if is_abnormal else "normal",
                "score": sad,
                "message": f"Distribution difference score {sad:.2f} ({'Abnormal' if is_abnormal else 'Normal'})"
            })

        result["bins"] = bins_data
        
        return result
