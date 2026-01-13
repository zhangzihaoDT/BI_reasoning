from __future__ import annotations

from typing import Any, Dict, List, Optional
import pandas as pd
import numpy as np
import re

from tools.base import BaseTool
from runtime.context import DataManager

class DistributionTool(BaseTool):
    name = "distribution"

    def can_handle(self, step: dict) -> bool:
        return step.get("tool") == "distribution" or step.get("tool") == "histogram"

    def execute(self, step: dict, state: dict):
        params = step.get("parameters", {})
        metric = params.get("metric")
        dimension = params.get("dimension")  # New: Categorical dimension
        bins_param = params.get("bins", 30)
        date_range = params.get("date_range")
        compare_date_range = params.get("compare_date_range")
        
        # Determine time column
        time_col = 'order_create_date'
        if 'lock' in str(metric) or 'lock_time' in str(metric):
            time_col = 'lock_time'
        if 'assign' in str(metric): # Handle assign data if needed, though mostly sales
             time_col = 'assign_date'

        dm = DataManager()
        
        # Helper to parse dates robustly including Chinese format
        def parse_dates(series: pd.Series) -> pd.Series:
            if series.empty:
                return pd.to_datetime(series)
            parsed = pd.to_datetime(series, errors='coerce')
            if parsed.isna().all() and series.dtype == 'object':
                try:
                    cleaned = series.astype(str).str.replace('年', '-').str.replace('月', '-').str.replace('日', '')
                    parsed = pd.to_datetime(cleaned, errors='coerce')
                except Exception:
                    pass
            if parsed.isna().any() and series.dtype == 'object':
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
            match = re.search(r"datediff\('day',\s*([a-zA-Z0-9_]+),\s*([a-zA-Z0-9_]+)\)", metric_expr)
            if match:
                start_col, end_col = match.groups()
                if start_col in df.columns and end_col in df.columns:
                    s = parse_dates(df[start_col])
                    e = parse_dates(df[end_col])
                    diff = (e - s).dt.days
                    return diff.dropna()
            if metric_expr in df.columns:
                return pd.to_numeric(df[metric_expr], errors='coerce').dropna()
            return pd.Series()

        # Get data
        if 'assign' in str(metric):
             df_primary = dm.filter_assign_data(date_range)
        else:
             df_primary = dm.filter_data(date_range, time_col=time_col)

        result = {
            "metric": metric,
            "dimension": dimension,
            "date_range": date_range,
            "signals": []
        }

        if df_primary.empty:
             result["signals"].append({
                "type": "data_quality_signal",
                "status": "warning",
                "message": f"Insufficient data to calculate distribution for {metric} in {date_range}. (Sample size: 0)"
            })
             return result

        # --- Categorical Distribution (if dimension is provided) ---
        if dimension:
            if dimension not in df_primary.columns:
                 result["signals"].append({
                    "type": "error",
                    "status": "failed",
                    "message": f"Dimension {dimension} not found in data."
                })
                 return result
            
            # Calculate primary distribution (PMF)
            primary_counts = df_primary[dimension].value_counts(normalize=True)
            primary_total = len(df_primary)
            
            # Comparison
            compare_counts = pd.Series(dtype=float)
            if compare_date_range:
                if 'assign' in str(metric):
                     df_compare = dm.filter_assign_data(compare_date_range)
                else:
                     df_compare = dm.filter_data(compare_date_range, time_col=time_col)
                
                if not df_compare.empty and dimension in df_compare.columns:
                    compare_counts = df_compare[dimension].value_counts(normalize=True)
            
            # Align categories
            all_cats = set(primary_counts.index) | set(compare_counts.index)
            # Sort categories (by primary value descending, then name)
            sorted_cats = sorted(list(all_cats), key=lambda x: (-primary_counts.get(x, 0), x))
            
            dist_data = []
            sad = 0.0 # Sum of Absolute Differences
            
            # Limit to top N categories to avoid huge payload, but keep tail sum?
            # User wants analysis, usually top 20 is enough.
            # But for SAD calculation we need all.
            
            for cat in sorted_cats:
                p = float(primary_counts.get(cat, 0))
                c = float(compare_counts.get(cat, 0))
                diff = p - c
                sad += abs(diff)
                
                # Only include in output if significant or top ranked
                # Let's return top 20 + others
                dist_data.append({
                    "category": str(cat),
                    "primary_pct": p,
                    "compare_pct": c,
                    "diff_pct": diff
                })
            
            # Trim result for display if too long (keep top 30)
            final_dist_data = dist_data[:30]
            if len(dist_data) > 30:
                # Aggregate rest? Or just truncate. Truncate for now.
                pass

            result["distribution"] = final_dist_data
            
            if not compare_counts.empty:
                threshold = 0.2 # 20% total shift is significant
                is_abnormal = sad > threshold
                result["comparison"] = {
                    "compare_date_range": compare_date_range,
                    "distance": sad,
                    "threshold": threshold
                }
                result["signals"].append({
                    "type": "distribution_signal",
                    "metric": metric,
                    "dimension": dimension,
                    "status": "abnormal" if is_abnormal else "normal",
                    "score": sad,
                    "message": f"Structural shift score {sad:.2f} ({'Abnormal' if is_abnormal else 'Normal'})"
                })
            
            return result

        # --- Numerical Histogram (Legacy/Fallback) ---
        series_primary = get_metric_series(df_primary, metric)
        if series_primary.empty:
             return result

        series_compare = pd.Series(dtype=float)
        if compare_date_range:
            if 'assign' in str(metric):
                 df_compare = dm.filter_assign_data(compare_date_range)
            else:
                 df_compare = dm.filter_data(compare_date_range, time_col=time_col)
            series_compare = get_metric_series(df_compare, metric)

        combined = series_primary
        if not series_compare.empty:
            combined = pd.concat([series_primary, series_compare])
            
        if combined.empty:
             return result

        upper_bound = np.percentile(combined, 99)
        if upper_bound == combined.min():
            upper_bound = combined.max()
        
        hist_range = (combined.min(), upper_bound)
        combined_clipped = np.clip(combined, hist_range[0], hist_range[1])
        hist_values, bin_edges = np.histogram(combined_clipped, bins=bins_param, range=hist_range)
        
        def calc_dist(series, edges, clip_max):
            series_clipped = np.clip(series, edges[0], clip_max)
            counts, _ = np.histogram(series_clipped, bins=edges)
            total = len(series)
            if total == 0:
                return [0.0] * len(counts)
            return [float(c)/total for c in counts]

        dist_primary = calc_dist(series_primary, bin_edges, upper_bound)
        
        bins_data = []
        for i in range(len(bin_edges)-1):
            bins_data.append({
                "range": f"[{bin_edges[i]:.1f}, {bin_edges[i+1]:.1f})",
                "min": float(bin_edges[i]),
                "max": float(bin_edges[i+1]),
                "primary_pct": dist_primary[i],
                "compare_pct": 0.0
            })
            
        if not series_compare.empty:
            dist_compare = calc_dist(series_compare, bin_edges, upper_bound)
            for i, d in enumerate(bins_data):
                d["compare_pct"] = dist_compare[i]
            
            sad = sum(abs(p - c) for p, c in zip(dist_primary, dist_compare))
            result["comparison"] = {
                "compare_date_range": compare_date_range,
                "distance": sad,
                "threshold": 0.3
            }
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
