from typing import Any, Dict, List
import pandas as pd

from tools.base import BaseTool
from runtime.context import DataManager


class RollupTool(BaseTool):
    name = "rollup"

    def can_handle(self, step: dict) -> bool:
        return step.get("tool") == "rollup"

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
        
        # Apply metric definition
        if metric in ['sales', '锁单量'] and 'lock_time' in df.columns:
            df = df[df['lock_time'].notna()]
        elif metric in ['开票量'] and 'invoice_upload_time' in df.columns and 'lock_time' in df.columns:
            df = df[df['invoice_upload_time'].notna() & df['lock_time'].notna()]
        
        rows: List[Dict[str, Any]] = []
        if dimension and dimension in df.columns:
            # Group by dimension and count
            grouped = df.groupby(dimension).size().sort_values(ascending=False)
            rows = [
                {"dimension": str(k), "value": int(v)}
                for k, v in grouped.items()
            ]
        elif dimension == "series_group" and "series_group" in df.columns:
             # Just in case explicit check
            grouped = df.groupby("series_group").size().sort_values(ascending=False)
            rows = [
                {"dimension": str(k), "value": int(v)}
                for k, v in grouped.items()
            ]
        else:
            # Fallback
            rows = [
                {"dimension": "All", "value": len(df)},
            ]

        return {
            "metric": metric,
            "dimension": dimension,
            "date_range": date_range,
            "rows": rows,
            "signals": [],
        }
