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
        df = dm.filter_data(date_range)
        
        # Apply metric definition
        if metric == 'sales' and 'lock_time' in df.columns:
            df = df[df['lock_time'].notna()]
        
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
