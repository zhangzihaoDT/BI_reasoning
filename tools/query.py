# tools/query.py
from tools.base import BaseTool
from runtime.context import DataManager

class QueryTool(BaseTool):
    name = "query"

    def can_handle(self, step: dict) -> bool:
        return step.get("tool") == "query"

    def execute(self, step: dict, state: dict):
        print(f"[QueryTool] executing: {step['id']}")
        params = step.get("parameters", {})
        date_range = params.get("date_range")
        metric = params.get("metric")

        dm = DataManager()
        df = dm.filter_data(date_range)
        
        # Apply metric definition
        if metric == 'sales' and 'lock_time' in df.columns:
            df = df[df['lock_time'].notna()]
        
        # Default to count if metric is sales or unspecified
        value = len(df)

        return {
            "value": value,
            "metric": metric,
            "signals": [],
        }
