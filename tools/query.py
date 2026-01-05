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
        
        # Determine time column based on metric
        time_col = 'order_create_date'
        if metric in ['sales', '锁单量']:
            time_col = 'lock_time'
        elif metric in ['开票量']:
            time_col = 'invoice_upload_time'
            
        df = dm.filter_data(date_range, time_col=time_col)
        
        # Apply metric definition (filter NaNs if not done by filter_data's implicit date logic)
        # Note: filter_data already filters for valid dates in time_col, so NaNs are removed.
        # But for safety and consistency with other filters:
        if metric in ['sales', '锁单量'] and 'lock_time' in df.columns:
             # This is technically redundant if filter_data returned only rows where lock_time falls in range
             # but keeps logic explicit.
            df = df[df['lock_time'].notna()]
        elif metric in ['开票量'] and 'invoice_upload_time' in df.columns and 'lock_time' in df.columns:
            df = df[df['invoice_upload_time'].notna() & df['lock_time'].notna()]
        
        # Default to count if metric is sales or unspecified
        value = len(df)

        return {
            "value": value,
            "metric": metric,
            "signals": [],
        }
