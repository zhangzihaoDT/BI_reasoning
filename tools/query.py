# tools/query.py
import json
import os
from tools.base import BaseTool
from runtime.context import DataManager
import pandas as pd

class QueryTool(BaseTool):
    name = "query"
    """
    Executes data queries on the order dataset.
    
    Parameters:
    - date_range (str): Time range filter (e.g., "2024-01-01/2024-01-31", "last_30_days").
    - metric (str): Metric to calculate (e.g., "sales", "交付量", "invoice_amount").
    - filters (dict|list): Additional filters on columns (e.g., {"product_name": "LS6"}).
    - interval (str, optional): Aggregation interval ("day", "week", "month", "year"). 
      If provided, returns a dictionary of {date: value}. If omitted, returns a single scalar value.
    """

    def can_handle(self, step: dict) -> bool:
        return step.get("tool") == "query"

    def execute(self, step: dict, state: dict):
        print(f"[QueryTool] executing: {step['id']}")
        params = step.get("parameters", {})
        date_range = params.get("date_range")
        metric = params.get("metric")
        filters = params.get("filters")

        # Load business definition for age limits
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        biz_def_path = os.path.join(base_dir, "world", "business_definition.json")
        age_limit = [18, 80]
        if os.path.exists(biz_def_path):
            try:
                with open(biz_def_path, 'r', encoding='utf-8') as f:
                    biz_def = json.load(f)
                    age_limit = biz_def.get("age_limit", [18, 80])
            except Exception:
                pass

        dm = DataManager()

        metric = str(metric) if metric is not None else None
        
        # Check if metric belongs to Assign Data
        assign_metrics = [
            "下发线索数", "下发线索当日试驾数", "下发线索 7 日试驾数", "下发线索 7 日锁单数",
            "下发线索 30日试驾数", "下发线索 30 日锁单数", "下发门店数",
            "下发线索当日锁单数 (门店)", "下发线索数 (门店)"
        ]
        
        if metric in assign_metrics:
            print(f"[QueryTool] Using Assign Data for metric: {metric}")
            # Note: Assign Data is global (no series breakdown), so series filters are ignored.
            # We allow the query to proceed to return global values (best effort).
            
            df = dm.get_assign_data()
            df = dm.filter_assign_data(date_range)
            
            if df.empty:
                return {"value": 0, "metric": metric, "filters": filters, "sample_size": 0}
                
            # Compute value
            if params.get("interval"):
                # Time series
                interval = params.get("interval")
                # Group by interval
                if interval == "day":
                    # df has 'assign_date'
                    if 'assign_date' in df.columns:
                        grouped = df.groupby(df['assign_date'].dt.strftime('%Y-%m-%d'))[metric].sum()
                        return {"value": grouped.to_dict(), "metric": metric, "interval": interval, "filters": filters}
                elif interval == "month":
                    if 'assign_date' in df.columns:
                        grouped = df.groupby(df['assign_date'].dt.strftime('%Y-%m'))[metric].sum()
                        return {"value": grouped.to_dict(), "metric": metric, "interval": interval, "filters": filters}
                # Default/Fallback
                return {"value": df[metric].sum(), "metric": metric, "filters": filters}
            else:
                # Scalar
                val = df[metric].sum()
                return {"value": val, "metric": metric, "filters": filters, "sample_size": len(df)}

        time_col = "order_create_date"
        if metric in ["sales", "锁单量", "锁单数"]:
            time_col = "lock_time"
        elif metric in ["交付数", "交付量"]:
            time_col = "delivery_date"
        elif metric in ["开票量", "开票数", "开票金额", "invoice_amount"]:
            time_col = "invoice_upload_time"
        elif metric in ["小订数", "小订量"]:
            time_col = "intention_payment_time"

        # Special handling for relative launch date:
        # We need to pre-filter by series to allow filter_data to resolve "launch_plus_Nd"
        # because filter_data needs to know WHICH series' launch date to use.
        
        # 1. Get raw data first (no date filter)
        df = dm.get_data()
        
        # 2. Apply explicit filters (e.g. series=LS9)
        df = dm.apply_filters(df, filters)

        # 3. Now apply date filter (which might need series context)
        # Note: We need to expose a new method or use filter_data carefully.
        # Since filter_data takes date_range and returns filtered DF, 
        # and our improved filter_data now checks the DF content for series.
        # But wait, dm.filter_data calls dm.get_data() internally and filters IT.
        # It does NOT take an existing DF.
        # So we need to refactor or hack it. 
        # Hack: Pass the pre-filtered DF to a new helper or modify filter_data to accept df?
        # Actually, let's look at context.py again. filter_data calls get_data().
        
        # Strategy: 
        # We can implement the logic here in QueryTool if it's specific to this flow, 
        # OR we modify context.py to allow passing an external DF.
        # Let's modify context.py to be more flexible.
        
        df = dm.filter_data_on_df(df, date_range, time_col=time_col)
        
        # 4. (Optional) Re-apply filters? No, already applied.
        # But wait, standard flow was: dm.filter_data -> apply filters.
        # Now we reversed it: apply filters -> dm.filter_data_on_df.
        # This is better for performance anyway (filter series first).

        if metric in ["sales", "锁单量", "锁单数"] and "lock_time" in df.columns:
            df = df[df["lock_time"].notna()]
        elif metric in ["交付数", "交付量"]:
            if "delivery_date" in df.columns:
                df = df[df["delivery_date"].notna()]
        elif metric in ["开票量", "开票数", "开票金额", "invoice_amount"]:
            if "invoice_upload_time" in df.columns:
                df = df[df["invoice_upload_time"].notna()]
        elif metric in ["小订数", "小订量"] and "intention_payment_time" in df.columns:
            df = df[df["intention_payment_time"].notna()]
        elif metric in ["age", "年龄", "平均年龄"] and "age" in df.columns:
            df = df[df["age"].notna()]
            # Apply business rule age filter
            df["age"] = pd.to_numeric(df["age"], errors="coerce")
            df = df[(df["age"] >= age_limit[0]) & (df["age"] <= age_limit[1])]
            
            # Record implicit filter for display
            if filters is None:
                filters = []
            filters.append({"field": "age", "op": "between", "value": age_limit})

        interval = params.get("interval")
        if interval and time_col in df.columns and not df.empty:
            rule_map = {
                "day": "D", "daily": "D",
                "week": "W", "weekly": "W",
                "month": "ME", "monthly": "ME",
                "quarter": "QE", "quarterly": "QE",
                "year": "YE", "yearly": "YE"
            }
            rule = rule_map.get(str(interval).lower())
            
            if rule:
                # Ensure time_col is datetime
                if not pd.api.types.is_datetime64_any_dtype(df[time_col]):
                    df[time_col] = pd.to_datetime(df[time_col], errors='coerce')
                    df = df.dropna(subset=[time_col])

                if metric in ["开票金额", "invoice_amount"] and "invoice_amount" in df.columns:
                    df = df.copy()
                    df["invoice_amount"] = pd.to_numeric(df["invoice_amount"], errors="coerce").fillna(0)
                    series = df.set_index(time_col).resample(rule)["invoice_amount"].sum()
                else:
                    series = df.set_index(time_col).resample(rule).size()
                
                result_dict = {
                    k.strftime('%Y-%m-%d'): (v.item() if hasattr(v, 'item') else v)
                    for k, v in series.items()
                    if v > 0
                }

                return {
                    "value": result_dict,
                    "metric": metric,
                    "interval": interval,
                    "sample_size": len(df),
                    "filters": filters,
                    "signals": [],
                }

        sample_size = len(df)

        if metric in ["开票金额", "invoice_amount"] and "invoice_amount" in df.columns:
            value = float(pd.to_numeric(df["invoice_amount"], errors="coerce").fillna(0).sum())
        elif metric in ["age", "年龄", "平均年龄"] and "age" in df.columns:
            value = float(pd.to_numeric(df["age"], errors="coerce").mean())
            if pd.isna(value):
                value = 0.0
            else:
                value = round(value, 1)
        else:
            value = int(len(df))

        return {
            "value": value,
            "metric": metric,
            "sample_size": sample_size,
            "filters": filters,
            "signals": [],
        }
