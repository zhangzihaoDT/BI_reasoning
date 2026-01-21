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
        time_col = "order_create_date"
        if metric in ["sales", "锁单量", "锁单数"]:
            time_col = "lock_time"
        elif metric in ["交付数", "交付量"]:
            time_col = "delivery_date"
        elif metric in ["开票量", "开票数", "开票金额", "invoice_amount"]:
            time_col = "invoice_upload_time"
        elif metric in ["小订数", "小订量"]:
            time_col = "intention_payment_time"

        df = dm.filter_data(date_range, time_col=time_col)

        def _apply_filters(_df: pd.DataFrame, _filters):
            if _df.empty or not _filters:
                return _df

            if isinstance(_filters, dict):
                _filters = [{"field": k, "op": "=", "value": v} for k, v in _filters.items()]

            if not isinstance(_filters, list):
                return _df

            for f in _filters:
                if not isinstance(f, dict):
                    continue
                field = f.get("field")
                op = (f.get("op") or "=").lower()
                value = f.get("value")
                if not field or field not in _df.columns:
                    continue

                if op in ["=", "=="]:
                    _df = _df[_df[field] == value]
                elif op in ["!=", "<>"]:
                    _df = _df[_df[field] != value]
                elif op == "in":
                    values = value if isinstance(value, list) else [value]
                    _df = _df[_df[field].isin(values)]
                elif op == "contains":
                    _df = _df[_df[field].astype(str).str.contains(str(value), na=False)]
                elif op in ["not_null", "notna", "exists", "is not null", "not null", "is_not_null"]:
                    _df = _df[_df[field].notna()]
                elif op in [">", ">=", "<", "<="]:
                    s = pd.to_numeric(_df[field], errors="coerce")
                    v = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
                    if pd.isna(v):
                        continue
                    if op == ">":
                        _df = _df[s > v]
                    elif op == ">=":
                        _df = _df[s >= v]
                    elif op == "<":
                        _df = _df[s < v]
                    elif op == "<=":
                        _df = _df[s <= v]
            return _df

        df = _apply_filters(df, filters)

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
