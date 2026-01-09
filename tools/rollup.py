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
        dimensions = params.get("dimensions")
        date_range = params.get("date_range")
        filters = params.get("filters")

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
            if "lock_time" in df.columns and "delivery_date" in df.columns:
                df = df[df["lock_time"].notna() & df["delivery_date"].notna()]
        elif metric in ["开票量", "开票数", "开票金额", "invoice_amount"]:
            if "invoice_upload_time" in df.columns and "lock_time" in df.columns:
                df = df[df["invoice_upload_time"].notna() & df["lock_time"].notna()]
        elif metric in ["小订数", "小订量"] and "intention_payment_time" in df.columns:
            df = df[df["intention_payment_time"].notna()]

        rows: List[Dict[str, Any]] = []
        if dimensions and isinstance(dimensions, list):
            group_fields = [str(d) for d in dimensions if d]
        elif dimension:
            group_fields = [str(dimension)]
        else:
            group_fields = []

        if "age_band" in group_fields and "age_band" not in df.columns and "age" in df.columns:
            age_num = pd.to_numeric(df["age"], errors="coerce")
            bins = [0, 18, 25, 35, 45, 55, 65, 200]
            labels = ["0-17", "18-24", "25-34", "35-44", "45-54", "55-64", "65+"]
            df = df.copy()
            df["age_band"] = pd.cut(age_num, bins=bins, labels=labels, right=False, include_lowest=True)

        valid_group_fields = [g for g in group_fields if g in df.columns]
        if valid_group_fields:
            if metric in ["开票金额", "invoice_amount"] and "invoice_amount" in df.columns:
                amount = pd.to_numeric(df["invoice_amount"], errors="coerce").fillna(0)
                grouped = amount.groupby([df[g] for g in valid_group_fields], observed=True).sum().sort_values(ascending=False)
                rows = []
                for idx, v in grouped.items():
                    if not isinstance(idx, tuple):
                        idx = (idx,)
                    row = {g: (None if pd.isna(val) else str(val)) for g, val in zip(valid_group_fields, idx)}
                    row["value"] = float(v)
                    rows.append(row)
            else:
                grouped = df.groupby(valid_group_fields, observed=True).size().sort_values(ascending=False)
                rows = []
                for idx, v in grouped.items():
                    if not isinstance(idx, tuple):
                        idx = (idx,)
                    row = {g: (None if pd.isna(val) else str(val)) for g, val in zip(valid_group_fields, idx)}
                    row["value"] = int(v)
                    rows.append(row)
        else:
            # Fallback
            if metric in ["开票金额", "invoice_amount"] and "invoice_amount" in df.columns:
                val = float(pd.to_numeric(df["invoice_amount"], errors="coerce").fillna(0).sum())
                rows = [{"dimension": "All", "value": val}]
            else:
                rows = [
                    {"dimension": "All", "value": len(df)},
                ]

        return {
            "metric": metric,
            "dimension": dimension,
            "dimensions": dimensions,
            "date_range": date_range,
            "rows": rows,
            "signals": [],
        }
