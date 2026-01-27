import json
import os
from typing import Any, Dict, List
import pandas as pd

from tools.base import BaseTool
from runtime.context import DataManager


class RollupTool(BaseTool):
    name = "rollup"

    def can_handle(self, step: dict) -> bool:
        return step.get("tool") == "rollup" or step.get("tool") == "top_n"

    def execute(self, step: dict, state: dict):
        tool_name = step.get("tool")
        params = step.get("parameters", {})
        metric = params.get("metric")
        dimension = params.get("dimension")
        dimensions = params.get("dimensions")
        date_range = params.get("date_range")
        filters = params.get("filters")
        interval = params.get("interval") # Add interval support
        
        # Top N specific params
        n_limit = params.get("n", 5)
        order = params.get("order", "desc")

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

        # Special handling for relative launch date:
        # We need to pre-filter by series to allow filter_data to resolve "launch_plus_Nd"
        # because filter_data needs to know WHICH series' launch date to use.
        
        # 1. Get raw data first (no date filter)
        df = dm.get_data()

        # 2. Apply explicit filters (e.g. series=LS9)
        # Use DataManager's apply_filters to handle model_series_mapping expansion
        df = dm.apply_filters(df, filters)

        # 3. Now apply date filter (which might need series context)
        # Use filter_data_on_df instead of filter_data
        df = dm.filter_data_on_df(df, date_range, time_col=time_col)


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

        rows: List[Dict[str, Any]] = []
        if dimensions and isinstance(dimensions, list):
            group_fields = [str(d) for d in dimensions if d]
        elif dimension:
            group_fields = [str(dimension)]
        else:
            group_fields = []

        # Handle time-based grouping if 'interval' is set or dimensions contain time keywords
        time_dim = None
        if interval:
            time_dim = interval
        else:
            # Auto-detect if any dimension is time-like but not in columns
            for d in group_fields:
                if d in ["day", "date", "week", "month", "year"] and d not in df.columns:
                    time_dim = d
                    break
        
        if time_dim:
            # Create the time column
            if time_col in df.columns:
                # Add the derived column to df
                # Use a specific name to avoid collision, e.g., "_date_grouped"
                # But we need to match what's in group_fields or update group_fields
                
                # If the dimension was requested as "day" or "date", we should map it.
                # If interval is set but dimension not explicit, we might need to add it?
                # Usually Rollup needs explicit dimensions.
                
                # Let's standardize: if "day"/"date" is in dimensions, map it to the derived col.
                # If not in dimensions but interval is set, maybe the user expects time series?
                # RollupTool typically expects dimensions to be explicit.
                
                # Mapping:
                # "day", "date" -> YYYY-MM-DD
                # "month" -> YYYY-MM
                # "year" -> YYYY
                
                derived_col_name = time_dim # e.g. "day"
                
                if time_dim in ["day", "date"]:
                    df[derived_col_name] = df[time_col].dt.strftime('%Y-%m-%d')
                elif time_dim == "month":
                    df[derived_col_name] = df[time_col].dt.strftime('%Y-%m')
                elif time_dim == "year":
                    df[derived_col_name] = df[time_col].dt.strftime('%Y')
                elif time_dim == "week":
                    df[derived_col_name] = df[time_col].dt.strftime('%Y-W%U')
                
                # Ensure this derived column is in group_fields if it wasn't already
                if derived_col_name not in group_fields:
                    # If interval was passed but not in dimensions, we prepend it to group by time first
                    group_fields.insert(0, derived_col_name) 

        # Capture the time range bounds if possible, to reindex later
        # We need the ACTUAL range applied, which might come from date_range parsing.
        # But filter_data_on_df applies it inside. We can inspect df[time_col] range.
        # However, df[time_col] only has present data. We want the REQUESTED range.
        # This is tricky without returning the range from filter_data.
        # But we can re-parse date_range here for bounds if it's "launch_plus_Nd".
        
        expected_time_range = None
        expected_time_range_strs = []
        if date_range and date_range.startswith("launch_plus_") and time_dim in ["day", "date"]:
             try:
                 import re
                 days_match = re.search(r'(\d+)d', date_range)
                 days = int(days_match.group(1)) if days_match else 7
                 
                 target_series = None
                 
                 # Normalize filters to list for inspection
                 temp_filters = filters
                 if isinstance(temp_filters, dict):
                     temp_filters = [{"field": k, "op": "=", "value": v} for k, v in temp_filters.items()]
                 
                 if temp_filters:
                     for f in temp_filters:
                         if isinstance(f, dict) and f.get("field") == "series" and f.get("op") in ["=", "=="]:
                             target_series = f.get("value")
                             break
                 
                 if not target_series and 'series' in df.columns:
                     unique = df['series'].dropna().unique()
                     if len(unique) == 1:
                         target_series = unique[0]
                 
                 if target_series:
                     # Load biz_def if not loaded (copy-paste logic from top, but simplified)
                     if 'biz_def' not in locals():
                         if os.path.exists(biz_def_path):
                             with open(biz_def_path, 'r', encoding='utf-8') as f:
                                 biz_def = json.load(f)
                         else:
                             biz_def = {}

                     launch_info = biz_def.get("time_periods", {}).get(target_series)
                     # Correctly use 'end' as Launch Date based on schema.md
                     if launch_info and "end" in launch_info:
                         launch_date = pd.to_datetime(launch_info["end"])
                         end_date = launch_date + pd.Timedelta(days=days - 1)
                         expected_time_range = pd.date_range(start=launch_date, end=end_date, freq='D')
                         expected_time_range_strs = expected_time_range.strftime('%Y-%m-%d').tolist()
             except Exception as e:
                 print(f"[RollupTool] Error calculating expected time range: {e}")
                 pass

        if "age_band" in group_fields and "age_band" not in df.columns and "age" in df.columns:
            age_num = pd.to_numeric(df["age"], errors="coerce")
            bins = [0, 18, 25, 35, 45, 55, 65, 200]
            labels = ["0-17", "18-24", "25-34", "35-44", "45-54", "55-64", "65+"]
            df = df.copy()
            df["age_band"] = pd.cut(age_num, bins=bins, labels=labels, right=False, include_lowest=True)

        valid_group_fields = [g for g in group_fields if g in df.columns]
        if valid_group_fields:
            # Helper to fill zeros for time series
            def _fill_time_zeros(grouped_series, _expected_time_range_strs, _time_col_name):
                # If it's a MultiIndex (e.g. day, product_name)
                if isinstance(grouped_series.index, pd.MultiIndex):
                    # Check which level is the time column
                    time_level_idx = -1
                    if _time_col_name in grouped_series.index.names:
                        time_level_idx = grouped_series.index.names.index(_time_col_name)
                    
                    if time_level_idx != -1:
                        # Reindex logic for MultiIndex
                        # We want the Cartesian product of (Expected Times) x (Other Levels)
                        # Or at least ensure Expected Times are present for existing Other Levels?
                        # Usually "by day by product", we want all days for each product that appears.
                        
                        # Get unique values for other levels
                        levels = [list(lvl) for lvl in grouped_series.index.levels]
                        # Replace the time level with our expected range
                        # Note: This assumes the time level in index matches the format of expected_time_range_strs
                        # Since we derived the column using strftime('%Y-%m-%d') and expected is same format.
                        
                        # We can't just modify levels[time_level_idx] because reindex needs a full MultiIndex
                        # Let's use pd.MultiIndex.from_product
                        
                        # Extract unique values for non-time dimensions from the data
                        # (We only fill zeros for products that actually exist in the filtered data, 
                        #  we don't invent products that have 0 sales ever)
                        
                        new_levels = []
                        for i, name in enumerate(grouped_series.index.names):
                            if name == _time_col_name:
                                new_levels.append(_expected_time_range_strs)
                            else:
                                # Keep existing unique values
                                new_levels.append(grouped_series.index.get_level_values(i).unique())
                                
                        full_idx = pd.MultiIndex.from_product(new_levels, names=grouped_series.index.names)
                        return grouped_series.reindex(full_idx, fill_value=0)
                else:
                    # Single Index (just time)
                    if grouped_series.index.name == _time_col_name:
                        return grouped_series.reindex(_expected_time_range_strs, fill_value=0)
                
                return grouped_series

            if metric in ["开票金额", "invoice_amount"] and "invoice_amount" in df.columns:
                amount = pd.to_numeric(df["invoice_amount"], errors="coerce").fillna(0)
                grouped = amount.groupby([df[g] for g in valid_group_fields], observed=True).sum()
                
                # Apply zero-filling if we have expected range and the time column is in grouping
                if expected_time_range is not None and time_dim in valid_group_fields:
                    grouped = _fill_time_zeros(grouped, expected_time_range_strs, time_dim)
                
                # Sort descending? For time series, usually we want sorted by time? 
                # Or user wants "top products"?
                # If "by day", usually chronological.
                # If "by product", usually by value.
                # Mixed? "by day by product" -> sort by day then product?
                # The original code did .sort_values(ascending=False).
                # If we have time, maybe we shouldn't sort by value globally.
                # Let's keep existing behavior (sort by value) but ensure all days are present.
                # Or, if time is primary, sort by time?
                # Let's sort by index (which puts time first if it's first dim)
                if time_dim in valid_group_fields:
                     grouped = grouped.sort_index()
                else:
                     if tool_name == "top_n":
                         ascending = (order == "asc")
                         grouped = grouped.sort_values(ascending=ascending)
                         if n_limit:
                             grouped = grouped.head(n_limit)
                     else:
                         grouped = grouped.sort_values(ascending=False)
                
                rows = []
                for idx, v in grouped.items():
                    if not isinstance(idx, tuple):
                        idx = (idx,)
                    row = {g: (None if pd.isna(val) else str(val)) for g, val in zip(valid_group_fields, idx)}
                    row["value"] = float(v)
                    rows.append(row)
            elif metric in ["age", "年龄", "平均年龄"] and "age" in df.columns:
                age_num = pd.to_numeric(df["age"], errors="coerce")
                grouped = age_num.groupby([df[g] for g in valid_group_fields], observed=True).mean()
                
                if expected_time_range is not None and time_dim in valid_group_fields:
                    grouped = _fill_time_zeros(grouped, expected_time_range_strs, time_dim)
                
                if time_dim in valid_group_fields:
                     grouped = grouped.sort_index()
                else:
                     if tool_name == "top_n":
                         ascending = (order == "asc")
                         grouped = grouped.sort_values(ascending=ascending)
                         if n_limit:
                             grouped = grouped.head(n_limit)
                     else:
                         grouped = grouped.sort_values(ascending=False)

                rows = []
                for idx, v in grouped.items():
                    if not isinstance(idx, tuple):
                        idx = (idx,)
                    row = {g: (None if pd.isna(val) else str(val)) for g, val in zip(valid_group_fields, idx)}
                    row["value"] = round(float(v), 1)
                    rows.append(row)
            else:
                grouped = df.groupby(valid_group_fields, observed=True).size()
                
                if expected_time_range is not None and time_dim in valid_group_fields:
                    grouped = _fill_time_zeros(grouped, expected_time_range_strs, time_dim)
                
                if time_dim in valid_group_fields:
                     grouped = grouped.sort_index()
                else:
                     if tool_name == "top_n":
                         ascending = (order == "asc")
                         grouped = grouped.sort_values(ascending=ascending)
                         if n_limit:
                             grouped = grouped.head(n_limit)
                     else:
                         grouped = grouped.sort_values(ascending=False)

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
            elif metric in ["age", "年龄", "平均年龄"] and "age" in df.columns:
                val = float(pd.to_numeric(df["age"], errors="coerce").mean())
                if pd.isna(val):
                    val = 0.0
                else:
                    val = round(val, 1)
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
            "sample_size": len(df),
            "filters": filters,
            "rows": rows,
            "signals": [],
        }
