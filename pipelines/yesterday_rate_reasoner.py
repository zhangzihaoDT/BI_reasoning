"""
此脚本是 yesterday_rate.py 的 Reasoner 增强版。
功能：
1) 保留原有 DSL（转化率相关的趋势对比）
2) 在 DSL 完成后，计算“门店线索占比”和“门店当日锁单率”的历史对比与条件对比
3) 若判定为高风险，调度工具箱（rollup/trend）做结构归因
4) 使用 DeepSeek Reasoner 输出极简高密度简报
"""
import os
import sys
import argparse
import json
import time
from typing import Dict, Any, List, Tuple

import numpy as np
import pandas as pd

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agents.execution_graph import build_execution_graph
from runtime.context import DataManager


def _safe_rate(n: float, d: float) -> float:
    return float(n / d) if d and d > 0 else 0.0


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--date", type=str, help="Single date to analyze (YYYY-MM-DD or 'yesterday')")
    p.add_argument("--start", type=str, help="Start date for range analysis (YYYY-MM-DD)")
    p.add_argument("--end", type=str, help="End date for range analysis (YYYY-MM-DD)")
    p.add_argument("--history-start-days-ago", type=int, default=60)
    p.add_argument("--history-end-days-ago", type=int, default=30)
    p.add_argument("--z-threshold", type=float, default=2.0)
    p.add_argument("--z-mid", type=float, default=1.2)
    p.add_argument("--share-window", type=float, default=0.05, help="条件对比时门店线索占比的容忍窗口")
    args = p.parse_args()
    if not args.date and not args.start:
        args.date = "yesterday"
    return args


def _compute_today_and_history(dm: DataManager, target_date: pd.Timestamp, h_start: pd.Timestamp, h_end: pd.Timestamp) -> Dict[str, Any]:
    df = dm.get_assign_data().copy()
    if df.empty or "assign_date" not in df.columns:
        return {
            "today": {"leads": 0.0, "store_leads": 0.0, "store_lock_same_day": 0.0},
            "history": pd.DataFrame(columns=["assign_date", "leads", "store_leads", "store_lock_same_day"]),
        }
    df["assign_date"] = pd.to_datetime(df["assign_date"], errors="coerce")
    df = df[df["assign_date"].notna()]
    cols = ["下发线索数", "下发线索数 (门店)", "下发线索当日锁单数 (门店)"]
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
        else:
            df[c] = 0.0
    d_target = df[df["assign_date"].dt.normalize() == target_date.normalize()]
    today = {
        "leads": float(d_target["下发线索数"].sum()),
        "store_leads": float(d_target["下发线索数 (门店)"].sum()),
        "store_lock_same_day": float(d_target["下发线索当日锁单数 (门店)"].sum()),
    }
    df_hist = df[(df["assign_date"] >= h_start) & (df["assign_date"] <= h_end)]
    if df_hist.empty:
        hist_df = pd.DataFrame(columns=["assign_date", "leads", "store_leads", "store_lock_same_day"])
    else:
        daily = df_hist.groupby(df_hist["assign_date"].dt.normalize())[
            ["下发线索数", "下发线索数 (门店)", "下发线索当日锁单数 (门店)"]
        ].sum().reset_index()
        daily.rename(columns={
            "assign_date": "assign_date",
            "下发线索数": "leads",
            "下发线索数 (门店)": "store_leads",
            "下发线索当日锁单数 (门店)": "store_lock_same_day",
        }, inplace=True)
        hist_df = daily
    return {"today": today, "history": hist_df}


def _percent_rank(values: np.ndarray, x: float) -> float:
    n = int(values.size)
    if n == 0:
        return 0.0
    return float((values <= x).mean())


def assess_structure_risk(stats: Dict[str, Any], z_high: float, z_mid: float) -> Dict[str, Any]:
    today = stats["today"]
    hist_df = stats["history"]
    today_share = _safe_rate(today["store_leads"], today["leads"])
    today_store_rate = _safe_rate(today["store_lock_same_day"], today["store_leads"])
    share_hist = hist_df.copy()
    share_hist["store_share"] = share_hist.apply(lambda r: _safe_rate(r["store_leads"], r["leads"]), axis=1)
    share_hist["store_rate"] = share_hist.apply(lambda r: _safe_rate(r["store_lock_same_day"], r["store_leads"]), axis=1)
    share_values = share_hist["store_share"].to_numpy(dtype=float)
    rate_values = share_hist["store_rate"].to_numpy(dtype=float)
    share_mean = float(np.mean(share_values)) if share_values.size > 0 else 0.0
    share_std = float(np.std(share_values, ddof=1)) if share_values.size > 1 else 0.0
    rate_mean = float(np.mean(rate_values)) if rate_values.size > 0 else 0.0
    rate_std = float(np.std(rate_values, ddof=1)) if rate_values.size > 1 else 0.0
    share_z = float((today_share - share_mean) / share_std) if share_std > 0 else 0.0
    rate_z = float((today_store_rate - rate_mean) / rate_std) if rate_std > 0 else 0.0
    risk_level = "低"
    flag = "正常结构"
    if abs(share_z) >= z_high or abs(rate_z) >= z_high:
        risk_level = "高"
        flag = "结构性异常"
    elif abs(share_z) >= z_mid or abs(rate_z) >= z_mid:
        risk_level = "中"
        flag = "趋势性偏离"
    return {
        "store_share": today_share,
        "store_rate": today_store_rate,
        "share_mean": share_mean,
        "share_std": share_std,
        "rate_mean": rate_mean,
        "rate_std": rate_std,
        "share_z": share_z,
        "rate_z": rate_z,
        "risk_level": risk_level,
        "flag": flag,
    }


def conditional_rate_assessment(stats: Dict[str, Any], window: float) -> Dict[str, Any]:
    today = stats["today"]
    hist_df = stats["history"]
    today_share = _safe_rate(today["store_leads"], today["leads"])
    hist_df = hist_df.copy()
    hist_df["store_share"] = hist_df.apply(lambda r: _safe_rate(r["store_leads"], r["leads"]), axis=1)
    hist_df["store_rate"] = hist_df.apply(lambda r: _safe_rate(r["store_lock_same_day"], r["store_leads"]), axis=1)
    lower = max(0.0, today_share - window)
    upper = min(1.0, today_share + window)
    cond = hist_df[(hist_df["store_share"] >= lower) & (hist_df["store_share"] <= upper)]
    cond_values = cond["store_rate"].to_numpy(dtype=float)
    cond_mean = float(np.mean(cond_values)) if cond_values.size > 0 else 0.0
    cond_std = float(np.std(cond_values, ddof=1)) if cond_values.size > 1 else 0.0
    today_store_rate = _safe_rate(today["store_lock_same_day"], today["store_leads"])
    cond_z = float((today_store_rate - cond_mean) / cond_std) if cond_std > 0 else 0.0
    return {
        "window": window,
        "share_lower": lower,
        "share_upper": upper,
        "conditional_mean": cond_mean,
        "conditional_std": cond_std,
        "today_store_rate": today_store_rate,
        "conditional_z": cond_z,
        "n_days": int(cond_values.size),
    }


def _build_dsl(date_range: str) -> List[Dict[str, Any]]:
    return [
        {
            "id": "assign_leads_mom",
            "tool": "trend",
            "parameters": {"metric": "assign_leads", "time_grain": "day", "compare_type": "mom", "date_range": date_range},
        },
        {
            "id": "assign_rate_7d_lock",
            "tool": "trend",
            "parameters": {"metric": "assign_rate_7d_lock", "time_grain": "day", "compare_type": "mom", "date_range": date_range},
        },
        {
            "id": "assign_rate_7d_test_drive",
            "tool": "trend",
            "parameters": {"metric": "assign_rate_7d_test_drive", "time_grain": "day", "compare_type": "mom", "date_range": date_range},
        },
    ]


def _toolbox_for_high_risk(date_range: str, compare_date_range: str = None) -> List[Dict[str, Any]]:
    tasks = [
        # 1. 结构分布 (仅保留车型 Series，因其对产品策略影响最大)
        {
            "id": "sales_dist_by_series",
            "tool": "distribution",
            "parameters": {
                "metric": "sales", 
                "dimension": "series_group", 
                "date_range": date_range,
                "compare_date_range": compare_date_range
            },
        },
        # 2. 销量趋势 (保留 30 天趋势以识别形态)
        {
            "id": "sales_trend_30d",
            "tool": "trend",
            "parameters": {"metric": "sales", "date_range": "last_30_days", "time_grain": "day"},
        },
        # 3. 核心转化率分布定位 (近 365 天分布，定位当前水位)
        {
            "id": "rate_dist_30d",
            "tool": "distribution",
            "parameters": {"metric": "assign_store_structure", "date_range": "yesterday", "compare_date_range": "last_365_days", "bins": 20, "return_buckets": False},
        },
        {
            "id": "rate_dist_store_share_30d",
            "tool": "distribution",
            "parameters": {"metric": "assign_store_leads_ratio", "date_range": "yesterday", "compare_date_range": "last_365_days", "bins": 20, "return_buckets": False},
        },
        {
            "id": "rate_dist_7d_lock_30d",
            "tool": "distribution",
            "parameters": {"metric": "assign_rate_7d_lock", "date_range": "yesterday", "compare_date_range": "last_365_days", "bins": 20, "return_buckets": False},
        },
        {
            "id": "rate_dist_7d_drive_30d",
            "tool": "distribution",
            "parameters": {"metric": "assign_rate_7d_test_drive", "date_range": "yesterday", "compare_date_range": "last_365_days", "bins": 20, "return_buckets": False},
        },
        # 4. 门店线索数环比 (用于归因总线索变化)
        {
            "id": "assign_trend_store_leads",
            "tool": "trend",
            "parameters": {"metric": "assign_store_leads", "time_grain": "day", "compare_type": "mom", "date_range": date_range},
        },
    ]
    return tasks


def _get_wow_tasks(date_range: str) -> List[Dict[str, Any]]:
    return [
        # 5. 门店线索数同比 (周同比)
        {
            "id": "assign_trend_store_leads_wow",
            "tool": "trend",
            "parameters": {"metric": "assign_store_leads", "time_grain": "day", "compare_type": "wow", "date_range": date_range},
        },
        # 6. 总线索数同比 (周同比)
        {
            "id": "assign_trend_leads_wow",
            "tool": "trend",
            "parameters": {"metric": "assign_leads", "time_grain": "day", "compare_type": "wow", "date_range": date_range},
        },
    ]


def _call_deepseek_reasoner(payload: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    api_key = None
    env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip() and not line.startswith("#"):
                    if "deepseek=" in line or "deepseek =" in line:
                        api_key = line.split("=", 1)[1].strip()
                        break
    if not api_key:
        return "⚠️ Error: DeepSeek API Key not found in .env", {}
    base_url = "https://api.deepseek.com"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    system_prompt = (
        "你是一位高密度业务诊断专家。请基于提供的 JSON 数据字典（包含 keys: 'core', 'sales_orders', 'leads_trend', 'rate_trend', 'signals'），输出一份**极简、去噪、高密度**的诊断报告。\n"
        "数据源映射说明：\n"
        "- **core**: 包含核心转化率指标 (assign_store_structure, 即**门店线索当日锁单率**) 及渠道结构指标 (assign_store_leads_ratio, 即**门店线索占比**) 的历史统计 (Z-score)。\n"
        "- **sales_orders**: \n"
        "    - **structure**: 包含结构分布 (series) 及 SAD 异动评分。\n"
        "    - **trend**: 包含销量趋势 (day_30) 及生命周期信号 (lifecycle)。**注意：销量日环比变化必须使用 `yesterday_change` 字段中的数据，严禁自行根据不完整的 `series` 列表末端计算，防止因数据截断导致误判。**\n"
        "- **leads_trend**: 包含线索量趋势。\n"
        "    - **total_leads**: 总线索数 (assign_leads) 的环比变化 (MoM/DoD)。\n"
        "    - **store_leads**: 门店线索数 (assign_store_leads) 的环比变化。\n"
        "    - **leads_wow**: 总线索数 (assign_leads) 的周同比变化 (WoW)。\n"
        "    - **store_leads_wow**: 门店线索数 (assign_store_leads) 的周同比变化 (WoW)。\n"
        "- **rate_trend**: 包含 3 组转化率及 1 组结构占比在近 365 天历史分布中的定位（Distribution Check）：\n"
        "    - **30d**: 门店线索当日锁单率 (assign_store_structure)\n"
        "    - **store_share_30d**: 门店线索占比 (assign_store_leads_ratio)\n"
        "    - **7d_lock_30d**: 7日锁单率 (assign_rate_7d_lock)\n"
        "    - **7d_drive_30d**: 7日试驾率 (assign_rate_7d_test_drive)\n"
        "- **signals**: 包含系统自动识别的异常信号。\n\n"
        "严格遵循以下格式和原则：\n"
        "1. **格式模板**：\n"
        "## 🟢/🔴 诊断结论：风险 [Low/High]\n"
        "**核心数据**：[基于 core 数据，指出转化率绝对值及偏离度，如“门店线索当日锁单率0.75% (Z-score -2.44)”；若门店线索占比有显著偏离也需指出，如“门店线索占比激增(Z=3.1)”]。\n"
        "**风险判定**：[一句话定性，如“门店线索当日锁单率显著低于历史均值，构成结构性异常”]。\n\n"
        "## 🔍 逐项排查 (Checklist)\n"
        "请按以下顺序逐项检查，**仅展示有问题（High Risk）的项**，若某项正常（如波动在合理范围内）则**直接省略**，保持报告极简。\n"
        "**1. 结构偏移 (Structure Check)**：[检查 sales_orders.structure。若 SAD > 0.1，指出具体的偏移因子。例：“车型结构偏移(SAD=0.34)，主因 LS9 占比回落(-14pct)被 CM2(+13pct)挤占。”]\n"
        "**2. 趋势断层 (Sales Trend Check)**：[检查 sales_orders.trend。观察 30 天趋势线，若呈现急剧下行或处于低位，指出具体形态。引用环比跌幅时务必使用 `yesterday_change` 字段。例：“LS9 销量处于上市退坡后的低位震荡期，日环比微跌 5%。”]\n"
        "**3. 转化率水位 (Rate Position Check)**：[检查 rate_trend 中的分布定位 (position)。若任一指标处于历史低位（如 P<10 或 低于历史均值-1σ），明确指出百分位 (Percentile)。特别注意门店线索占比 (store_share_30d) 的异常分布。例：“门店线索当日锁单率与7日试驾率均处于历史极低水位(P<5)，表明流量质量或承接能力出现系统性下滑。”]\n"
        "**4. 线索归因 (Leads Impact Check)**：[检查 leads_trend。若总线索 (total_leads) 或 门店线索 (store_leads) 任一发生显著波动（如跌幅 > 10%），则必须进行归因分析。检查总线索波动是否由门店线索导致，并对比 WoW 数据 (leads_wow, store_leads_wow) 确认是否为周期性波动。例：“总线索量环比下跌 4%，但门店线索大幅萎缩 (-26%) 且 WoW 同步下跌 20%，表明非周期性的渠道异常。”]\n\n"
        "## 💡 归因综述\n"
        "[基于上述检出的异常项，用一句话逻辑闭环解释核心转化率异常的原因。例：“LS9 上市退坡导致高转化客群流失，叠加长期转化率下行趋势，导致今日转化率击穿历史极值。”]\n\n"
        "2. **原则**：\n"
        "**有问题说，没问题不说**：不要罗列正常数据，只暴露风险。\n"
        "**量化优先**：禁止使用“大幅上升”等模糊词，必须使用“低-2.44σ”、“SAD 0.33”等精确数据。\n"
        "**逻辑闭环**：最后的归因综述必须基于 Checklist 中发现的问题。"
    )
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": json.dumps(payload, ensure_ascii=False, indent=2, default=str)},
    ]
    req = {"model": "deepseek-reasoner", "messages": messages, "stream": False}
    try:
        print("🤔 DeepSeek Reasoner is thinking...", end="", flush=True)
        t0 = time.time()
        import requests
        resp = requests.post(f"{base_url}/chat/completions", json=req, headers=headers)
        t1 = time.time()
        print(f" Done. ({t1 - t0:.2f}s)")
        if resp.status_code != 200:
            return f"Error from API: {resp.text}", {}
        data = resp.json()
        usage = data.get("usage", {})
        content = ""
        if "choices" in data and data["choices"]:
            content = data["choices"][0]["message"].get("content", "")
        return content, {"elapsed_sec": t1 - t0, "usage": usage}
    except Exception as e:
        return f"Error calling API: {str(e)}", {}


def analyze_point(target_date_str: str, args: argparse.Namespace) -> Dict[str, Any]:
    dm = DataManager()
    today = pd.Timestamp.now().normalize()
    if target_date_str == "yesterday":
        target_date = today - pd.Timedelta(days=1)
        date_range = "yesterday"
    else:
        target_date = pd.to_datetime(target_date_str, errors="raise").normalize()
        date_range = target_date.strftime("%Y-%m-%d")
    h_start = target_date - pd.Timedelta(days=int(args.history_start_days_ago))
    h_end = target_date - pd.Timedelta(days=int(args.history_end_days_ago))
    history_range_str = f"{h_start.strftime('%Y-%m-%d')}/{h_end.strftime('%Y-%m-%d')}"
    print(f"\n🔍 Analyzing Date: {date_range} (History Baseline: {history_range_str})")
    app = build_execution_graph()
    state = {
        "dsl_sequence": _build_dsl(date_range),
        "current_step": 0,
        "results": {},
        "signals": [],
    }
    final_state = app.invoke(state)
    stats = _compute_today_and_history(dm, target_date, h_start, h_end)
    structure_risk = assess_structure_risk(stats, z_high=float(args.z_threshold), z_mid=float(args.z_mid))
    conditional = conditional_rate_assessment(stats, window=float(args.share_window))
    lifecycle_finish_signals = []
    try:
        with open("/Users/zihao_/Documents/github/W52_reasoning/world/business_definition.json", "r", encoding="utf-8") as f:
            config = json.load(f)
        tps = config.get("time_periods", {})
        for series, period in tps.items():
            end_str = period.get("end")
            finish_str = period.get("finish")
            if not end_str or not finish_str:
                continue
            end_date = pd.to_datetime(end_str, errors="coerce")
            finish_date = pd.to_datetime(finish_str, errors="coerce")
            if pd.isna(end_date) or pd.isna(finish_date):
                continue
            if finish_date.normalize() + pd.Timedelta(days=31) <= target_date <= finish_date.normalize() + pd.Timedelta(days=60):
                days_since_finish = int((target_date - finish_date.normalize()).days)
                lifecycle_finish_signals.append({
                    "type": "lifecycle_finish_window",
                    "series": series,
                    "end": str(end_date.date()),
                    "finish": str(finish_date.date()),
                    "days_since_finish": days_since_finish,
                    "date_range": date_range,
                })
    except Exception:
        pass
    final_state["results"]["assign_structure"] = {
        "today": stats["today"],
        "history_window": {"start": str(h_start.date()), "end": str(h_end.date())},
        "structure_risk": structure_risk,
        "conditional": conditional,
    }
    final_state["signals"].append(
        {
            "type": "structure_anomaly",
            "metric": "assign_store_structure",
            "risk_level": structure_risk["risk_level"],
            "share_z": structure_risk["share_z"],
            "rate_z": structure_risk["rate_z"],
            "flag": structure_risk["flag"],
            "date_range": date_range,
        }
    )
    final_state["signals"].extend(lifecycle_finish_signals)
    if structure_risk["risk_level"] == "高":
        toolbox = _toolbox_for_high_risk(date_range, history_range_str)
        print("⚙️ 高风险触发：调度工具箱进行排查")
        state2 = {
            "dsl_sequence": toolbox,
            "current_step": 0,
            "results": {},
            "signals": [],
        }
        deep_state = app.invoke(state2)
        final_state["results"]["toolbox_analysis"] = deep_state["results"]

        # 检查是否需要触发 WoW 周期性排查
        # 触发条件：门店线索环比变化幅度 >= 10%
        store_leads_res = deep_state["results"].get("assign_trend_store_leads", {})
        change_pct = store_leads_res.get("change_pct", 0.0)
        
        if abs(change_pct) >= 0.1:
            print(f"⚠️ 检测到门店线索显著波动 ({change_pct:.1%})，追加 WoW 周期性排查...")
            wow_tasks = _get_wow_tasks(date_range)
            state3 = {
                "dsl_sequence": wow_tasks,
                "current_step": 0,
                "results": {},
                "signals": [],
            }
            wow_state = app.invoke(state3)
            # Merge results
            final_state["results"]["toolbox_analysis"].update(wow_state["results"])

    # Group results for DeepSeek
    sales_structure = {}
    sales_trend = {}
    rate_trend = {}
    leads_trend = {}
    
    # 0. Add initial DSL results to leads_trend
    if "assign_leads_mom" in final_state["results"]:
        leads_trend["total_leads"] = final_state["results"]["assign_leads_mom"]
    
    # 1. Add lifecycle check to sales_trend
    if "lifecycle_check" in final_state["results"]:
        sales_trend["lifecycle"] = final_state["results"]["lifecycle_check"]

    # 3. Split toolbox results if available
    if "toolbox_analysis" in final_state["results"]:
        for k, v in final_state["results"]["toolbox_analysis"].items():
            if k.startswith("sales_dist_"):
                clean_key = k.replace("sales_dist_by_", "")
                sales_structure[clean_key] = v
            elif k.startswith("sales_trend_"):
                clean_key = k.replace("sales_trend_", "")
                sales_trend[clean_key] = v
            elif k.startswith("rate_dist_"):
                clean_key = k.replace("rate_dist_", "")
                rate_trend[clean_key] = v
            elif k.startswith("assign_trend_"):
                clean_key = k.replace("assign_trend_", "")
                leads_trend[clean_key] = v
            else:
                pass

    payload = {
        "date": date_range,
        "core": final_state["results"].get("assign_structure", {}),
        "sales_orders": {
            "structure": sales_structure,
            "trend": sales_trend
        },
        "leads_trend": leads_trend,
        "rate_trend": rate_trend,
        "signals": final_state["signals"],
    }
    
    report, metrics = _call_deepseek_reasoner(payload)
    final_state["results"]["reasoner_report"] = report
    final_state["results"]["reasoner_metrics"] = metrics
    return final_state


def main() -> None:
    args = _parse_args()
    if args.start and args.end:
        s = pd.to_datetime(args.start)
        e = pd.to_datetime(args.end)
        dates = pd.date_range(start=s, end=e, freq="D")
        for d in dates:
            d_str = d.strftime("%Y-%m-%d")
            state = analyze_point(d_str, args)
            print("\n" + "=" * 50)
            print(f"📊 Assign Structure Reasoner Report ({d_str})")
            print("=" * 50)
            print(state["results"].get("reasoner_report", ""))
    elif args.date:
        state = analyze_point(args.date, args)
        print("\n" + "=" * 50)
        print(f"📊 Assign Structure Reasoner Report ({args.date})")
        print("=" * 50)
        print(state["results"].get("reasoner_report", ""))
        m = state["results"].get("reasoner_metrics", {})
        if m:
            usage = m.get("usage", {})
            print("\n------------------------------")
            print("⏱️  性能统计 (Performance Metrics)")
            print("------------------------------")
            print(f"⏳ 运行耗时: {float(m.get('elapsed_sec', 0)):.2f} 秒")
            print("🎫 Token 开销:")
            print(f"   - Input Tokens: {usage.get('prompt_tokens', 0)}")
            print(f"   - Output Tokens: {usage.get('completion_tokens', 0)}")
            print(f"   - Total Tokens: {usage.get('total_tokens', 0)}")
            print("------------------------------")
    else:
        print("Error: Please provide --date or --start and --end")


if __name__ == "__main__":
    main()
