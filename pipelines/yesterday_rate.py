"""
此脚本用于演示/测试目的。
它跳过了 PlanningAgent 的自然语言理解阶段，直接执行一套预定义好的 DSL 序列。
场景：分析“昨日转化率如何”或“一段时间内的转化率轨迹”。
用途：调试 Execution Graph 或 演示标准分析流程。
"""

import os
import sys
import argparse
from typing import Dict, Any, List

import numpy as np
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agents.execution_graph import build_execution_graph
from runtime.context import DataManager


def _safe_rate(n: float, d: float) -> float:
    return float(n / d) if d and d > 0 else 0.0


def _percent_rank(values: np.ndarray, x: float) -> float:
    n = int(values.size)
    if n == 0:
        return 0.0
    return float((values <= x).mean())


def _level_from_percentile(p: float) -> str:
    if p <= 1 / 3:
        return "低"
    if p <= 2 / 3:
        return "中"
    return "高"


def _risk_level_from_flags(
    flag: str,
) -> str:
    if flag == "结构性异常":
        return "高"
    if flag in {"高波动异常", "趋势性偏离"}:
        return "中"
    return "低"


def compute_volume_stats(
    dm: DataManager,
    col: str,
    target_date: pd.Timestamp,
    history_start: pd.Timestamp,
    history_end: pd.Timestamp,
) -> dict:
    df = dm.get_assign_data()
    if df.empty or "assign_date" not in df.columns:
        return {
            "value": 0.0,
            "percentile": 0.0,
            "position": "低",
            "n_days": 0,
            "below_hist_min": False,
            "above_hist_max": False,
        }

    df = df.copy()
    df["assign_date"] = pd.to_datetime(df["assign_date"], errors="coerce")
    df = df[df["assign_date"].notna()]

    # Target value
    d_target = df[df["assign_date"].dt.normalize() == target_date.normalize()]
    value = float(d_target[col].sum()) if col in d_target.columns else 0.0

    # History values
    df_hist = df[(df["assign_date"] >= history_start) & (df["assign_date"] <= history_end)]
    if df_hist.empty or col not in df_hist.columns:
        hist_values = np.array([], dtype=float)
    else:
        daily = df_hist.groupby(df_hist["assign_date"].dt.normalize())[col].sum()
        hist_values = daily.fillna(0.0).astype(float).to_numpy()

    percentile = _percent_rank(hist_values, value)
    n_days = int(hist_values.size)
    hist_min = float(np.min(hist_values)) if n_days > 0 else 0.0
    hist_max = float(np.max(hist_values)) if n_days > 0 else 0.0
    below_hist_min = bool(n_days > 0 and value < hist_min)
    above_hist_max = bool(n_days > 0 and value > hist_max)
    position = _level_from_percentile(percentile)

    return {
        "value": value,
        "percentile": percentile,
        "n_days": n_days,
        "below_hist_min": below_hist_min,
        "above_hist_max": above_hist_max,
        "position": position,
    }


def compute_rate_stats(
    dm: DataManager,
    numerator_col: str,
    denominator_col: str,
    target_date: pd.Timestamp,
    history_start: pd.Timestamp,
    history_end: pd.Timestamp,
    n_min: float,
    z_high: float,
    z_mid: float,
    cv_low: float,
) -> dict:
    df = dm.get_assign_data()
    if df.empty or "assign_date" not in df.columns:
        return {
            "value": 0.0,
            "leads": 0.0,
            "mean": 0.0,
            "std": 0.0,
            "z": 0.0,
            "cv": 0.0,
            "percentile": 0.0,
            "position": "低",
            "anomaly_detected": False,
            "flag": "无数据",
            "history_window": {"start": str(history_start.date()), "end": str(history_end.date())},
        }

    df = df.copy()
    df["assign_date"] = pd.to_datetime(df["assign_date"], errors="coerce")
    df = df[df["assign_date"].notna()]

    d_target = df[df["assign_date"].dt.normalize() == target_date.normalize()]
    leads = float(d_target[denominator_col].sum()) if denominator_col in d_target.columns else 0.0
    num = float(d_target[numerator_col].sum()) if numerator_col in d_target.columns else 0.0
    value = _safe_rate(num, leads)

    df_hist = df[(df["assign_date"] >= history_start) & (df["assign_date"] <= history_end)]
    if (
        df_hist.empty
        or numerator_col not in df_hist.columns
        or denominator_col not in df_hist.columns
        or "assign_date" not in df_hist.columns
    ):
        hist_values = np.array([], dtype=float)
    else:
        daily = (
            df_hist.groupby(df_hist["assign_date"].dt.normalize())[[numerator_col, denominator_col]]
            .sum()
            .reset_index()
        )
        denom = daily[denominator_col].replace(0, np.nan)
        hist_values = (daily[numerator_col] / denom).fillna(0.0).astype(float).to_numpy()

    mean = float(np.mean(hist_values)) if hist_values.size > 0 else 0.0
    std = float(np.std(hist_values, ddof=1)) if hist_values.size > 1 else 0.0

    if std > 0:
        z = float((value - mean) / std)
    else:
        z = 0.0

    if mean != 0:
        cv = float(abs(std / mean))
    else:
        cv = float("inf") if std > 0 else 0.0

    percentile = _percent_rank(hist_values, value)
    n_days = int(hist_values.size)
    hist_min = float(np.min(hist_values)) if n_days > 0 else 0.0
    hist_max = float(np.max(hist_values)) if n_days > 0 else 0.0
    below_hist_min = bool(n_days > 0 and value < hist_min)
    above_hist_max = bool(n_days > 0 and value > hist_max)
    percentile_resolution = float(1.0 / n_days) if n_days > 0 else 0.0
    position = _level_from_percentile(percentile)

    anomaly_detected = False
    flag = "正常波动"
    if leads < n_min:
        flag = "样本不足"
    else:
        abs_z = abs(z)
        if abs_z >= z_high and cv < cv_low:
            anomaly_detected = True
            flag = "结构性异常"
        elif abs_z >= z_high and cv >= cv_low:
            anomaly_detected = True
            flag = "高波动异常"
        elif abs_z >= z_mid:
            anomaly_detected = True
            flag = "趋势性偏离"

    return {
        "value": value,
        "leads": leads,
        "mean": mean,
        "std": std,
        "z": z,
        "cv": cv,
        "percentile_method": "empirical_cdf",
        "percentile": percentile,
        "n_days": n_days,
        "hist_min": hist_min,
        "hist_max": hist_max,
        "below_hist_min": below_hist_min,
        "above_hist_max": above_hist_max,
        "percentile_resolution": percentile_resolution,
        "position": position,
        "anomaly_detected": anomaly_detected,
        "flag": flag,
        "thresholds": {"n_min": n_min, "z_high": z_high, "z_mid": z_mid, "cv_low": cv_low},
        "history_window": {"start": str(history_start.date()), "end": str(history_end.date())},
    }


def _format_percentile(stats: dict) -> str:
    p = float(stats.get("percentile", 0.0))
    n = int(stats.get("n_days", 0))
    below = bool(stats.get("below_hist_min", False))
    above = bool(stats.get("above_hist_max", False))
    if n <= 0:
        return "P0.0"
    if below:
        return f"P<{(1.0 / n) * 100:.1f}（低于历史最小值）"
    if above:
        return f"P>{(1.0 - 1.0 / n) * 100:.1f}（高于历史最大值）"
    return f"P{p * 100:.1f}"

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--date", type=str, help="Single date to analyze (YYYY-MM-DD or 'yesterday')")
    p.add_argument("--start", type=str, help="Start date for range analysis (YYYY-MM-DD)")
    p.add_argument("--end", type=str, help="End date for range analysis (YYYY-MM-DD)")
    
    p.add_argument("--history-start-days-ago", type=int, default=60)
    p.add_argument("--history-end-days-ago", type=int, default=30)
    p.add_argument("--n-min", type=float, default=50.0)
    p.add_argument("--z-threshold", type=float, default=2.0)
    p.add_argument("--z-mid", type=float, default=1.2)
    p.add_argument("--cv-threshold", type=float, default=0.4)
    
    args = p.parse_args()
    
    if not args.date and not args.start:
        args.date = "yesterday"
        
    return args


def analyze_point(target_date_str: str, args: argparse.Namespace) -> Dict[str, Any]:
    dm = DataManager()
    today = pd.Timestamp.now().normalize()
    
    if target_date_str == "yesterday":
        target_date = today - pd.Timedelta(days=1)
        date_range = "yesterday"
    else:
        target_date = pd.to_datetime(target_date_str, errors="raise").normalize()
        date_range = target_date.strftime("%Y-%m-%d")

    history_start = target_date - pd.Timedelta(days=int(args.history_start_days_ago))
    history_end = target_date - pd.Timedelta(days=int(args.history_end_days_ago))
    
    history_range_str = f"{history_start.strftime('%Y-%m-%d')}/{history_end.strftime('%Y-%m-%d')}"

    print(f"\n🔍 Analyzing Date: {date_range} (History Baseline: {history_range_str})")

    N_min = float(args.n_min)
    z_high = float(args.z_threshold)
    z_mid = float(args.z_mid)
    cv_low = float(args.cv_threshold)

    app = build_execution_graph()
    dsl_sequence = [
        {
            "id": "assign_leads_mom",
            "tool": "trend",
            "parameters": {
                "metric": "assign_leads",
                "time_grain": "day",
                "compare_type": "mom",
                "date_range": date_range,
            },
        },
        {
            "id": "assign_rate_7d_conversion",
            "tool": "trend",
            "parameters": {
                "metric": "assign_rate_7d_lock",
                "time_grain": "day",
                "compare_type": "mom",
                "date_range": date_range,
            },
        },
        {
            "id": "assign_rate_7d_test_drive",
            "tool": "trend",
            "parameters": {
                "metric": "assign_rate_7d_test_drive",
                "time_grain": "day",
                "compare_type": "mom",
                "date_range": date_range,
            },
        },
    ]

    initial_state = {
        "dsl_sequence": dsl_sequence,
        "current_step": 0,
        "results": {},
        "signals": [],
    }

    final_state = app.invoke(initial_state)

    conversion_stats = compute_rate_stats(
        dm=dm,
        numerator_col="下发线索 7 日锁单数",
        denominator_col="下发线索数",
        target_date=target_date,
        history_start=history_start,
        history_end=history_end,
        n_min=N_min,
        z_high=z_high,
        z_mid=z_mid,
        cv_low=cv_low,
    )
    test_drive_stats = compute_rate_stats(
        dm=dm,
        numerator_col="下发线索 7 日试驾数",
        denominator_col="下发线索数",
        target_date=target_date,
        history_start=history_start,
        history_end=history_end,
        n_min=N_min,
        z_high=z_high,
        z_mid=z_mid,
        cv_low=cv_low,
    )

    leads_stats = compute_volume_stats(
        dm=dm,
        col="下发线索数",
        target_date=target_date,
        history_start=history_start,
        history_end=history_end,
    )

    final_state["results"]["rate_stats"] = {
        "history_window_days_ago": {
            "start_days_ago": int(args.history_start_days_ago),
            "end_days_ago": int(args.history_end_days_ago),
        },
        "params": {"N_min": N_min, "z_high": z_high, "z_mid": z_mid, "cv_low": cv_low},
        "leads_stats": leads_stats,
        "7d_conversion_rate": conversion_stats,
        "7d_test_drive_rate": test_drive_stats,
    }

    final_state["signals"].append(
        {
            "type": "volume_signal",
            "metric": "assign_leads",
            "position": leads_stats["position"],
            "percentile": leads_stats["percentile"],
            "date_range": date_range,
        }
    )

    final_state["signals"].append(
        {
            "type": "anomaly_decision",
            "metric": "7d_conversion_rate",
            "flag": conversion_stats["flag"],
            "z": conversion_stats["z"],
            "cv": conversion_stats["cv"],
            "anomaly_detected": conversion_stats["anomaly_detected"],
            "date_range": date_range,
            "history_window": conversion_stats["history_window"],
            "leads": conversion_stats["leads"],
            "position": conversion_stats["position"],
            "percentile": conversion_stats["percentile"],
        }
    )
    final_state["signals"].append(
        {
            "type": "anomaly_decision",
            "metric": "7d_test_drive_rate",
            "flag": test_drive_stats["flag"],
            "z": test_drive_stats["z"],
            "cv": test_drive_stats["cv"],
            "anomaly_detected": test_drive_stats["anomaly_detected"],
            "date_range": date_range,
            "history_window": test_drive_stats["history_window"],
            "leads": test_drive_stats["leads"],
            "position": test_drive_stats["position"],
            "percentile": test_drive_stats["percentile"],
        }
    )
    
    return final_state


def generate_assessment(state: Dict[str, Any], date_str: str, verbose: bool = True):
    results = state["results"]
    rate_stats = results.get("rate_stats", {})
    conversion_stats = rate_stats.get("7d_conversion_rate", {})
    test_drive_stats = rate_stats.get("7d_test_drive_rate", {})
    leads_stats = rate_stats.get("leads_stats", {})

    overall_risk = max(
        [
            _risk_level_from_flags(
                flag=conversion_stats.get("flag", "无数据"),
            ),
            _risk_level_from_flags(
                flag=test_drive_stats.get("flag", "无数据"),
            ),
        ],
        key=lambda x: {"低": 0, "中": 1, "高": 2}.get(x, 0),
    )

    icon = {"低": "🟢", "中": "🟡", "高": "🔴"}.get(overall_risk, "❓")
    
    reasons = []
    if conversion_stats.get("anomaly_detected"):
        reasons.append(f"转化率异常 (7日锁单): {conversion_stats.get('flag')} (Z={conversion_stats.get('z',0):.2f})")
    if test_drive_stats.get("anomaly_detected"):
        reasons.append(f"试驾率异常 (7日试驾): {test_drive_stats.get('flag')} (Z={test_drive_stats.get('z',0):.2f})")
    if leads_stats.get("position") == "高" and leads_stats.get("percentile", 0) > 0.9:
        reasons.append(f"线索量激增 ({_format_percentile(leads_stats)})")
    elif leads_stats.get("position") == "低" and leads_stats.get("percentile", 0) < 0.1:
        reasons.append(f"线索量过低 ({_format_percentile(leads_stats)})")

    if verbose:
        print(f"\n{icon} [{date_str}] 转化率综合评估：风险等级：{overall_risk}")
        print(
            "📍 当前下发线索在历史分布位置："
            f"{leads_stats.get('position', 'N/A')} ({_format_percentile(leads_stats)})"
        )
        print(
            "📍 当前7日转化率在历史分布位置："
            f"{conversion_stats.get('position', 'N/A')} ({_format_percentile(conversion_stats)})"
        )
        print(
            "📍 当前7日试驾率在历史分布位置："
            f"{test_drive_stats.get('position', 'N/A')} ({_format_percentile(test_drive_stats)})"
        )
        
        if reasons:
            print("   风险因子：")
            for r in reasons:
                print(f"   - {r}")

    return {
        "date": date_str,
        "risk_level": overall_risk,
        "reasons": reasons,
        "icon": icon
    }


def analyze_range(start_date: str, end_date: str, args: argparse.Namespace):
    print(f"🚀 Starting Trajectory Analysis: {start_date} to {end_date}")
    
    s = pd.to_datetime(start_date)
    e = pd.to_datetime(end_date)
    
    dates = pd.date_range(start=s, end=e, freq='D')
    
    trajectory = []
    
    for d in dates:
        d_str = d.strftime("%Y-%m-%d")
        state = analyze_point(d_str, args)
        
        # Print concise result for each day
        assessment = generate_assessment(state, d_str, verbose=True)
        trajectory.append(assessment)
        
    # Summary of trajectory
    print("\n" + "="*50)
    print(f"📅 区间轨迹汇总 ({start_date} ~ {end_date})")
    print("="*50)
    
    high_risk_days = [t for t in trajectory if t['risk_level'] == '高']
    med_risk_days = [t for t in trajectory if t['risk_level'] == '中']
    
    print(f"共分析 {len(trajectory)} 天")
    print(f"🔴 高风险天数: {len(high_risk_days)}")
    print(f"🟡 中风险天数: {len(med_risk_days)}")
    
    if high_risk_days:
        print("\n⚠️ 高风险日期详情:")
        for t in high_risk_days:
            print(f"  - {t['date']}: {', '.join(t['reasons'])}")
            
    if not high_risk_days and not med_risk_days:
        print("\n✅ 区间内表现平稳，无显著异常。")


def main() -> None:
    args = _parse_args()
    
    if args.start and args.end:
        analyze_range(args.start, args.end, args)
    elif args.date:
        state = analyze_point(args.date, args)
        print("\nFinal results:")
        print(state["results"])
        print("\nSignals:")
        print(state["signals"])
        generate_assessment(state, args.date, verbose=True)
    else:
        print("Error: Please provide --date or --start and --end")


if __name__ == "__main__":
    main()
