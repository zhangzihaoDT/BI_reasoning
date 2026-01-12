"""
此脚本用于演示/测试目的。
它跳过了 PlanningAgent 的自然语言理解阶段，直接执行一套预定义好的 DSL 序列。
场景：分析“昨日销量如何”或“一段时间内的销量轨迹”。
用途：调试 Execution Graph 或 演示标准分析流程。
"""
import sys
import os
import argparse
import pandas as pd
from typing import List, Dict, Any

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agents.execution_graph import build_execution_graph
from agents.suggestion_agent import SuggestionAgent


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--date", type=str, help="Single date to analyze (YYYY-MM-DD or 'yesterday')")
    p.add_argument("--start", type=str, help="Start date for range analysis (YYYY-MM-DD)")
    p.add_argument("--end", type=str, help="End date for range analysis (YYYY-MM-DD)")
    args = p.parse_args()
    
    # Default to yesterday if nothing provided
    if not args.date and not args.start:
        args.date = "yesterday"
        
    return args


def analyze_point(target_date_str: str) -> Dict[str, Any]:
    """
    Run analysis for a single point in time.
    Returns the final state containing results and signals.
    """
    today = pd.Timestamp.now().normalize()
    
    if target_date_str == "yesterday":
        target_date = today - pd.Timedelta(days=1)
        date_range = "yesterday"
        
        # Explicitly compute history range for consistent comparison
        # History: [target_date - 30, target_date - 1]
        hist_s = target_date - pd.Timedelta(days=30)
        hist_e = target_date - pd.Timedelta(days=1)
        
        history_range_str = f"{hist_s.strftime('%Y-%m-%d')}/{hist_e.strftime('%Y-%m-%d')}"
        
    else:
        target_date = pd.to_datetime(target_date_str, errors="raise").normalize()
        date_range = target_date.strftime("%Y-%m-%d")
        
        hist_s = target_date - pd.Timedelta(days=30)
        hist_e = target_date - pd.Timedelta(days=1)
        history_range_str = f"{hist_s.strftime('%Y-%m-%d')}/{hist_e.strftime('%Y-%m-%d')}"

    print(f"\n🔍 Analyzing Date: {date_range} (History Baseline: {history_range_str})")

    app = build_execution_graph()

    dsl_sequence = [
        {
            "id": "baseline_query",
            "tool": "query",
            "parameters": {"metric": "sales", "date_range": date_range},
        },
        {
            "id": "short_term_trend",
            "tool": "trend",
            "parameters": {
                "metric": "sales",
                "time_grain": "day",
                "compare_type": "mom",
                "date_range": date_range,
            },
        },
        {
            "id": "cycle_comparison",
            "tool": "trend",
            "parameters": {
                "metric": "sales",
                "time_grain": "day",
                "compare_type": "wow",
                "date_range": date_range,
            },
        },
        {
            "id": "anomaly_check",
            "tool": "trend",
            "parameters": {
                "metric": "sales",
                "time_grain": "day",
                "compare_type": "vs_avg",
                "date_range": history_range_str, 
            },
        },
        {
            "id": "structural_rollup",
            "tool": "rollup",
            "parameters": {
                "metric": "sales",
                "dimension": "series_group",
                "date_range": date_range,
            },
        },
        {
            "id": "composition_share",
            "tool": "composition",
            "parameters": {
                "metric": "sales",
                "dimension": "series_group",
                "date_range": date_range,
            },
        },
        {
            "id": "pareto_scan",
            "tool": "pareto",
            "parameters": {
                "metric": "sales",
                "dimension": "series_group",
                "date_range": date_range,
            },
        },
        {
            "id": "distribution_analysis",
            "tool": "histogram",
            "parameters": {
                "metric": "datediff('day',first_assign_time,lock_time)",
                "date_range": date_range,
                "compare_date_range": history_range_str,
                "bins": 30,
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
    return final_state


def generate_assessment(signals: List[Dict], date_str: str, verbose: bool = True):
    risk_score = 0
    reasons = []

    for signal in signals:
        if signal.get('type') == 'anomaly_decision':
            if signal.get('anomaly_detected'):
                risk_score += 2
                reasons.append(f"趋势异常: {signal.get('metric')} ({signal.get('flag')})")
        
        elif signal.get('type') == 'distribution_signal':
            if signal.get('status') == 'abnormal':
                risk_score += 2
                reasons.append(f"分布偏移: {signal.get('metric')} (差异评分: {signal.get('score'):.2f})")
            elif signal.get('status') == 'warning':
                risk_score += 1
                reasons.append(f"分布预警: {signal.get('metric')}")
        
        elif signal.get('type') == 'data_quality_signal':
             if signal.get('status') == 'warning':
                risk_score += 1
                reasons.append(f"数据质量: {signal.get('message')}")

    if risk_score == 0:
        level = "低"
        icon = "🟢"
    elif risk_score <= 2:
        level = "中"
        icon = "🟡"
    else:
        level = "高"
        icon = "🔴"

    if verbose:
        print(f"\n{icon} [{date_str}] 综合评估：风险等级：{level}")
        if reasons:
            print("   风险因子：")
            for r in reasons:
                print(f"   - {r}")
                
        if level in ["中", "高"]:
            print("\n🤖 分析建议 (Suggestion Agent):")
            agent = SuggestionAgent()
            suggestions = agent.generate_suggestions(
                risk_level=level,
                risk_factors=reasons,
                analysis_results=signals 
            )
            print(suggestions)
            
    return {
        "date": date_str,
        "risk_level": level,
        "risk_score": risk_score,
        "reasons": reasons,
        "icon": icon
    }


def analyze_range(start_date: str, end_date: str):
    print(f"🚀 Starting Trajectory Analysis: {start_date} to {end_date}")
    
    s = pd.to_datetime(start_date)
    e = pd.to_datetime(end_date)
    
    dates = pd.date_range(start=s, end=e, freq='D')
    
    trajectory = []
    
    for d in dates:
        d_str = d.strftime("%Y-%m-%d")
        state = analyze_point(d_str)
        
        # Print concise result for each day
        print(f"Processing {d_str}...", end="\r")
        assessment = generate_assessment(state["signals"], d_str, verbose=False)
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
        analyze_range(args.start, args.end)
    elif args.date:
        state = analyze_point(args.date)
        print("\nFinal results:")
        print(state["results"])
        print("\nSignals:")
        print(state["signals"])
        generate_assessment(state["signals"], args.date, verbose=True)
    else:
        print("Error: Please provide --date or --start and --end")

if __name__ == "__main__":
    main()
