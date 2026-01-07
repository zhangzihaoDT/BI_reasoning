"""
此脚本用于演示/测试目的。
它跳过了 PlanningAgent 的自然语言理解阶段，直接执行一套预定义好的 DSL 序列。
场景：分析“昨日销量如何”。
用途：调试 Execution Graph 或 演示标准分析流程。
"""
import sys
import os
# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agents.execution_graph import build_execution_graph
from agents.suggestion_agent import SuggestionAgent

app = build_execution_graph()

dsl_sequence = [
    {
        "id": "baseline_query",
        "tool": "query",
        "parameters": {"metric": "sales", "date_range": "yesterday"},
    },
    {
        "id": "short_term_trend",
        "tool": "trend",
        "parameters": {
            "metric": "sales",
            "time_grain": "day",
            "compare_type": "mom",
            "date_range": "yesterday",
        },
    },
    {
        "id": "cycle_comparison",
        "tool": "trend",
        "parameters": {
            "metric": "sales",
            "time_grain": "day",
            "compare_type": "wow",
            "date_range": "yesterday",
        },
    },
    {
        "id": "anomaly_check",
        "tool": "trend",
        "parameters": {
            "metric": "sales",
            "time_grain": "day",
            "compare_type": "vs_avg",
            "date_range": "last_30_days",
        },
    },
    {
        "id": "structural_rollup",
        "tool": "rollup",
        "parameters": {
            "metric": "sales",
            "dimension": "series_group",
            "date_range": "yesterday",
        },
    },
    {
        "id": "composition_share",
        "tool": "composition",
        "parameters": {
            "metric": "sales",
            "dimension": "series_group",
            "date_range": "yesterday",
        },
    },
    {
        "id": "pareto_scan",
        "tool": "pareto",
        "parameters": {
            "metric": "sales",
            "dimension": "series_group",
            "date_range": "yesterday",
        },
    },
    {
        "id": "distribution_analysis",
        "tool": "histogram",
        "parameters": {
            "metric": "datediff('day',first_assign_time,lock_time)",
            "date_range": "yesterday",
            "compare_date_range": "last_30_days",
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

print("\nFinal results:")
print(final_state["results"])
print("\nSignals:")
print(final_state["signals"])

def generate_assessment(signals):
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

    print(f"\n{icon} 综合评估：风险等级：{level}")
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

generate_assessment(final_state["signals"])
