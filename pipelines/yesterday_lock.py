"""
此脚本用于演示/测试目的。
它跳过了 PlanningAgent 的自然语言理解阶段，直接执行一套预定义好的 DSL 序列。
场景：分析“昨日销量如何”。
用途：调试 Execution Graph 或 演示标准分析流程。
"""
from agents.execution_graph import build_execution_graph

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
