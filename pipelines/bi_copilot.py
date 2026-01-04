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
