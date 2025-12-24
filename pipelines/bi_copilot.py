from agents.execution_graph import build_execution_graph

app = build_execution_graph()

dsl_sequence = [
    {
        "id": "baseline_query",
        "tool": "query",
        "parameters": {"metric": "sales"},
    },
    {
        "id": "second_query",
        "tool": "query",
        "parameters": {"metric": "sales"},
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
