# ⭐ LangGraph 定义（新增）
# agents/execution_graph.py
from langgraph.graph import StateGraph, END

from agents.execution_state import ExecutionState
from tools.router import ToolRouter
from tools.query import QueryTool
from tools.trend import TrendTool
from tools.rollup import RollupTool
from tools.decompose import AdditiveTool, RatioTool, CompositionTool, ParetoTool, DualAxisTool
from runtime.signals import evaluate_breadth_scan_and_plan


# 1️⃣ 注册工具
tool_router = ToolRouter(
    tools=[
        QueryTool(),
        TrendTool(),
        RollupTool(),
        CompositionTool(),
        ParetoTool(),
        AdditiveTool(),
        RatioTool(),
        DualAxisTool(),
    ]
)


# 2️⃣ LangGraph Node：执行一个 DSL step
def execute_step(state: ExecutionState) -> ExecutionState:
    step = state["dsl_sequence"][state["current_step"]]

    print(f"\n==> Running step {state['current_step']} : {step['id']}")

    result = tool_router.execute(step, state)

    state["results"][step["id"]] = result

    for s in result.get("signals", []):
        state["signals"].append(s)

    if step["id"] == "anomaly_check":
        plan = evaluate_breadth_scan_and_plan(
            results=state["results"],
            metric=step["parameters"].get("metric", "sales"),
            date_range=step["parameters"].get("date_range", "yesterday"),
            dimensions=[
                "store_name",
                "store_city",
                "parent_region_name",
                "first_middle_channel_name",
                "series_group",
            ],
            core_metrics=["lock_rate", "delivery_rate"],
        )
        decision = plan["decision"]
        state["signals"].append(
            {
                "type": "anomaly_decision",
                "flag": decision["flag"],
                "z": decision["z"],
                "cv": decision["cv"],
                "anomaly_detected": decision["anomaly_detected"],
                "metric": step["parameters"].get("metric", "sales"),
                "date_range": step["parameters"].get("date_range", "yesterday"),
                "dimensions": [
                    "store_name",
                    "store_city",
                    "parent_region_name",
                    "first_middle_channel_name",
                    "series_group",
                ],
                "core_metrics": ["lock_rate", "delivery_rate"],
            }
        )
        if decision["anomaly_detected"]:
            existing_ids = [s["id"] for s in state["dsl_sequence"]]
            for s in plan["next_steps"]:
                if s["id"] not in existing_ids:
                    state["dsl_sequence"].append(s)

    state["current_step"] += 1
    return state


# 3️⃣ 判断是否继续
# 新增一个判断函数（同文件）
def next_step(state: ExecutionState):
    if state["current_step"] < len(state["dsl_sequence"]):
        return "continue"
    return "end"


# 4️⃣ 构建 Graph
def build_execution_graph():
    graph = StateGraph(ExecutionState)

    graph.add_node("execute_step", execute_step)
    graph.set_entry_point("execute_step")

    graph.add_conditional_edges(
        "execute_step",
        next_step,
        {
            "continue": "execute_step",
            "end": END,
        },
    )

    return graph.compile()
