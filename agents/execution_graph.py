# ⭐ LangGraph 定义（新增）
# agents/execution_graph.py
from langgraph.graph import StateGraph, END

from agents.execution_state import ExecutionState
from tools.router import ToolRouter
from tools.query import QueryTool


# 1️⃣ 注册工具
tool_router = ToolRouter(
    tools=[
        QueryTool(),
        # TrendTool(), RollupTool() 以后加
    ]
)


# 2️⃣ LangGraph Node：执行一个 DSL step
def execute_step(state: ExecutionState) -> ExecutionState:
    step = state["dsl_sequence"][state["current_step"]]

    print(f"\n==> Running step {state['current_step']} : {step['id']}")

    result = tool_router.execute(step, state)

    state["results"][step["id"]] = result

    # ⭐ 收集 signals
    for s in result.get("signals", []):
        state["signals"].append(s)

    state["current_step"] += 1
    return state


# 3️⃣ 判断是否继续
# 新增一个判断函数（同文件）
def next_step(state: ExecutionState):
    # 👇 如果检测到异常，而且还没做 drilldown
    if "abnormal_change" in state["signals"]:
        existing_ids = [s["id"] for s in state["dsl_sequence"]]
        if "drilldown_query" not in existing_ids:
            print("⚠️  anomaly detected → injecting drilldown step")

            state["dsl_sequence"].append(
                {
                    "id": "drilldown_query",
                    "tool": "query",
                    "parameters": {"metric": "sales_by_channel"},
                }
            )

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
