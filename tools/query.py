# tools/query.py
from tools.base import BaseTool


class QueryTool(BaseTool):
    name = "query"

    def can_handle(self, step: dict) -> bool:
        return step.get("tool") == "query"

    def execute(self, step: dict, state: dict):
        print(f"[QueryTool] executing: {step['id']}")

        # 👇 模拟一个异常信号
        signals = []
        if step["id"] == "second_query":
            signals.append("abnormal_change")

        return {
            "value": 123,
            "metric": step["parameters"].get("metric"),
            "signals": signals,
        }
