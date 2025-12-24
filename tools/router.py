# tools/router.py
class ToolRouter:
    def __init__(self, tools):
        self.tools = tools

    def execute(self, step: dict, state: dict):
        for tool in self.tools:
            if tool.can_handle(step):
                return tool.execute(step, state)

        raise ValueError(f"No tool found for step: {step['tool']}")
