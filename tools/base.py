# tools/base.py
class BaseTool:
    name: str

    def can_handle(self, step: dict) -> bool:
        raise NotImplementedError

    def execute(self, step: dict, state: dict):
        raise NotImplementedError
