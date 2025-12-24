# ⭐ State 定义（新增）
from typing import TypedDict, List, Dict, Any

class ExecutionState(TypedDict):
    dsl_sequence: List[Dict[str, Any]]
    current_step: int
    results: Dict[str, Any]
    signals: List[str]
