import sys
import json
import re
import os

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agents.planning_agent import PlanningAgent
from agents.execution_graph import build_execution_graph

def parse_json_from_markdown(text):
    """
    Extracts JSON from a string that might be wrapped in markdown code blocks.
    """
    try:
        # Try parsing directly
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Try finding code blocks
    match = re.search(r'```json\s*(.*?)\s*```', text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(1))
        except json.JSONDecodeError:
            pass
            
    match = re.search(r'```\s*(.*?)\s*```', text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(1))
        except json.JSONDecodeError:
            pass

    print(f"Failed to parse JSON from: {text}")
    return None

def transform_plan_to_dsl(plan_json):
    """
    Transforms the PlanningAgent output format to the ExecutionGraph input format.
    
    PlanningAgent Output Item:
    {
        "step_id": 1,
        "action_name": "Baseline Query",
        "tool_name": "query",
        "parameters": {...},
        "reasoning": "...",
        "output_key": "baseline_query"
    }
    
    ExecutionGraph Input Item:
    {
        "id": "baseline_query",
        "tool": "query",
        "parameters": {...}
    }
    """
    dsl_sequence = []
    for step in plan_json:
        dsl_step = {
            "id": step.get("output_key", f"step_{step.get('step_id')}"),
            "tool": step.get("tool_name"),
            "parameters": step.get("parameters", {})
        }
        dsl_sequence.append(dsl_step)
    return dsl_sequence

import dataclasses

class EnhancedJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if dataclasses.is_dataclass(o):
            return dataclasses.asdict(o)
        return super().default(o)

def run_pipeline(query):
    print(f"\n🚀 Starting Full Pipeline for Query: '{query}'\n")
    
    # 1. Plan
    print("--- Phase 1: Planning (Agent) ---")
    agent = PlanningAgent()
    plan_text = agent.generate_plan(query)
    # print("Plan Generated (Raw):")
    # print(plan_text) 
    
    plan_json = parse_json_from_markdown(plan_text)
    if not plan_json:
        print("❌ Error: Could not parse plan into JSON.")
        print("Raw Output:")
        print(plan_text)
        return

    print(f"✅ Plan parsed successfully. {len(plan_json)} steps generated.")
    
    # 2. Transform
    dsl_sequence = transform_plan_to_dsl(plan_json)
    print("Transformed DSL Sequence:")
    print(json.dumps(dsl_sequence, indent=2, ensure_ascii=False))
    
    # 3. Execute
    print("\n--- Phase 2: Execution (Graph) ---")
    app = build_execution_graph()
    
    initial_state = {
        "dsl_sequence": dsl_sequence,
        "current_step": 0,
        "results": {},
        "signals": [],
    }
    
    try:
        final_state = app.invoke(initial_state)
        
        # 4. Results
        print("\n--- Phase 3: Results ---")
        print("\nFinal Results:")
        print(json.dumps(final_state["results"], indent=2, ensure_ascii=False, cls=EnhancedJSONEncoder))
        
        print("\nSignals:")
        print(json.dumps(final_state["signals"], indent=2, ensure_ascii=False, cls=EnhancedJSONEncoder))
        
    except Exception as e:
        print(f"❌ Execution Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    query = sys.argv[1] if len(sys.argv) > 1 else "昨日销量如何"
    run_pipeline(query)
