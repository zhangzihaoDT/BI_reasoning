import sys
import os
import json

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agents.query_agent import QueryAgent

def run_query_pipeline(query):
    print(f"\n🚀 Starting Query Pipeline for: '{query}'\n")
    
    agent = QueryAgent()
    result = agent.run(query)
    
    print("\n--- Final Result ---")
    if isinstance(result, dict):
        metric = result.get("metric")
        if "rows" in result and isinstance(result.get("rows"), list):
            rows = result.get("rows") or []
            dims = result.get("dimensions") or result.get("dimension")
            print(f"🔢 {metric} | {dims} | rows={len(rows)}")
            for r in rows[:30]:
                print(json.dumps(r, ensure_ascii=False))
        else:
            value = result.get("value")
            print(f"🔢 {metric}: {value}")
        if result.get("signals"):
            print(f"⚠️ Signals: {json.dumps(result['signals'], ensure_ascii=False)}")
    else:
        print(result)

if __name__ == "__main__":
    query = sys.argv[1] if len(sys.argv) > 1 else "昨日锁单数"
    run_query_pipeline(query)
