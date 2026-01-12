import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agents.query_agent_reasoner import QueryAgentReasoner

from typing import Dict, Any

def print_metrics(metrics: Dict[str, Any]):
    if not metrics:
        return
        
    usage = metrics.get("usage", {})
    elapsed = metrics.get("elapsed_sec", 0)
    
    print("\n" + "-"*30)
    print("⏱️  性能统计 (Performance Metrics)")
    print("-"*30)
    print(f"⏳ 运行耗时: {elapsed:.2f} 秒")
    print(f"🎫 Token 开销:")
    print(f"   - Input Tokens: {usage.get('prompt_tokens', 0)}")
    print(f"   - Output Tokens: {usage.get('completion_tokens', 0)}")
    print(f"   - Total Tokens: {usage.get('total_tokens', 0)}")
    print("-"*30 + "\n")

def run_query_pipeline(query):
    print(f"\n🚀 Starting Reasoner Query Pipeline for: '{query}'\n")
    agent = QueryAgentReasoner()
    result, metrics = agent.run(query)
    print("\n--- Final Result ---")
    print(result)
    print_metrics(metrics)

if __name__ == "__main__":
    query = sys.argv[1] if len(sys.argv) > 1 else "昨日锁单数"
    run_query_pipeline(query)
