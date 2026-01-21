import sys
import os
import json
import pandas as pd

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agents.query_agent import QueryAgent

def display_result(result):
    print("\n--- Final Result ---")
    if not isinstance(result, dict):
        print(result)
        return

    metric = result.get("metric")
    
    # Case 1: Rollup Tool (has "rows")
    if "rows" in result and isinstance(result.get("rows"), list):
        rows = result.get("rows") or []
        dims = result.get("dimensions") or result.get("dimension")
        print(f"📊 Breakdown: {metric} by {dims} | Count: {len(rows)}")
        
        # Convert to DataFrame for nicer display if possible, else print JSON lines
        if rows:
            try:
                df = pd.DataFrame(rows)
                # Reorder columns: dimensions first, then value
                cols = [c for c in df.columns if c != "value"] + ["value"]
                print(df[cols].to_markdown(index=False))
            except ImportError:
                # Fallback if tabulate/markdown not available (though pandas usually handles string output)
                print(df[cols].to_string(index=False))
            except Exception:
                 for r in rows[:30]:
                    print(json.dumps(r, ensure_ascii=False))
        else:
            print("No data found.")

    # Case 2: Query Tool with Interval (Time Series)
    elif result.get("interval"):
        interval = result.get("interval")
        data = result.get("value")
        print(f"📈 Time Series ({interval}): {metric}")
        if isinstance(data, dict):
            # Sort by date key
            sorted_items = sorted(data.items())
            for date_str, val in sorted_items:
                print(f"  {date_str}: {val}")
        else:
            print(f"  {data}")

    # Case 3: Simple Scalar Query
    else:
        value = result.get("value")
        print(f"🔢 {metric}: {value}")

    # Metadata (Sample Size & Filters)
    sample_size = result.get("sample_size")
    filters = result.get("filters")
    
    if sample_size is not None:
        print(f"📉 Sample Size: {sample_size}")
    
    if filters:
        print(f"🔍 Filters: {json.dumps(filters, ensure_ascii=False)}")

    # Signals
    if result.get("signals"):
        print(f"\n⚠️ Signals: {json.dumps(result['signals'], ensure_ascii=False)}")
    print("--------------------\n")

def run_query_pipeline(query):
    print(f"\n🚀 Processing: '{query}'")
    
    try:
        agent = QueryAgent()
        result = agent.run(query)
        display_result(result)
    except Exception as e:
        print(f"❌ Error: {str(e)}")

def interactive_mode():
    print("🤖 Welcome to the Data Query Assistant!")
    print("Type your query below (or 'exit'/'quit' to stop).")
    print("Example: 'LS6 增程 2025年12月 的开票数'")
    
    while True:
        try:
            user_input = input("\nUser> ").strip()
            if user_input.lower() in ["exit", "quit", "q"]:
                print("Bye! 👋")
                break
            if not user_input:
                continue
                
            run_query_pipeline(user_input)
        except KeyboardInterrupt:
            print("\nBye! 👋")
            break
        except Exception as e:
            print(f"System Error: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        # Command line mode
        query = " ".join(sys.argv[1:])
        run_query_pipeline(query)
    else:
        # Interactive mode
        interactive_mode()
