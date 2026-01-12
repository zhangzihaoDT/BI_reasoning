"""
此脚本是 yesterday_lock.py 的 Reasoner 增强版。
它保留了原有的 DSL 执行图逻辑，但在最后引入 DeepSeek Thinking Mode (deepseek-reasoner)
对分析结果（基线、趋势、结构、异常信号）进行深度解读，生成自然的业务日报。
"""
import sys
import os
import argparse
import json
import requests
import pandas as pd
import time
from typing import List, Dict, Any, Tuple

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agents.execution_graph import build_execution_graph

def _load_api_key():
    env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
    if os.path.exists(env_path):
        with open(env_path, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip() and not line.startswith('#'):
                    if 'deepseek=' in line:
                        return line.split('=', 1)[1].strip()
                    if 'deepseek =' in line:
                            return line.split('=', 1)[1].strip()
    return None

API_KEY = _load_api_key()

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--date", type=str, help="Single date to analyze (YYYY-MM-DD or 'yesterday')")
    p.add_argument("--start", type=str, help="Start date for range analysis (YYYY-MM-DD)")
    p.add_argument("--end", type=str, help="End date for range analysis (YYYY-MM-DD)")
    args = p.parse_args()
    
    if not args.date and not args.start:
        args.date = "yesterday"
        
    return args

def calculate_risk(signals: List[Dict]) -> Dict[str, Any]:
    """
    根据信号计算风险等级（复用 yesterday_lock.py 的逻辑）
    """
    risk_score = 0
    reasons = []

    for signal in signals:
        if signal.get('type') == 'anomaly_decision':
            if signal.get('anomaly_detected'):
                risk_score += 2
                reasons.append(f"趋势异常: {signal.get('metric')} ({signal.get('flag')})")
        
        elif signal.get('type') == 'distribution_signal':
            if signal.get('status') == 'abnormal':
                risk_score += 2
                reasons.append(f"分布偏移: {signal.get('metric')} (差异评分: {signal.get('score'):.2f})")
            elif signal.get('status') == 'warning':
                risk_score += 1
                reasons.append(f"分布预警: {signal.get('metric')}")
        
        elif signal.get('type') == 'data_quality_signal':
             if signal.get('status') == 'warning':
                risk_score += 1
                reasons.append(f"数据质量: {signal.get('message')}")

    if risk_score == 0:
        level = "Low"
        icon = "🟢"
    elif risk_score <= 2:
        level = "Medium"
        icon = "🟡"
    else:
        level = "High"
        icon = "🔴"

    return {
        "level": level,
        "score": risk_score,
        "reasons": reasons,
        "icon": icon
    }

def call_deepseek_reasoner(context_data: Dict[str, Any], prompt_type: str = "daily") -> Tuple[str, Dict[str, Any]]:
    """
    调用 DeepSeek Reasoner 模型生成分析报告
    Returns: (content, metrics)
    """
    if not API_KEY:
        return "⚠️ Error: DeepSeek API Key not found in .env", {}

    base_url = "https://api.deepseek.com"
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json"
    }

    # 将复杂对象转换为 JSON 字符串以便 LLM 理解
    data_str = json.dumps(context_data, ensure_ascii=False, indent=2, default=str)

    if prompt_type == "daily":
        system_prompt = """你是一位“数据侦探”。请根据提供的经营数据（风险评估、核心指标、同环比、异常信号、结构拆解、分布特征），生成一份**极简**且**高密度**的【每日经营诊断】。

**原则：**
1. **结论先行**：直接引用 `risk_assessment` 中的风险等级和原因。
2. **拒绝废话**：不要写“数据表明”、“经过分析”等垫话。
3. **关键信息**：必须包含具体的 `sales` 数值、同环比变化、以及具体的 `signals` 详情。

**输出模板：**

## {risk_icon} 诊断结论：风险 {risk_level}
**核心数据**：销量 {sales} ({mom_str}, {wow_str})。
**风险判定**：{risk_reasons_str}（若无风险则写“各项指标运行平稳”）。

## 🔍 异动归因
**1. 结构拆解**：{top_contributor} 占比 {top_share}，{change_desc}。
**2. 分布特征**：{distribution_desc}。
**3. 异常信号**：
- {signal_1}
- {signal_2}
*(若无信号则不显示此小节)*

**注意**：
- 替换模板中的 {...} 为实际数据。
- 如果 risk_level 为 High，请使用严肃警示语气。
"""
    else: # range summary
        system_prompt = """你是一位“趋势捕手”。请根据区间内的每日核心指标与异常信号，生成一份**高密度**的【区间经营轨迹综述】。

**原则：**
1. **宏观视角**：关注整体趋势（上升/下降/震荡），而非每日流水账。
2. **异常驱动**：重点复盘区间内的“异常点”（高风险日期、突变点）。
3. **极简输出**：拒绝废话。

**输出格式：**

**1. 轨迹概览**
[区间总量] [趋势形态：如“先抑后扬”] [关键极值：最高/最低日]。

**2. 异常复盘**
- [日期]: [异常描述] (引用 Z-Score 或 风险等级)。
- [日期]: [异常描述]。
*(若无异常，写“区间内运行平稳，无显著异常点”)*

**3. 总结与建议**
[一句话总结区间表现及后续关注点]。
"""

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"请根据以下数据生成分析报告：\n\n{data_str}"}
    ]

    payload = {
        "model": "deepseek-reasoner",
        "messages": messages,
        "stream": False
    }

    try:
        print("🤔 DeepSeek Reasoner is thinking...", end="", flush=True)
        start_time = time.time()
        resp = requests.post(f"{base_url}/chat/completions", json=payload, headers=headers)
        end_time = time.time()
        elapsed_sec = end_time - start_time
        print(f" Done. ({elapsed_sec:.2f}s)")
        
        if resp.status_code != 200:
            return f"Error from API: {resp.text}", {}
            
        data = resp.json()
        metrics = {
            "elapsed_sec": elapsed_sec,
            "usage": data.get("usage", {})
        }
        
        if "choices" in data and data["choices"]:
            choice = data["choices"][0]
            # 可以选择性地打印 reasoning_content
            # reasoning = choice["message"].get("reasoning_content", "")
            content = choice["message"].get("content", "")
            return content, metrics
        return "Error: No content in response.", metrics
    except Exception as e:
        return f"Error calling API: {str(e)}", {}

def analyze_point(target_date_str: str) -> Dict[str, Any]:
    """
    Run analysis for a single point in time using Execution Graph.
    """
    today = pd.Timestamp.now().normalize()
    
    if target_date_str == "yesterday":
        target_date = today - pd.Timedelta(days=1)
        date_range = "yesterday"
        
        hist_s = target_date - pd.Timedelta(days=30)
        hist_e = target_date - pd.Timedelta(days=1)
        history_range_str = f"{hist_s.strftime('%Y-%m-%d')}/{hist_e.strftime('%Y-%m-%d')}"
        
    else:
        target_date = pd.to_datetime(target_date_str, errors="raise").normalize()
        date_range = target_date.strftime("%Y-%m-%d")
        
        hist_s = target_date - pd.Timedelta(days=30)
        hist_e = target_date - pd.Timedelta(days=1)
        history_range_str = f"{hist_s.strftime('%Y-%m-%d')}/{hist_e.strftime('%Y-%m-%d')}"

    print(f"\n🔍 Analyzing Date: {date_range} (History Baseline: {history_range_str})")

    app = build_execution_graph()

    # 定义与 yesterday_lock.py 相同的 DSL 序列
    dsl_sequence = [
        {
            "id": "baseline_query",
            "tool": "query",
            "parameters": {"metric": "sales", "date_range": date_range},
        },
        {
            "id": "short_term_trend",
            "tool": "trend",
            "parameters": {
                "metric": "sales",
                "time_grain": "day",
                "compare_type": "mom",
                "date_range": date_range,
            },
        },
        {
            "id": "cycle_comparison",
            "tool": "trend",
            "parameters": {
                "metric": "sales",
                "time_grain": "day",
                "compare_type": "wow",
                "date_range": date_range,
            },
        },
        {
            "id": "anomaly_check",
            "tool": "trend",
            "parameters": {
                "metric": "sales",
                "time_grain": "day",
                "compare_type": "vs_avg",
                "date_range": history_range_str, 
            },
        },
        {
            "id": "structural_rollup",
            "tool": "rollup",
            "parameters": {
                "metric": "sales",
                "dimension": "series_group",
                "date_range": date_range,
            },
        },
        {
            "id": "pareto_scan",
            "tool": "pareto",
            "parameters": {
                "metric": "sales",
                "dimension": "series_group",
                "date_range": date_range,
            },
        },
        {
            "id": "distribution_analysis",
            "tool": "histogram",
            "parameters": {
                "metric": "datediff('day',first_assign_time,lock_time)",
                "date_range": date_range,
                "compare_date_range": history_range_str,
                "bins": 30,
            },
        }
    ]

    initial_state = {
        "dsl_sequence": dsl_sequence,
        "current_step": 0,
        "results": {},
        "signals": [],
    }

    # 执行 Graph
    final_state = app.invoke(initial_state)
    
    # 计算风险等级
    risk_assessment = calculate_risk(final_state["signals"])

    # 准备上下文数据供 Reasoner 使用
    context_data = {
        "date": date_range,
        "results": final_state["results"],
        "signals": final_state["signals"],
        "risk_assessment": risk_assessment
    }
    
    return context_data

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

def analyze_range(start_date: str, end_date: str):
    print(f"🚀 Starting Reasoner Trajectory Analysis: {start_date} to {end_date}")
    
    s = pd.to_datetime(start_date)
    e = pd.to_datetime(end_date)
    dates = pd.date_range(start=s, end=e, freq='D')
    
    daily_summaries = []
    
    for d in dates:
        d_str = d.strftime("%Y-%m-%d")
        context_data = analyze_point(d_str)
        
        # 仅收集每日核心数据，不生成每日简报
        print(f"Processing {d_str}...", flush=True)
        
        # 提取关键指标供区间分析使用
        baseline = context_data.get("results", {}).get("baseline_query", {})
        daily_summaries.append({
            "date": d_str,
            "core_metric": baseline,
            "signals": context_data.get("signals", [])
        })

    # 最后生成区间汇总
    print("\n📚 Generating Range Summary...")
    range_report, metrics = call_deepseek_reasoner({"range_data": daily_summaries}, prompt_type="range")
    print("\n" + "="*50)
    print(f"📅 区间轨迹深度综述 ({start_date} ~ {end_date})")
    print("="*50)
    print(range_report)
    print_metrics(metrics)

def main() -> None:
    args = _parse_args()
    
    if args.start and args.end:
        analyze_range(args.start, args.end)
    elif args.date:
        context_data = analyze_point(args.date)
        print(f"\n📝 Generating Report for {args.date}...")
        report, metrics = call_deepseek_reasoner(context_data, prompt_type="daily")
        print("\n" + "="*50)
        print(f"📊 DeepSeek Reasoner Analysis Report ({args.date})")
        print("="*50)
        print(report)
        print_metrics(metrics)
    else:
        print("Error: Please provide --date or --start and --end")

if __name__ == "__main__":
    main()
