import os
import json
import requests
import datetime
import time
from tools.query import QueryTool
from tools.rollup import RollupTool

class QueryAgentReasoner:
    def __init__(self, base_dir=None):
        if base_dir is None:
            self.base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        else:
            self.base_dir = base_dir
            
        self.schema_path = os.path.join(self.base_dir, "world", "schema.md")
        self.business_def_path = os.path.join(self.base_dir, "world", "business_definition.json")
        self.env_path = os.path.join(self.base_dir, ".env")
        
        self.api_key = self._load_api_key()
        self.context = self._load_context()
        self.base_url = "https://api.deepseek.com"

    def _load_api_key(self):
        if os.path.exists(self.env_path):
            with open(self.env_path, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip() and not line.startswith('#'):
                        if 'deepseek=' in line:
                            return line.split('=', 1)[1].strip()
                        if 'deepseek =' in line:
                             return line.split('=', 1)[1].strip()
        return None

    def _load_context(self):
        context = {}
        with open(self.schema_path, 'r', encoding='utf-8') as f:
            context['schema'] = f.read()
        with open(self.business_def_path, 'r', encoding='utf-8') as f:
            context['business_def'] = f.read()
        return context

    def get_tools_definition(self):
        return [
            {
                "type": "function",
                "function": {
                    "name": "query_data",
                    "description": "Query a single number metric like sales, orders, etc.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "metric": {
                                "type": "string", 
                                "enum": ["锁单量", "交付数", "开票量", "开票金额", "小订数"],
                                "description": "The metric to query"
                            },
                            "date_range": {
                                "type": "string",
                                "description": "Date range, e.g., 'yesterday', '2025-12', '2025-12-01', 'last_7_days'"
                            },
                            "filters": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "field": {"type": "string"},
                                        "op": {"type": "string", "enum": ["=", "!=", "in", "contains", ">", "<", ">=", "<="]},
                                        "value": {"type": ["string", "number", "array", "boolean"]}
                                    },
                                    "required": ["field", "op", "value"]
                                }
                            },
                            "interval": {
                                "type": "string",
                                "enum": ["day", "week", "month", "year"],
                                "description": "Time interval for aggregation. If provided, returns a time series dictionary (e.g., {'2025-01-31': 100}). Use this for trends or breakdowns over time."
                            }
                        },
                        "required": ["metric", "date_range"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "rollup_data",
                    "description": "Query a metric broken down by dimensions (rollup).",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "metric": {
                                "type": "string",
                                "enum": ["锁单量", "交付数", "开票量", "开票金额", "小订数"],
                                "description": "The metric to query"
                            },
                            "date_range": {
                                "type": "string",
                                "description": "Date range"
                            },
                            "dimension": {
                                "type": "string",
                                "description": "Single dimension to group by"
                            },
                            "dimensions": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "List of dimensions to group by"
                            },
                            "filters": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "field": {"type": "string"},
                                        "op": {"type": "string"},
                                        "value": {"type": ["string", "number", "array", "boolean"]}
                                    },
                                    "required": ["field", "op", "value"]
                                }
                            }
                        },
                        "required": ["metric", "date_range"]
                    }
                }
            }
        ]

    def execute_tool(self, tool_name, args):
        try:
            if tool_name == "query_data":
                tool = QueryTool()
                step = {"id": "query_call", "tool": "query", "parameters": args}
                return tool.execute(step, {})
            elif tool_name == "rollup_data":
                tool = RollupTool()
                step = {"id": "rollup_call", "tool": "rollup", "parameters": args}
                return tool.execute(step, {})
            return {"error": f"Unknown tool: {tool_name}"}
        except Exception as e:
            return {"error": f"Tool execution failed: {str(e)}"}

    def _call_api(self, messages, tools=None):
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        payload = {
            "model": "deepseek-reasoner",
            "messages": messages,
            "stream": False
        }
        if tools:
            payload["tools"] = tools

        try:
            start_time = time.time()
            resp = requests.post(f"{self.base_url}/chat/completions", json=payload, headers=headers)
            end_time = time.time()
            elapsed_sec = end_time - start_time
            
            if resp.status_code != 200:
                return None
            data = resp.json()
            if "choices" not in data or not data["choices"]:
                return None
            choice = data["choices"][0]
            message = choice["message"]
            return {
                "content": message.get("content"),
                "tool_calls": message.get("tool_calls"),
                "reasoning_content": message.get("reasoning_content"),
                "message_object": message,
                "usage": data.get("usage", {}),
                "elapsed_sec": elapsed_sec
            }
        except Exception:
            return None

    def run(self, query):
        if not self.api_key:
             return "Error: Deepseek API key not found in .env", {}

        today = datetime.date.today().strftime("%Y-%m-%d")
        system_prompt = f"""
You are a Data Analysis Assistant. 
Your goal is to answer the user's questions about business data by querying the database using available tools.

**Context:**
- Today's date: {today}
- Schema:
{self.context['schema']}
- Business Definitions:
{self.context['business_def']}

**Process:**
1. Analyze the user's request.
2. Call the appropriate tool (query_data or rollup_data) to get the data.
   - Use 'query_data' for single numbers OR time-series trends (using 'interval' parameter).
   - Use 'rollup_data' when grouping by categorical dimensions (e.g., by city, by model).
3. If the tool returns data, interpret it and answer the user's question in natural language.
4. If the data is empty, state that clearly.
5. If the user asks for analysis, provide insights based on the returned data.
"""

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": query}
        ]

        max_turns = 5
        turn = 0
        total_metrics = {
            "elapsed_sec": 0,
            "usage": {
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "total_tokens": 0
            }
        }
        
        while turn < max_turns:
            response = self._call_api(messages, tools=self.get_tools_definition())
            if not response:
                return "Error: Failed to get response from API.", total_metrics
            
            # Aggregate metrics
            total_metrics["elapsed_sec"] += response.get("elapsed_sec", 0)
            usage = response.get("usage", {})
            total_metrics["usage"]["prompt_tokens"] += usage.get("prompt_tokens", 0)
            total_metrics["usage"]["completion_tokens"] += usage.get("completion_tokens", 0)
            total_metrics["usage"]["total_tokens"] += usage.get("total_tokens", 0)

            if response.get("tool_calls"):
                messages.append(response["message_object"])
                tool_calls = response["tool_calls"]
                for tool_call in tool_calls:
                    func_name = tool_call["function"]["name"]
                    try:
                        args = json.loads(tool_call["function"]["arguments"])
                    except json.JSONDecodeError:
                        args = {}
                    tool_result = self.execute_tool(func_name, args)
                    messages.append({
                        "role": "tool",
                        "tool_call_id": tool_call["id"],
                        "content": json.dumps(tool_result, ensure_ascii=False)
                    })
                turn += 1
            else:
                content = response.get("content") or "No content returned."
                return content, total_metrics
        return "Error: Maximum turns reached.", total_metrics
