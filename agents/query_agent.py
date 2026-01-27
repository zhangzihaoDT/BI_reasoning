import os
import json
import urllib.request
import urllib.error
import datetime
import re
from tools.query import QueryTool
from tools.rollup import RollupTool
from tools.decompose import CompositionTool

class QueryAgent:
    def __init__(self, base_dir=None):
        if base_dir is None:
            self.base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        else:
            self.base_dir = base_dir
            
        self.schema_path = os.path.join(self.base_dir, "world", "schema.md")
        self.business_def_path = os.path.join(self.base_dir, "world", "business_definition.json")
        self.query_skills_path = os.path.join(self.base_dir, "agents", "query_skills.yaml")
        self.env_path = os.path.join(self.base_dir, ".env")
        
        self.api_key = self._load_api_key()
        self.context = self._load_context()

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
        if os.path.exists(self.query_skills_path):
            with open(self.query_skills_path, 'r', encoding='utf-8') as f:
                context['query_skills'] = f.read()
        else:
            context['query_skills'] = ""
        return context

    def _call_llm(self, system_prompt, user_prompt):
        if not self.api_key:
            return "Error: Deepseek API key not found."

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        data = {
            "model": "deepseek-chat",
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            "temperature": 0.1,
            "max_tokens": 500
        }
        
        try:
            req = urllib.request.Request(
                "https://api.deepseek.com/chat/completions",
                data=json.dumps(data).encode('utf-8'),
                headers=headers,
                method='POST'
            )
            with urllib.request.urlopen(req) as response:
                result = json.loads(response.read().decode('utf-8'))
                return result['choices'][0]['message']['content']
        except urllib.error.HTTPError as e:
            return f"Error calling API: {e.code} - {e.read().decode('utf-8')}"
        except Exception as e:
            return f"Error calling API: {str(e)}"

    def run(self, query):
        print(f"🤖 QueryAgent received: {query}")

        today = datetime.date.today().strftime("%Y-%m-%d")
        
        # Construct prompt using query_skills.yaml if available, else fallback to hardcoded
        skills_content = self.context.get('query_skills', '')
        
        if skills_content:
            system_prompt = f"""
You are a Data Query Assistant. Your ONLY goal is to convert natural language into a tool call JSON for data querying.
You are NOT an analyst. You do NOT answer questions directly. You ONLY output JSON (no markdown).

**Context:**
- Today's date: {today}
- Schema:
{self.context['schema']}
- Business Definitions:
{self.context['business_def']}

**Output JSON Format (always):**
{{
  "tool": "query_or_rollup_or_composition",
  "parameters": {{
    "metric": "metric_name",
    "date_range": "date_range_string",
    "filters": [{{"field":"series_group","op":"=","value":"LS9"}}],
    "dimension": "dimension_for_rollup_or_composition",
    "dimensions": ["dimension1","dimension2"],
    "interval": "day/week/month/year"
  }}
}}

**Skills & Rules:**
{skills_content}
"""
        else:
            system_prompt = f"""
You are a Data Query Assistant. Your ONLY goal is to convert natural language into a tool call JSON for data querying.
You are NOT an analyst. You do NOT answer questions directly. You ONLY output JSON (no markdown).

**Context:**
- Today's date: {today}
- Schema:
{self.context['schema']}
- Business Definitions:
{self.context['business_def']}

**Output JSON Format (always):**
{{
  "tool": "query_or_rollup_or_composition",
  "parameters": {{
    "metric": "metric_name",
    "date_range": "date_range_string",
    "filters": [{{"field":"series_group","op":"=","value":"LS9"}}],
    "dimension": "dimension_for_rollup_or_composition",
    "dimensions": ["dimension1","dimension2"],
    "interval": "day/week/month/year"
  }}
}}

**Rules:**
- Choose tool:
  - Use "query" when user asks for a single number or simple time series trend.
  - Use "rollup" when user asks for breakdown/grouping by a dimension (e.g. "by city", "each model").
  - Use "composition" when user asks for percentage/ratio/share of a dimension (e.g. "product mix", "percentage by type").
- metric must be one of: 锁单量/交付数/开票量/开票金额/小订数/年龄/下发线索数 (or sales/age/leads).
- date_range:
  - "昨日/昨天" -> "yesterday"
  - "last 7 days" -> "last_7_days"
  - "last 30 days" -> "last_30_days"
  - "2025年12月" -> "2025-12"
  - "2025年12月1日" -> "2025-12-01"
  - default "yesterday" if absent
- filters:
  - If query contains model names like LS6/LS9/LS7/L7, use field="series" and op="in" with those names.
  - If query explicitly mentions a series_group key (CM2/CM1/CM0/DM1/DM0/LS9/LS7/L7/其他) together with "车型分组" or "series_group", use field="series_group".
  - If query contains product type words like "增程" or "纯电", use field="product_type" with "=".
  - If query contains city/region/store/channel names, add corresponding filters using "=" when exact, otherwise use "contains".
  - If query implies population subset (e.g. "开票订单", "交付用户"), add filter field IS NOT NULL:
    - "开票" -> {"field":"invoice_upload_time","op":"not_null","value":true}
    - "交付" -> {"field":"delivery_date","op":"not_null","value":true}
    - "锁单" -> {"field":"lock_time","op":"not_null","value":true}
- rollup dimension allowed:
  - series, product_name, series_group, product_type, parent_region_name, store_city, store_name, first_middle_channel_name, gender, age_band

**Examples:**
User: "昨日锁单数"
{{"tool":"query","parameters":{{"metric":"锁单量","date_range":"yesterday"}}}}

User: "LS9 2025年12月交付数"
{{"tool":"query","parameters":{{"metric":"交付数","date_range":"2025-12","filters":[{{"field":"series_group","op":"=","value":"LS9"}}]}}}}

User: "LS9 2025年12月交付数 按城市"
{{"tool":"rollup","parameters":{{"metric":"交付数","date_range":"2025-12","filters":[{{"field":"series_group","op":"=","value":"LS9"}}],"dimension":"store_city"}}}}

User: "LS6,LS9 2025年12月分别锁单多少"
{{"tool":"rollup","parameters":{{"metric":"锁单量","date_range":"2025-12","filters":[{{"field":"series","op":"in","value":["LS6","LS9"]}}],"dimension":"series"}}}}

User: "LS9 2025年12月锁单 按产品名称看各车型贡献"
{{"tool":"rollup","parameters":{{"metric":"锁单量","date_range":"2025-12","filters":[{{"field":"series_group","op":"=","value":"LS9"}}],"dimension":"product_name"}}}}

User: "2025年12月车型为 CM2 增程的锁单量?"
{{"tool":"query","parameters":{{"metric":"锁单量","date_range":"2025-12","filters":[{{"field":"series_group","op":"=","value":"CM2"}},{{"field":"product_type","op":"=","value":"增程"}}]}}}}
"""

        if not self.api_key:
            extracted = self._heuristic_extract(query)
        else:
            llm_response = self._call_llm(system_prompt, query)
            if isinstance(llm_response, str) and llm_response.startswith("Error"):
                extracted = self._heuristic_extract(query)
            else:
                if "```json" in llm_response:
                    llm_response = llm_response.split("```json")[1].split("```")[0].strip()
                elif "```" in llm_response:
                    llm_response = llm_response.split("```")[1].split("```")[0].strip()
                try:
                    extracted = json.loads(llm_response)
                except json.JSONDecodeError:
                    extracted = self._heuristic_extract(query)

        tool_name = extracted.get("tool") or "query"
        parameters = extracted.get("parameters") or {}

        # Post-process parameters to ensure population filters are applied for Age queries
        # This handles cases where LLM misses the instruction
        metric = parameters.get("metric")
        filters = parameters.get("filters", [])
        q_lower = query.lower()
        
        if metric == "age" or "age" in str(metric).lower():
            # Check if relevant filters already exist to avoid duplication
            has_invoice = any(f.get("field") == "invoice_upload_time" for f in filters)
            has_delivery = any(f.get("field") == "delivery_date" for f in filters)
            has_lock = any(f.get("field") == "lock_time" for f in filters)
            
            if ("开票" in query or "invoice" in q_lower) and not has_invoice:
                filters.append({"field": "invoice_upload_time", "op": "not_null", "value": True})
            elif ("交付" in query or "delivery" in q_lower) and not has_delivery:
                filters.append({"field": "delivery_date", "op": "not_null", "value": True})
            elif ("锁单" in query or "lock" in q_lower) and not has_lock:
                filters.append({"field": "lock_time", "op": "not_null", "value": True})
            
            parameters["filters"] = filters

        step = {"id": "query_action", "tool": tool_name, "parameters": parameters}

        if tool_name == "composition":
            tool = CompositionTool()
        elif tool_name == "rollup":
            tool = RollupTool()
        else:
            tool = QueryTool()
            
        try:
            return tool.execute(step, {})
        except Exception as e:
            return f"❌ Query Execution Failed: {str(e)}"

    def _heuristic_extract(self, query: str) -> dict:
        q = str(query or "").strip()
        q_no_space = re.sub(r"\s+", "", q)

        metric = None
        if any(k in q for k in ["锁单数", "锁单量", "销量"]):
            metric = "锁单量"
        elif any(k in q for k in ["交付数", "交付量"]):
            metric = "交付数"
        elif any(k in q for k in ["开票金额"]):
            metric = "开票金额"
        elif any(k in q for k in ["开票数", "开票量"]):
            metric = "开票量"
        elif any(k in q for k in ["小订数", "小订量", "意向金"]):
            metric = "小订数"
        elif any(k in q for k in ["平均年龄", "年龄", "岁"]):
            metric = "age"
        elif any(k in q for k in ["下发线索数", "线索数", "线索", "leads"]):
            metric = "下发线索数"
        else:
            metric = "锁单量"

        date_range = "yesterday"
        if "昨日" in q or "昨天" in q:
            date_range = "yesterday"
        elif "近两周" in q:
             date_range = "last_14_days"
        elif "近一周" in q:
             date_range = "last_7_days"
        elif "近一月" in q or "近一个月" in q:
             date_range = "last_30_days"
        elif re.search(r"近(\d+)天", q):
             m = re.search(r"近(\d+)天", q)
             date_range = f"last_{m.group(1)}_days"
        elif re.search(r"近(\d+)周", q):
             m = re.search(r"近(\d+)周", q)
             days = int(m.group(1)) * 7
             date_range = f"last_{days}_days"
        elif re.search(r"近(\d+)月", q):
             m = re.search(r"近(\d+)月", q)
             days = int(m.group(1)) * 30
             date_range = f"last_{days}_days"
        elif "至今" in q or "since" in q:
            # Handle "YYYY年MM月DD日至今"
            m_day = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日", q_no_space)
            if m_day:
                y, mo, d = m_day.groups()
                start_date = f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"
                today_str = datetime.date.today().strftime("%Y-%m-%d")
                date_range = f"{start_date}/{today_str}"
        else:
            m_day = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日", q_no_space)
            if m_day:
                y, mo, d = m_day.groups()
                date_range = f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"
            else:
                m_month = re.search(r"(\d{4})年(\d{1,2})月", q_no_space)
                if m_month:
                    y, mo = m_month.groups()
                    date_range = f"{int(y):04d}-{int(mo):02d}"

        dimension = None
        if re.search(r"(按|分|各).*(大区)", q):
            dimension = "parent_region_name"
        elif re.search(r"(按|分|各).*(城市)", q):
            dimension = "store_city"
        elif re.search(r"(按|分|各).*(门店)", q):
            dimension = "store_name"
        elif re.search(r"(按|分|各).*(渠道)", q):
            dimension = "first_middle_channel_name"
        elif re.search(r"(按|分|各).*(产品|产品名称)", q):
            dimension = "product_name"
        elif re.search(r"(按|分|各).*(车型|车型分组|版本)", q):
            dimension = "series_group"
        elif re.search(r"(按|分|各).*(性别)", q):
            dimension = "gender"
        elif re.search(r"(按|分|各).*(年龄段|年龄)", q):
            dimension = "age_band"

        filters = []
        try:
            business_def = json.loads(self.context.get("business_def") or "{}")
            series_group_logic = business_def.get("series_group_logic") or {}
            series_keys = list(series_group_logic.keys())
            model_series_mapping = business_def.get("model_series_mapping") or {}
            model_keys = list(model_series_mapping.keys())
        except Exception:
            series_keys = []
            model_keys = []

        matched_models = [m for m in model_keys if m and m in q]
        if matched_models:
            filters.append({"field": "series", "op": "in", "value": matched_models})
        else:
            for k in series_keys:
                if k and k in q:
                    filters.append({"field": "series_group", "op": "=", "value": k})
                    break

        if ("女性" in q) or ("女" in q and "男女" not in q and "男女" not in q):
            filters.append({"field": "gender", "op": "=", "value": "女"})
        elif ("男性" in q) or ("男" in q and "男女" not in q):
            filters.append({"field": "gender", "op": "=", "value": "男"})

        if "增程" in q:
            filters.append({"field": "product_type", "op": "=", "value": "增程"})
        elif "纯电" in q:
            filters.append({"field": "product_type", "op": "=", "value": "纯电"})
            
        # Population filters for Age query
        if metric == "age":
            if "开票" in q or "invoice" in q.lower():
                filters.append({"field": "invoice_upload_time", "op": "not_null", "value": True})
            elif "交付" in q or "delivery" in q.lower():
                filters.append({"field": "delivery_date", "op": "not_null", "value": True})
            elif "锁单" in q or "lock" in q.lower():
                filters.append({"field": "lock_time", "op": "not_null", "value": True})

        tool = "rollup" if dimension else "query"
        # Heuristic override for composition
        if any(k in q for k in ["占比", "比例", "份额", "构成", "composition", "share", "ratio", "mix"]):
            tool = "composition"

        parameters = {"metric": metric, "date_range": date_range}
        if filters:
            parameters["filters"] = filters
        if tool == "rollup":
            parameters["dimension"] = dimension
        if tool == "composition":
            parameters["dimension"] = dimension
            # Try to find interval
            if any(k in q for k in ["每天", "daily", "by day", "day"]):
                parameters["interval"] = "day"
            elif any(k in q for k in ["每周", "weekly", "by week", "week"]):
                parameters["interval"] = "week"
            elif any(k in q for k in ["每月", "monthly", "by month", "month"]):
                parameters["interval"] = "month"
        return {"tool": tool, "parameters": parameters}
