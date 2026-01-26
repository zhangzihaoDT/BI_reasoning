import os
import sys
import json
import urllib.request
import urllib.error

class PlanningAgent:
    def __init__(self, base_dir=None):
        if base_dir is None:
            self.base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        else:
            self.base_dir = base_dir
            
        self.schema_path = os.path.join(self.base_dir, "world", "schema.md")
        self.business_def_path = os.path.join(self.base_dir, "world", "business_definition.json")
        self.tool_path = os.path.join(self.base_dir, "world", "tool.md")
        self.planning_rules_path = os.path.join(self.base_dir, "agents", "planning_skills.yaml")
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
                        if 'deepseek =' in line: # handle potential spaces
                             return line.split('=', 1)[1].strip()
        return None

    def _load_context(self):
        context = {}
        with open(self.schema_path, 'r', encoding='utf-8') as f:
            context['schema'] = f.read()
        with open(self.business_def_path, 'r', encoding='utf-8') as f:
            context['business_def'] = f.read()
        with open(self.tool_path, 'r', encoding='utf-8') as f:
            context['tools'] = f.read()
        with open(self.planning_rules_path, 'r', encoding='utf-8') as f:
            context['planning_rules'] = f.read()
        return context

    def generate_plan(self, query):
        if not self.api_key:
            return "Error: Deepseek API key not found in .env"

        system_prompt = f"""
You are a senior Data Analyst Planning Agent. 
Your goal is to translate a user's natural language business query into a structured "Evaluation Action Matrix" (DSL), strictly following the defined Planning Rules.

**Context Resources:**

1. **Data Schema (`schema.md`):**
{self.context['schema']}

2. **Business Definitions (`business_definition.json`):**
{self.context['business_def']}

3. **Available Tools (`tool.md`):**
{self.context['tools']}

4. **Planning Rules (`planning_rules.yaml`):**
{self.context['planning_rules']}

**Task:**

1. **Intent Classification:**
   - Analyze the user's query against the `intents` defined in `planning_rules.yaml`.
   - Identify the matching intent (e.g., `status_check`, `trend_analysis`).

2. **Strategy Selection:**
   - Use the `default_strategy` associated with the identified intent.
   - Retrieve the `dsl_sequence` from the `strategies` section.

3. **Plan Generation (Instantiation):**
   - Instantiate the steps defined in the strategy's `dsl_sequence`.
   - **Replace Placeholders:** You must intelligently replace placeholders like `{{primary_metric}}`, `{{target_date}}`, `{{default_structure_dimension}}` with actual values derived from the User Query and Schema.
     - `{{primary_metric}}`: Map user terms (e.g., "销量") to schema columns (e.g., "Order Number 不同计数"). Use `defaults` in yaml if needed.
     - `{{target_date}}`: Infer from query (e.g., "yesterday" -> "yesterday", "last week" -> "last_week").
     - `{{default_structure_dimension}}`: Infer from query (e.g., "按城市" -> "Store City") OR use `defaults` from yaml (e.g., "车型分组").
   - **Reasoning:** Update the reasoning to be specific to the current query.
   - **Output Key:** Ensure every step has a unique and meaningful `output_key`.
   - **Limit:** Keep the total number of steps under 10 to ensure the output is not truncated. Prioritize the most critical analysis steps.

**Output Format (JSON):**
Return a JSON array of objects. Each object represents an action step and must have the following fields:
- `step_id`: Unique integer ID (1, 2, 3...)
- `action_name`: A short, human-readable description.
- `tool_name`: The exact name of the tool to use.
- `parameters`: A dictionary of arguments.
- `reasoning`: A brief explanation.
- `output_key`: A stable key.

**Example:**
If User asks: "昨日销量如何"
And Intent is `status_check` -> Strategy `breadth_scan`
Then output should follow the `breadth_scan` sequence (Baseline -> Short-term Trend -> Cycle Comparison -> Structural Rollup), with parameters filled in.

**User Query:**
{query}
"""
        
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        data = {
            "model": "deepseek-chat",
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": query}
            ],
            "temperature": 0.1,
            "max_tokens": 2000
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

if __name__ == "__main__":
    agent = PlanningAgent()
    query = sys.argv[1] if len(sys.argv) > 1 else "昨日销量如何"
    print(f"Query: {query}\n")
    plan = agent.generate_plan(query)
    print(plan)
