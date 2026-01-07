import os
import sys
import json
import urllib.request
import urllib.error

class SuggestionAgent:
    def __init__(self, base_dir=None):
        if base_dir is None:
            self.base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        else:
            self.base_dir = base_dir
            
        self.schema_path = os.path.join(self.base_dir, "world", "schema.md")
        self.env_path = os.path.join(self.base_dir, ".env")
        
        self.api_key = self._load_api_key()
        self.schema = self._load_schema()

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

    def _load_schema(self):
        with open(self.schema_path, 'r', encoding='utf-8') as f:
            return f.read()

    def _call_llm(self, system_prompt, user_prompt="Please provide the output."):
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
            "temperature": 0.3,
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

    def generate_suggestions(self, risk_level, risk_factors, analysis_results):
        if not self.api_key:
            return ["Error: Deepseek API key not found. Cannot generate suggestions."]

        # Step 1: Generate English suggestions
        english_prompt = f"""
You are a Senior BI Analyst assisting an automated analysis system.

**Context Resources:**
Data Schema:
{self.schema}

**Current Situation:**
- Risk Level: {risk_level}
- Detected Risk Factors:
{json.dumps(risk_factors, indent=2, ensure_ascii=False)}

Note: Statistical anomaly has already been evaluated.

**Task:**
Provide exactly 3 high-priority analysis suggestions in ONE sentence each.
Prefix each with [PROCESS], [FUNNEL], [CHANNEL].
Use extreme-value-sensitive metrics where appropriate (e.g., lock_time tail >30 days, P90).
Format: 👉 [TAG] [Action] → To confirm whether [specific hypothesis].
Return ONLY a numbered list.
"""
        english_suggestions = self._call_llm(english_prompt, "Please provide the suggestions.")
        if english_suggestions.startswith("Error"):
            return [english_suggestions]

        # Step 2: Convert English suggestions to concise Chinese
        chinese_translation_prompt = f"""
You are an expert translator and business analyst.
Translate the following English analysis suggestions into concise Chinese.
Keep technical metrics and dimensions intact (like `series_group`, `lock_time`).
Keep the statistical tail/extreme value logic.
Keep one action per sentence, max 25 words, prefixed by [PROCESS], [FUNNEL], [CHANNEL].

English suggestions:
{english_suggestions}
"""
        chinese_suggestions = self._call_llm(chinese_translation_prompt, "Please translate.")
        
        return chinese_suggestions
