from typing import Optional, Dict, Any
import pandas as pd
import json
import os

OPENAI_API_KEY = os.getenv('OPENAI_API_KEY','')
ANTHROPIC_API_KEY = os.getenv('ANTHROPIC_API_KEY','')

_SYSTEM = "You are a careful data scientist. Provide brief, caveated insights."

def _openai_insight(prompt: str) -> Optional[str]:
    if not OPENAI_API_KEY: return None
    from openai import OpenAI
    client = OpenAI(api_key=OPENAI_API_KEY)
    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=0.2,
        messages=[{"role":"system","content":_SYSTEM},{"role":"user","content":prompt}],
    )
    return resp.choices[0].message.content.strip()

def _anthropic_insight(prompt: str) -> Optional[str]:
    if not ANTHROPIC_API_KEY: return None
    from anthropic import Anthropic
    client = Anthropic(api_key=ANTHROPIC_API_KEY)
    msg = client.messages.create(
        model="claude-3-haiku-20240307",
        temperature=0.2,
        max_tokens=400,
        system=_SYSTEM,
        messages=[{"role":"user","content":prompt}]
    )
    return "".join(b.text for b in msg.content if b.type=="text").strip()

def summarize_result(result: pd.DataFrame, max_rows: int=5) -> str:
    if not isinstance(result, pd.DataFrame): return str(result)
    head = result.head(max_rows)
    meta = {"rows": int(result.shape[0]), "cols": int(result.shape[1]), "preview_rows": int(min(max_rows, result.shape[0]))}
    return json.dumps({"meta": meta, "columns": list(result.columns), "preview": head.to_dict(orient="records")})

def generate_insights(question: str, plan: Dict[str,Any], result: pd.DataFrame, schema: Dict[str,str]) -> Optional[str]:
    t = (plan or {}).get("type","")
    if not any(t.startswith(x) for x in ["aggregate","correlation","anomaly","group_count","sort_top"]):
        return None
    summary = summarize_result(result, max_rows=5)
    cols_blob = "\\n".join(f"- {c}: {schema.get(c,'?')}" for c in result.columns)
    prompt = f'''
User question: {question}
Plan: {json.dumps(plan)}
Result summary (JSON): {summary}

Columns (type):
{cols_blob}

Write 3–5 concise bullet insights. Be cautious:
- Correlation does not imply causation.
- Mention potential confounders and data quality concerns if relevant.
- If hypotheses are speculative, say so.
'''
    return _openai_insight(prompt) or _anthropic_insight(prompt)
