"""
AI Agent Service — Module 5 Integration
Uses Groq LLM (llama3) to generate natural language summaries of extracted metadata.
Falls back to rule-based summaries if no API key is configured.
"""
import json
from typing import Dict, Any, Optional

from groq import AsyncGroq

from app.config import get_settings

settings = get_settings()

GROQ_MODEL = "llama-3.1-8b-instant"


def _build_prompt(meta: Dict[str, Any]) -> str:
    """Build a concise prompt for the LLM to summarize dataset metadata."""
    stats_preview = {}
    for col, col_stats in list(meta.get("statistics", {}).items())[:5]:
        stats_preview[col] = {k: v for k, v in col_stats.items()
                               if k in ("dtype", "mean", "std", "min", "max", "null_pct", "top_values")}

    custom = meta.get("custom_metrics", {})
    prompt = f"""You are a DataHub Expert Analyst.
A dataset named '{meta.get("file_name", "unknown")}' ({meta.get("file_type", "").upper()}) has been indexed.

Metadata provided:
- Dimensions: {meta.get("row_count", "?")} rows x {meta.get("column_count", "?")} columns.
- Statistical Sample: {json.dumps(stats_preview)}
- Domain Specific Metrics: {json.dumps(custom) if custom else "None"}

TASK: Provide a high-density, professional summary in exactly 3 meaningful sentences.
1. Scope: Define the scale and likely business/scientific domain of the data.
2. Numeric Evidence: Cite 2-3 specific numeric facts (means, ranges, or peaks) from the stats.
3. Quality/Alerts: Identify one specific data quality trait (skew, nulls) or a trend in the custom metrics.
Format: Use standard sentence casing. Avoid generic phrases like 'This dataset'. Be direct."""
    return prompt


def _rule_based_summary(meta: Dict[str, Any]) -> str:
    """Fallback summary that extracts key statistical insights when LLM is unavailable."""
    rows = meta.get("row_count", 0)
    cols = meta.get("column_count", 0)
    fname = meta.get("file_name", "dataset")
    ftype = meta.get("file_type", "").upper()
    stats = meta.get("statistics", {})

    # Extract top numeric insight
    numeric_insight = ""
    for col, s in stats.items():
        if "mean" in s:
            numeric_insight = f" Notably, '{col}' averages {s['mean']:.2f} (range: {s['min']} to {s['max']})."
            break # just one for brevity

    # Extract top categorical insight
    cat_insight = ""
    for col, s in stats.items():
        if "top_values" in s and s["top_values"]:
            top_val = list(s["top_values"].keys())[0]
            cat_insight = f" The most frequent '{col}' is '{top_val}'."
            break

    # Find columns with high null %
    high_nulls = [f"{col} ({s['null_pct']}%)" for col, s in stats.items() if s.get("null_pct", 0) > 5]
    null_info = f" Quality issues: High nulls in {', '.join(high_nulls[:2])}." if high_nulls else " Data quality appears robust with minimal nulls."

    # Custom metrics
    metrics_info = ""
    for metric, vals in meta.get("custom_metrics", {}).items():
        if isinstance(vals, dict) and "max" in vals:
            metrics_info += f" Recorded '{metric}' peak: {vals['max']}."
        elif isinstance(vals, (int, float, str)):
            metrics_info += f" {metric.capitalize()}: {vals}."

    return (
        f"{rows:,} rows, {cols} cols. {fname} ({ftype}) focus.{numeric_insight}{cat_insight}"
        f"{null_info}{metrics_info}"
    ).strip()


async def generate_ai_summary(meta: Dict[str, Any]) -> str:
    """
    Call Groq LLM API to generate a natural language summary of the metadata.
    Falls back to rule-based summary if API key is missing or call fails.
    """
    if not settings.groq_api_key:
        return _rule_based_summary(meta)

    prompt = _build_prompt(meta)
    try:
        client = AsyncGroq(api_key=settings.groq_api_key)
        completion = await client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=200,
            temperature=0.4,
        )
        return completion.choices[0].message.content.strip()
    except Exception as e:
        # Gracefully fall back to rule-based summary
        return _rule_based_summary(meta)


async def chat_with_ai(question: str, context: str = "") -> str:
    """
    Interactive AI chat endpoint — allows users to ask questions about their data.
    """
    if not settings.groq_api_key:
        return (
            "AI chat requires a Groq API key. Please configure GROQ_API_KEY in .env. "
            "Visit https://console.groq.com to get a free API key.\n\n"
            f"Your question was: '{question}'"
        )

    system_prompt = """You are DataHub AI, a world-class Data Science & Analytics assistant. 
Your goal is to help users interpret complex datasets, audit data quality, and understand machine learning performance.

STRICT GUIDELINES:
1. Formatting: Always use Markdown for readability. Use bolding for keys, bullet points for lists, and tables for comparisons.
2. Character: Be technical, concise, and highly actionable.
3. Privacy: Do not mention internal database IDs or table names unless relevant.
4. If context is provided: Treat it as your source of truth for the specific dataset being discussed.
5. If no context is provided: Answer generally about DataHub's capabilities (versioning, metadata extraction, distributed storage)."""

    messages = [{"role": "system", "content": system_prompt}]
    if context:
        messages.append({"role": "user", "content": f"Context:\n{context}"})
    messages.append({"role": "user", "content": question})

    try:
        client = AsyncGroq(api_key=settings.groq_api_key)
        completion = await client.chat.completions.create(
            model=GROQ_MODEL,
            messages=messages,
            max_tokens=500,
            temperature=0.5,
        )
        return completion.choices[0].message.content.strip()
    except Exception as e:
        return f"AI service temporarily unavailable: {str(e)}"
