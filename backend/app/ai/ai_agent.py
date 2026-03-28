"""
AI Agent Service — Module 5 Integration
Uses Groq LLM (llama3) to generate natural language summaries of extracted metadata.
Falls back to rule-based summaries if no API key is configured.
"""
import json
import re
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
    prompt = f"""You are a data analyst AI assistant for DataHub, a version control system for ML datasets.

A file named '{meta.get("file_name", "unknown")}' ({meta.get("file_type", "").upper()}) was just committed.

Key facts:
- Rows: {meta.get("row_count", "?")}
- Columns: {meta.get("column_count", "?")}
- Schema (first 5 cols): {json.dumps(list(meta.get("columns_schema", {}).keys())[:5])}
- Sample statistics: {json.dumps(stats_preview, indent=2)}
- Detected ML metrics: {json.dumps(custom) if custom else "None"}

Write a concise 2-3 sentence natural language summary of this dataset for a data scientist.
Focus on: data shape, notable quality issues (nulls, skew), and any detected performance metrics.
Be direct and technical."""
    return prompt


def _rule_based_summary(meta: Dict[str, Any]) -> str:
    """Fallback summary when no Groq API key is available."""
    rows = meta.get("row_count", 0)
    cols = meta.get("column_count", 0)
    fname = meta.get("file_name", "dataset")
    ftype = meta.get("file_type", "").upper()

    # Find columns with high null %
    high_nulls = []
    for col, s in meta.get("statistics", {}).items():
        if s.get("null_pct", 0) > 10:
            high_nulls.append(f"{col} ({s['null_pct']}%)")

    # Custom metrics
    metrics_info = ""
    for metric, vals in meta.get("custom_metrics", {}).items():
        metrics_info += f" The '{metric}' metric reached a max of {vals.get('max', '?'):.4f}."

    null_info = ""
    if high_nulls:
        null_info = f" Notable null values detected in: {', '.join(high_nulls[:3])}."

    return (
        f"The committed file '{fname}' ({ftype}) contains {rows:,} rows and {cols} columns."
        f"{null_info}"
        f"{metrics_info}"
        f" Data has been indexed and is ready for version-controlled querying."
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

    system_prompt = """You are DataHub AI, an expert data analyst assistant.
You help data scientists understand their datasets, debug quality issues, and interpret model metrics.
Be concise, technical, and actionable."""

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
