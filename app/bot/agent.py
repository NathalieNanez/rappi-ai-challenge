"""
Agent: the core conversational engine using the ReAct pattern.
Uses Anthropic's Claude with tool_use to answer data questions.
Maintains per-session conversation memory and tracks API cost.

ReAct pattern:
  1. Thought  — reason about what data is needed
  2. Action   — execute a tool (sql_query, generate_chart, get_context_info)
  3. Observation — process the tool result
  4. Repeat until the answer is complete
"""

import json
import logging
import os
from dataclasses import dataclass, field
from typing import Any, Iterator

import anthropic

from app.bot.prompts import SYSTEM_PROMPT, METRIC_GLOSSARY, FOLLOWUP_PROMPT, SQL_FIX_PROMPT
from app.bot.tools import TOOL_DEFINITIONS, TOOL_REGISTRY
from app.data.loader import get_loader
from app.utils.cost_monitor import CostMonitor

logger = logging.getLogger(__name__)

MODEL = "claude-sonnet-4-5"
MAX_TOKENS = 4096
MAX_TURNS = int(os.getenv("MAX_CONVERSATION_TURNS", "20"))

# Intent keywords used by the ReAct dispatcher to guide query construction.
INTENT_KEYWORDS: dict[str, list[str]] = {
    "trend": [
        "trend", "tendencia", "evolución", "evolution", "over time",
        "semanas", "weeks", "time series", "historical", "progresión",
    ],
    "comparison": [
        "compare", "comparar", "vs", "versus", "diferencia", "difference",
        "wealthy", "non wealthy", "benchmark", "ranking",
    ],
    "anomaly": [
        "anomaly", "anomalies", "anomalía", "anomalías", "spike", "drop", "caída", "subida",
        "unusual", "outlier", "atípico", "cambio brusco",
    ],
}


def classify_intent(text: str) -> str:
    """Classify user query intent from text keywords. Returns the best match."""
    text_lower = text.lower()
    scores: dict[str, int] = {k: 0 for k in INTENT_KEYWORDS}
    for intent, keywords in INTENT_KEYWORDS.items():
        for kw in keywords:
            if kw in text_lower:
                scores[intent] += 1
    best = max(scores, key=scores.get)
    return best if scores[best] > 0 else "general"


@dataclass
class Message:
    role: str  # "user" | "assistant"
    content: Any  # str or list[dict] for multi-part messages


@dataclass
class ConversationSession:
    session_id: str
    messages: list[dict] = field(default_factory=list)
    charts: list[str] = field(default_factory=list)
    turn_count: int = 0
    cost_monitor: CostMonitor = field(default_factory=CostMonitor)

    def add_user(self, text: str) -> None:
        self.messages.append({"role": "user", "content": text})
        self.turn_count += 1

    def add_assistant(self, content: Any) -> None:
        self.messages.append({"role": "assistant", "content": content})

    def trim_if_needed(self) -> None:
        """Keep last N turns to stay within context window."""
        if len(self.messages) > MAX_TURNS * 2:
            self.messages = self.messages[:2] + self.messages[-(MAX_TURNS * 2 - 2):]


class RappiAgent:
    """
    ReAct agentic loop using Anthropic tool_use.
    Handles: Thought -> Action (sql_query / generate_chart) -> Observation -> repeat.
    Integrates CostMonitor for per-session API spend tracking.
    """

    def __init__(self) -> None:
        self.client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        self.loader = get_loader()
        self._system_prompt = SYSTEM_PROMPT.format(
            schema=self.loader.schema_description(),
            glossary=METRIC_GLOSSARY,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def chat(
        self,
        session: ConversationSession,
        user_message: str,
    ) -> dict[str, Any]:
        """
        Process one user turn using the ReAct pattern.
        Returns:
          {
            "text": str,              # markdown text response
            "charts": list[str],      # list of Plotly figure JSON strings
            "followups": list[str],   # suggested follow-up questions
            "tool_calls": list[dict], # debug info
            "intent": str,            # classified intent
            "cost": dict,             # session cost summary
          }
        """
        session.add_user(user_message)
        session.trim_if_needed()

        # Classify user intent to guide the agent's reasoning.
        intent = classify_intent(user_message)
        logger.info("User intent classified as: %s", intent)

        charts: list[str] = []
        csv_buffers: list[str] = []
        tool_calls_log: list[dict] = []
        final_text = ""

        # ReAct agentic loop: iterate until model stops requesting tools.
        messages = list(session.messages)
        max_iterations = 6

        for iteration in range(max_iterations):
            response = self.client.messages.create(
                model=MODEL,
                max_tokens=MAX_TOKENS,
                system=self._system_prompt,
                tools=TOOL_DEFINITIONS,
                messages=messages,
            )

            # Track API cost for this call.
            session.cost_monitor.record_call(
                model=MODEL,
                input_tokens=response.usage.input_tokens,
                output_tokens=response.usage.output_tokens,
            )

            # Collect text and tool-use blocks from the response.
            text_parts: list[str] = []
            tool_uses: list[Any] = []

            for block in response.content:
                if block.type == "text":
                    text_parts.append(block.text)
                elif block.type == "tool_use":
                    tool_uses.append(block)

            if text_parts:
                final_text = "\n".join(text_parts)

            # If model signals end_turn or no tools requested, we are done.
            if response.stop_reason == "end_turn" or not tool_uses:
                messages.append({"role": "assistant", "content": response.content})
                break

            # Process tool calls (Action step in ReAct).
            messages.append({"role": "assistant", "content": response.content})
            tool_results: list[dict] = []

            for tool_use in tool_uses:
                result = self._execute_tool(tool_use, charts, intent)
                if tool_use.name == "export_results" and result.get("ok"):
                    csv_buffers.append(result["csv_content"])
                    
                tool_calls_log.append({
                    "tool": tool_use.name,
                    "input": tool_use.input,
                    "result_ok": result.get("ok", True),
                })
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": tool_use.id,
                    "content": json.dumps(result),
                })

            # Feed Observation back to the model.
            messages.append({"role": "user", "content": tool_results})

        # Persist final assistant response in session.
        session.add_assistant(final_text)
        session.charts.extend(charts)

        followups = self._get_followups(user_message, final_text, session.cost_monitor)

        return {
            "text": final_text,
            "charts": charts,
            "csv_buffers": csv_buffers,
            "followups": followups,
            "tool_calls": tool_calls_log,
            "intent": intent,
            "cost": session.cost_monitor.session_total(),
        }

    def stream_chat(
        self,
        session: ConversationSession,
        user_message: str,
    ) -> Iterator[dict[str, Any]]:
        """
        Streaming version: yields events as they arrive.
        Event types: "text_delta", "chart", "csv_data", "followups", "cost", "done"
        """
        result = self.chat(session, user_message)

        for char in result["text"]:
            yield {"type": "text_delta", "delta": char}

        for chart_json in result["charts"]:
            yield {"type": "chart", "figure_json": chart_json}
            
        for csv_str in result.get("csv_buffers", []):
            yield {"type": "csv_data", "csv_content": csv_str}

        yield {"type": "followups", "questions": result["followups"]}
        yield {"type": "cost", "cost_report": result["cost"]}
        yield {"type": "done"}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _execute_tool(
        self, tool_use: Any, charts: list[str], intent: str
    ) -> dict[str, Any]:
        """Dispatch tool call, handle SQL errors with auto-retry."""
        tool_name = tool_use.name
        tool_input = tool_use.input

        if tool_name not in TOOL_REGISTRY:
            return {"ok": False, "error": f"Unknown tool: {tool_name}"}

        tool_fn = TOOL_REGISTRY[tool_name]
        result = tool_fn(**tool_input)

        # Auto-retry SQL errors once with LLM fix.
        if tool_name == "sql_query" and not result.get("ok"):
            logger.warning("SQL failed, attempting auto-fix: %s", result["error"])
            fixed_sql = self._fix_sql(tool_input["sql"], result["error"])
            if fixed_sql:
                result = TOOL_REGISTRY["sql_query"](sql=fixed_sql)
                if result.get("ok"):
                    result["auto_fixed"] = True

        # Collect charts from generated figures.
        if tool_name == "generate_chart" and result.get("ok"):
            charts.append(result["figure_json"])

        return result

    def _fix_sql(self, original_sql: str, error: str) -> str | None:
        """Ask Claude to fix a broken SQL query."""
        try:
            response = self.client.messages.create(
                model=MODEL,
                max_tokens=512,
                messages=[{
                    "role": "user",
                    "content": SQL_FIX_PROMPT.format(
                        sql=original_sql,
                        error=error,
                        schema=self.loader.schema_description(),
                    )
                }]
            )
            # Note: we do not track cost for SQL fix calls to keep debug costs low,
            # but production systems should track these as well.
            fixed = response.content[0].text.strip()
            fixed = fixed.replace("```sql", "").replace("```", "").strip()
            return fixed
        except Exception as e:
            logger.error("SQL fix failed: %s", e)
            return None

    def _get_followups(
        self, user_msg: str, assistant_msg: str, cost_monitor: CostMonitor
    ) -> list[str]:
        """Generate proactive follow-up question suggestions."""
        try:
            context = f"User asked: {user_msg}\nAssistant answered: {assistant_msg[:500]}"
            response = self.client.messages.create(
                model=MODEL,
                max_tokens=256,
                messages=[{
                    "role": "user",
                    "content": FOLLOWUP_PROMPT.format(context=context)
                }]
            )
            cost_monitor.record_call(
                model=MODEL,
                input_tokens=response.usage.input_tokens,
                output_tokens=response.usage.output_tokens,
            )
            raw = response.content[0].text.strip()
            # Handle potential markdown json fences from Claude
            raw = raw.replace("```json", "").replace("```", "").strip()
            return json.loads(raw)
        except Exception as e:
            logger.error(f"Error generando followups: {e}")
            return [
                "¿Qué zonas muestran la mayor caída en Perfect Orders esta semana?",
                "¿Cómo se compara el desempeño entre zonas Wealthy y Non-Wealthy en Colombia?",
                "¿Qué métricas están mejorando consistentemente en todos los países?",
            ]


# Session store (in-memory; swap for Redis in production).
_sessions: dict[str, ConversationSession] = {}


def get_or_create_session(session_id: str) -> ConversationSession:
    if session_id not in _sessions:
        _sessions[session_id] = ConversationSession(session_id=session_id)
    return _sessions[session_id]


def clear_session(session_id: str) -> None:
    _sessions.pop(session_id, None)
