"""
Cost Monitor: tracks Anthropic API spend per session.

Provides a CostMonitor class that records each API call's token usage
and estimates the USD cost based on published per-model pricing.
Required by the case study to demonstrate cost awareness in production.
"""

import logging
import time
from typing import Any

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Pricing table (USD per 1 million tokens) — updated for Claude 3.5 / 4 era.
# Source: https://docs.anthropic.com/en/docs/about-claude/pricing
# ---------------------------------------------------------------------------

MODEL_PRICING: dict[str, dict[str, float]] = {
    "claude-sonnet-4-5": {"input": 3.00, "output": 15.00},
    "claude-sonnet-4-20250514": {"input": 3.00, "output": 15.00},
    "claude-3-5-sonnet-20241022": {"input": 3.00, "output": 15.00},
    "claude-3-5-haiku-20241022": {"input": 0.80, "output": 4.00},
    "claude-3-opus-20240229": {"input": 15.00, "output": 75.00},
}

# Fallback pricing when the model is not in the table.
DEFAULT_PRICING: dict[str, float] = {"input": 3.00, "output": 15.00}


class APICallRecord(BaseModel):
    """Single API call record with token counts and cost estimate."""

    model: str
    input_tokens: int
    output_tokens: int
    estimated_cost_usd: float = Field(default=0.0)
    timestamp: float = Field(default_factory=time.time)


class CostMonitor:
    """
    Per-session cost tracker for Anthropic API calls.

    Usage:
        monitor = CostMonitor()
        monitor.record_call("claude-sonnet-4-5", input_tokens=1200, output_tokens=350)
        print(monitor.session_report())
    """

    def __init__(self) -> None:
        self._calls: list[APICallRecord] = []

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def record_call(
        self, model: str, input_tokens: int, output_tokens: int
    ) -> APICallRecord:
        """Record a single API call and return the record."""
        pricing = MODEL_PRICING.get(model, DEFAULT_PRICING)
        cost = (
            (input_tokens / 1_000_000) * pricing["input"]
            + (output_tokens / 1_000_000) * pricing["output"]
        )
        record = APICallRecord(
            model=model,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            estimated_cost_usd=round(cost, 6),
        )
        self._calls.append(record)
        logger.debug(
            "API call recorded: model=%s, in=%d, out=%d, cost=$%.6f",
            model, input_tokens, output_tokens, cost,
        )
        return record

    def session_total(self) -> dict[str, Any]:
        """Return aggregate session statistics."""
        total_input = sum(c.input_tokens for c in self._calls)
        total_output = sum(c.output_tokens for c in self._calls)
        total_cost = sum(c.estimated_cost_usd for c in self._calls)
        return {
            "total_calls": len(self._calls),
            "total_input_tokens": total_input,
            "total_output_tokens": total_output,
            "total_tokens": total_input + total_output,
            "estimated_cost_usd": round(total_cost, 6),
        }

    def session_report(self) -> str:
        """Return a human-readable session cost report."""
        totals = self.session_total()
        lines = [
            "== API Cost Report ==",
            f"Total API calls:    {totals['total_calls']}",
            f"Input tokens:       {totals['total_input_tokens']:,}",
            f"Output tokens:      {totals['total_output_tokens']:,}",
            f"Total tokens:       {totals['total_tokens']:,}",
            f"Estimated cost:     ${totals['estimated_cost_usd']:.4f}",
        ]
        if self._calls:
            lines.append("")
            lines.append("-- Per-call breakdown --")
            for i, call in enumerate(self._calls, 1):
                lines.append(
                    f"  {i}. {call.model} | in={call.input_tokens:,} "
                    f"out={call.output_tokens:,} | ${call.estimated_cost_usd:.6f}"
                )
        return "\n".join(lines)

    @property
    def calls(self) -> list[APICallRecord]:
        """Read-only access to recorded calls."""
        return list(self._calls)

    def reset(self) -> None:
        """Clear all recorded calls for a fresh session."""
        self._calls.clear()
