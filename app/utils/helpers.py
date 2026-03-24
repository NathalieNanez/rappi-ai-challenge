"""
utils/helpers.py — Shared utilities used across the application.
Keeps cross-cutting concerns out of business logic modules.
"""

import hashlib
import json
import logging
import os
import time
from functools import wraps
from typing import Any, Callable

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────
# Formatting helpers
# ──────────────────────────────────────────────

def fmt_pct(value: float, decimals: int = 1) -> str:
    """Format a 0-1 float as a percentage string."""
    if value is None or (isinstance(value, float) and value != value):  # NaN
        return "N/A"
    return f"{value * 100:.{decimals}f}%"


def fmt_num(value: float, decimals: int = 2) -> str:
    """Format a float with fixed decimal places."""
    if value is None or (isinstance(value, float) and value != value):
        return "N/A"
    return f"{value:,.{decimals}f}"


def fmt_delta(new_val: float, old_val: float, is_pct: bool = False) -> str:
    """Return a ±XX% change string."""
    if old_val == 0 or old_val is None or new_val is None:
        return "N/A"
    delta = (new_val - old_val) / abs(old_val) * 100
    sign = "+" if delta >= 0 else ""
    formatted = fmt_pct(new_val - old_val) if is_pct else f"{sign}{delta:.1f}%"
    return formatted


def severity_emoji(severity: str) -> str:
    return {"critical": "🔴", "warning": "🟡", "info": "🔵"}.get(severity, "⚪")


def category_emoji(category: str) -> str:
    return {
        "anomaly": "🚨", "trend": "📉", "benchmark": "📊",
        "correlation": "🔗", "opportunity": "🚀",
    }.get(category, "📌")


# ──────────────────────────────────────────────
# Simple in-process cache (no Redis dependency)
# ──────────────────────────────────────────────

class SimpleCache:
    """Thread-safe in-process TTL cache. Fallback when Redis is not available."""

    def __init__(self, default_ttl: int = 3600):
        self._store: dict[str, tuple[Any, float]] = {}
        self.default_ttl = default_ttl

    def get(self, key: str) -> Any | None:
        if key not in self._store:
            return None
        value, expires_at = self._store[key]
        if time.time() > expires_at:
            del self._store[key]
            return None
        return value

    def set(self, key: str, value: Any, ttl: int | None = None) -> None:
        ttl = ttl or self.default_ttl
        self._store[key] = (value, time.time() + ttl)

    def delete(self, key: str) -> None:
        self._store.pop(key, None)

    def clear(self) -> None:
        self._store.clear()

    def make_key(self, *args, **kwargs) -> str:
        """Generate a deterministic cache key from arguments."""
        raw = json.dumps({"args": args, "kwargs": kwargs}, sort_keys=True, default=str)
        return hashlib.md5(raw.encode()).hexdigest()


# Module-level singleton
_cache = SimpleCache(default_ttl=int(os.getenv("CACHE_TTL", "3600")))


def cached(ttl: int | None = None):
    """Decorator: cache function result by arguments."""
    def decorator(fn: Callable) -> Callable:
        @wraps(fn)
        def wrapper(*args, **kwargs):
            key = _cache.make_key(fn.__qualname__, *args, **kwargs)
            result = _cache.get(key)
            if result is not None:
                logger.debug(f"Cache hit: {fn.__qualname__}")
                return result
            result = fn(*args, **kwargs)
            _cache.set(key, result, ttl)
            return result
        return wrapper
    return decorator


# ──────────────────────────────────────────────
# Retry decorator
# ──────────────────────────────────────────────

def retry(max_attempts: int = 3, delay: float = 1.0, exceptions: tuple = (Exception,)):
    """Decorator: retry a function on failure with exponential backoff."""
    def decorator(fn: Callable) -> Callable:
        @wraps(fn)
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(max_attempts):
                try:
                    return fn(*args, **kwargs)
                except exceptions as e:
                    last_exc = e
                    wait = delay * (2 ** attempt)
                    logger.warning(
                        f"{fn.__qualname__} attempt {attempt + 1}/{max_attempts} failed: {e}. "
                        f"Retrying in {wait:.1f}s..."
                    )
                    time.sleep(wait)
            raise last_exc
        return wrapper
    return decorator


# ──────────────────────────────────────────────
# SQL helpers
# ──────────────────────────────────────────────

def sanitize_sql_string(value: str) -> str:
    """Basic SQL injection prevention for string values."""
    return value.replace("'", "''").replace(";", "").replace("--", "")


def build_filter_clause(
    country: str | None = None,
    city: str | None = None,
    zone_type: str | None = None,
    prioritization: str | None = None,
    metric: str | None = None,
) -> str:
    """Build a WHERE clause from optional filter parameters."""
    conditions = []
    if country:
        conditions.append(f"COUNTRY = '{sanitize_sql_string(country)}'")
    if city:
        conditions.append(f"CITY = '{sanitize_sql_string(city)}'")
    if zone_type:
        conditions.append(f"ZONE_TYPE = '{sanitize_sql_string(zone_type)}'")
    if prioritization:
        conditions.append(f"ZONE_PRIORITIZATION = '{sanitize_sql_string(prioritization)}'")
    if metric:
        conditions.append(f"METRIC = '{sanitize_sql_string(metric)}'")
    return ("WHERE " + " AND ".join(conditions)) if conditions else ""


# ──────────────────────────────────────────────
# Data validation
# ──────────────────────────────────────────────

VALID_COUNTRIES = {"AR", "BR", "CL", "CO", "CR", "EC", "MX", "PE", "UY"}
VALID_ZONE_TYPES = {"Wealthy", "Non Wealthy"}
VALID_PRIORITIZATIONS = {"High Priority", "Prioritized", "Not Prioritized"}

VALID_METRICS = {
    "% PRO Users Who Breakeven",
    "% Restaurants Sessions With Optimal Assortment",
    "Gross Profit UE",
    "Lead Penetration",
    "MLTV Top Verticals Adoption",
    "Non-Pro PTC > OP",
    "Perfect Orders",
    "Pro Adoption (Last Week Status)",
    "Restaurants Markdowns / GMV",
    "Restaurants SS > ATC CVR",
    "Restaurants SST > SS CVR",
    "Retail SST > SS CVR",
    "Turbo Adoption",
}


def validate_metric(metric: str) -> str | None:
    """Return the metric name if valid, else the closest fuzzy match or None."""
    if metric in VALID_METRICS:
        return metric
    # Fuzzy: find the metric whose name contains the query string (case-insensitive)
    lower = metric.lower()
    for m in VALID_METRICS:
        if lower in m.lower() or m.lower() in lower:
            return m
    return None


def validate_country(country: str) -> str | None:
    upper = country.strip().upper()
    return upper if upper in VALID_COUNTRIES else None
