"""Heuristic column profiling and metadata generation for parquet outputs."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


def heuristic_profile(df: pd.DataFrame) -> list[dict[str, Any]]:
    """Profile each column in *df* and return heuristic classification guesses.

    Returns a list of dicts, one per column, with stats and a ``heuristic_guess``
    sub-dict containing ``semantic_type``, ``role``, and ``confidence``.
    """
    row_count = len(df)
    profiles: list[dict[str, Any]] = []

    for col in df.columns:
        series = pd.Series(df[col])
        dtype_str = str(series.dtype)
        unique_count = int(series.nunique())
        unique_ratio = unique_count / row_count if row_count > 0 else 0.0
        null_pct = float(series.isna().mean())

        profile: dict[str, Any] = {
            "name": col,
            "dtype": dtype_str,
            "unique_count": unique_count,
            "unique_ratio": unique_ratio,
            "null_pct": null_pct,
            "sample_values": _sample_values(series),
        }

        # Numeric range stats
        if pd.api.types.is_numeric_dtype(series):
            non_null = series.dropna()
            if len(non_null) > 0:
                profile["min"] = _safe_scalar(non_null.min())
                profile["max"] = _safe_scalar(non_null.max())

        # Flag likely-encoded integers
        if pd.api.types.is_integer_dtype(series) and unique_count <= 20:
            profile["likely_encoded"] = True

        profile["heuristic_guess"] = _classify(col, profile)
        profiles.append(profile)

    return profiles


# ── Classification heuristics ──────────────────────────────────────────


_TARGET_NAMES = {"target", "label", "class", "y", "outcome"}


def _classify(col_name: str, p: dict[str, Any]) -> dict[str, Any]:
    """Return ``semantic_type``, ``role``, and ``confidence`` for a column."""
    dtype = p["dtype"]
    unique_count = p["unique_count"]
    unique_ratio = p["unique_ratio"]
    name_lower = col_name.lower().strip()

    # 1. Target detection by name
    if name_lower in _TARGET_NAMES:
        return {"semantic_type": "target", "role": "target", "confidence": 0.85}

    # 2. Datetime
    if "datetime" in dtype or any(
        kw in name_lower for kw in ("date", "time", "timestamp")
    ):
        return {"semantic_type": "datetime", "role": "metadata", "confidence": 0.90}

    # 3. Boolean / binary
    if dtype == "bool" or unique_count == 2:
        return {"semantic_type": "binary", "role": "feature", "confidence": 0.85}

    # 4. String / object columns
    if dtype in ("object", "string", "str", "category"):
        if unique_ratio > 0.9:
            return {"semantic_type": "identifier", "role": "identifier", "confidence": 0.75}
        return {"semantic_type": "categorical", "role": "feature", "confidence": 0.80}

    # 5. Numeric columns
    if "int" in dtype or "float" in dtype:
        if unique_count <= 15:
            return {"semantic_type": "categorical", "role": "feature", "confidence": 0.70}
        return {"semantic_type": "continuous", "role": "feature", "confidence": 0.80}

    # Fallback
    return {"semantic_type": "continuous", "role": "feature", "confidence": 0.50}


# ── Metadata builder ───────────────────────────────────────────────────


def build_metadata_from_heuristics(
    profiles: list[dict[str, Any]],
    *,
    row_count: int,
    node_id: str,
) -> dict[str, Any]:
    """Build a ``.meta.json``-compatible dict from heuristic profiles (no LLM)."""
    columns: dict[str, Any] = {}
    for p in profiles:
        guess = p["heuristic_guess"]
        columns[p["name"]] = {
            "dtype": p["dtype"],
            "semantic_type": guess["semantic_type"],
            "role": guess["role"],
            "nullable": p["null_pct"] > 0,
            "unique_count": p["unique_count"],
            "unique_ratio": round(p["unique_ratio"], 4),
            "null_pct": round(p["null_pct"], 4),
            "sample_values": p.get("sample_values", [])[:5],
            "reasoning": _build_reasoning(p, guess),
        }
    return {
        "columns": columns,
        "row_count": row_count,
        "generated_by": "auto-heuristic",
        "node_id": node_id,
    }


def _build_reasoning(p: dict[str, Any], guess: dict[str, Any]) -> str:
    """Build a human-readable reasoning string for a classification."""
    parts = [f"dtype={p['dtype']}, {p['unique_count']} unique ({p['unique_ratio']:.1%})"]
    if p["null_pct"] > 0:
        parts.append(f"{p['null_pct']:.1%} null")
    if "min" in p:
        parts.append(f"range=[{p['min']}, {p['max']}]")
    if p.get("likely_encoded"):
        parts.append("integer with <=20 unique values — likely encoded")
    sem = guess["semantic_type"]
    role = guess["role"]
    parts.append(f"→ {sem} ({role})")
    return "; ".join(parts)


# ── Helpers ────────────────────────────────────────────────────────────


def _sample_values(series: pd.Series, n: int = 5) -> list:
    """Return up to *n* representative non-null values as native Python types."""
    non_null = series.dropna()
    if len(non_null) == 0:
        return []
    sample = non_null.drop_duplicates().head(n)
    return [_safe_scalar(v) for v in sample]


def _safe_scalar(val: Any) -> Any:
    """Convert numpy/pandas scalar to native Python type."""
    try:
        return val.item()
    except (AttributeError, ValueError):
        return val
