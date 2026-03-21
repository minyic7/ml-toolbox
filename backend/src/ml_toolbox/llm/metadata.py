"""Heuristic column profiling and metadata generation for parquet outputs."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

# ── Numeric dtype detection ───────────────────────────────────────────

NUMERIC_DTYPE_STRINGS = {
    "int8", "int16", "int32", "int64",
    "uint8", "uint16", "uint32", "uint64",
    "float16", "float32", "float64",
    # Pandas nullable types (capital letter)
    "Int8", "Int16", "Int32", "Int64",
    "UInt8", "UInt16", "UInt32", "UInt64",
    "Float32", "Float64",
}


def _is_numeric_dtype(series: pd.Series) -> bool:  # type: ignore[type-arg]
    """Check if a series has a numeric dtype, including pandas nullable types."""
    return str(series.dtype) in NUMERIC_DTYPE_STRINGS or pd.api.types.is_numeric_dtype(series)


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
        if _is_numeric_dtype(series):
            non_null = series.dropna()
            if len(non_null) > 0:
                profile["min"] = _safe_scalar(non_null.min())
                profile["max"] = _safe_scalar(non_null.max())

        # Collect all unique values when cardinality is low
        if unique_count <= 20:
            profile["all_values"] = sorted(
                _safe_scalar(v) for v in series.dropna().unique()
            )

        # Unknown/unsupported dtypes (bytes, nested structs, etc.)
        if (
            not _is_numeric_dtype(series)
            and series.dtype != object
            and str(series.dtype) not in ("bool", "category")
            and "datetime" not in dtype_str
        ):
            profile["heuristic_guess"] = {
                "semantic_type": "unknown",
                "role": "ignore",
                "confidence": 0.0,
            }
        else:
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

    # 1. Binary + target name → possible target (confidence 0.7 signals guess)
    if name_lower in _TARGET_NAMES and (dtype == "bool" or unique_count == 2):
        return {"semantic_type": "binary", "role": "target", "confidence": 0.7}

    # 2. Target detection by name (non-binary)
    if name_lower in _TARGET_NAMES:
        return {"semantic_type": "target", "role": "target", "confidence": 0.85}

    # 3. Datetime
    if "datetime" in dtype or any(
        kw in name_lower for kw in ("date", "time", "timestamp")
    ):
        return {"semantic_type": "datetime", "role": "metadata", "confidence": 0.90}

    # 4. Boolean / binary
    if dtype == "bool" or unique_count == 2:
        return {"semantic_type": "binary", "role": "feature", "confidence": 0.85}

    # 5. String / object columns
    if dtype in ("object", "string", "str", "category"):
        if unique_ratio > 0.9:
            return {"semantic_type": "identifier", "role": "identifier", "confidence": 0.75}
        return {"semantic_type": "categorical", "role": "feature", "confidence": 0.80}

    # 6. Numeric columns (including nullable Int64, Float64, UInt32, etc.)
    if dtype in NUMERIC_DTYPE_STRINGS or "int" in dtype or "float" in dtype:
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
    name = p["name"]
    dtype = p["dtype"]
    sem = guess["semantic_type"]
    role = guess["role"]
    unique = p["unique_count"]

    # Possible target: binary + name matches target keyword
    if sem == "binary" and role == "target":
        values = p.get("all_values", p.get("sample_values", []))
        return (
            f"{name}: {dtype}, binary values: {values}, "
            f"name matches target keyword \u2192 possible target, please confirm"
        )

    if sem == "binary":
        values = p.get("all_values", p.get("sample_values", []))
        return f"{name}: {dtype}, binary values: {values} \u2192 binary {role}"

    if sem in ("categorical", "categorical_or_ordinal", "ordinal"):
        if unique <= 20:
            values = sorted(p.get("all_values", p.get("sample_values", [])))
            return f"{name}: {dtype}, {unique} values: {values} \u2192 {sem} {role}"
        return f"{name}: {dtype}, {unique} unique values \u2192 {sem} {role}"

    if sem == "continuous":
        mn = p.get("min", "?")
        mx = p.get("max", "?")
        return (
            f"{name}: {dtype}, {unique} unique values, "
            f"range {mn}\u2013{mx} \u2192 continuous {role}"
        )

    if sem in ("identifier", "id"):
        ratio = p.get("unique_ratio", 0)
        return (
            f"{name}: {dtype}, {unique} unique "
            f"({ratio:.0%} of rows) \u2192 identifier, suggest ignore"
        )

    if sem == "unknown":
        return f"{name}: {dtype}, unsupported dtype \u2192 unknown, suggest ignore"

    # Fallback (datetime, target-by-name-only, etc.)
    return f"{name}: {dtype}, {unique} unique \u2192 {sem} {role}"


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


# ── Column casting ────────────────────────────────────────────────────


def cast_by_metadata(
    df: pd.DataFrame, metadata: dict[str, Any],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Cast columns based on inferred semantic types.

    Returns ``(df, results)`` where *results* maps column names to a dict
    with ``status``, optional ``to``, ``success_rate``, ``reason``, and
    ``kept_dtype`` fields.
    """
    columns_meta = metadata.get("columns", {})
    results: dict[str, Any] = {}

    for col_name, meta in columns_meta.items():
        if col_name not in df.columns:
            continue
        if df[col_name].dtype != object:  # already typed, skip
            results[col_name] = {"status": "skipped", "reason": "already typed"}
            continue

        sem_type = meta.get("semantic_type", "")

        if sem_type in ("continuous", "binary"):
            numeric = pd.to_numeric(df[col_name], errors="coerce")
            col_series = pd.Series(df[col_name])
            numeric_series = pd.Series(numeric)
            non_null = int(col_series.notna().sum())
            success_rate = float(numeric_series.notna().sum()) / max(non_null, 1)

            if success_rate >= 0.9:
                df[col_name] = numeric
                results[col_name] = {
                    "status": "cast",
                    "to": "numeric",
                    "success_rate": round(success_rate, 4),
                }
            else:
                results[col_name] = {
                    "status": "failed",
                    "reason": f"Only {success_rate:.0%} of values are numeric",
                    "kept_dtype": str(df[col_name].dtype),
                }
        else:
            # categorical, ordinal, datetime, id, text → keep as string
            results[col_name] = {
                "status": "skipped",
                "reason": f"semantic_type={sem_type}, keep as string",
            }

    return df, results
