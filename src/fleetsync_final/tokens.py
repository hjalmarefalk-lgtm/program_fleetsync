"""Phase 3 tokens: deterministic tokenization with optional output columns."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, Iterable
import re

import pandas as pd

from .artifacts import TokenArtifact, assert_no_df_mutation
from .models import TokenSpec
from .models import ValidationIssue, ValidationReport
from .validation import issue, compute_valid
from .naming_columns import make_unique_against, normalize_col_segment

TokenHandler = Callable[[pd.DataFrame, TokenSpec, str | None], TokenArtifact]

_TOKEN_COL_MAX_LEN = 64


@dataclass(frozen=True)
class TokenColumnSpec:
    """Deterministic token column contract for a single token spec."""

    token_id: str
    produced_cols: list[str]


def _presence_base_name(source_col: str | None, value_label: str) -> str:
    source_part = normalize_col_segment(source_col or "")
    value_part = normalize_col_segment(value_label)
    return f"{source_part}_{value_part}"


def predict_token_output_columns(
    token_specs: list[TokenSpec], existing_columns: Iterable[str] | None = None
) -> list[str]:
    """Predict token output column headers without loading data (metadata-only).

    This is used by semantic validation to avoid false warnings when summaries
    reference token-derived columns.
    """
    used: set[str] = set(map(str, existing_columns or []))
    produced: list[str] = []
    for spec in token_specs:
        if spec.token_type != "presence_columns":
            continue
        inputs = spec.inputs or {}
        source_col = inputs.get("source_col")
        values = inputs.get("values")
        if not isinstance(source_col, str) or not source_col:
            continue
        if not isinstance(values, list) or not all(isinstance(v, str) for v in values):
            continue
        for value in values:
            base = _presence_base_name(source_col, value)
            unique = make_unique_against(existing=used, proposed=[base], max_len=_TOKEN_COL_MAX_LEN)[0]
            used.add(unique)
            produced.append(unique)

    return produced


def apply_token_columns(
    df: pd.DataFrame, token_specs: list[TokenSpec]
) -> tuple[pd.DataFrame, list[ValidationIssue], list[str], list[TokenColumnSpec], dict[str, str]]:
    """Apply token specs as output columns (Total + split sheets by default).

    Contract:
    - Column naming is deterministic via <normalized source_col>_<normalized value>.
    - Token output columns never overwrite existing columns.
    - Missing source columns yield WARNINGs; columns still exist with zero values.
    - Order is stable: token_specs order, then each token's produced_cols order.
    """
    warnings: list[ValidationIssue] = []
    out_df = df.copy()
    produced_cols: list[str] = []
    produced_specs: list[TokenColumnSpec] = []
    produced_col_to_value: dict[str, str] = {}

    used: set[str] = set(map(str, out_df.columns))

    for spec in token_specs:
        if spec.token_type != "presence_columns":
            continue

        inputs = spec.inputs or {}
        source_col = inputs.get("source_col")
        values = inputs.get("values")
        separator = inputs.get("separator")

        if separator is not None and not isinstance(separator, str):
            warnings.append(
                issue(
                    "TOKEN_COL_004",
                    "WARNING",
                    "presence_columns separator must be a string",
                    f"tokens[{spec.token_id}].inputs.separator",
                )
            )
            separator = None

        if not isinstance(values, list) or not all(isinstance(v, str) for v in values):
            warnings.append(
                issue(
                    "TOKEN_COL_001",
                    "WARNING",
                    "presence_columns missing inputs.values",
                    f"tokens[{spec.token_id}].inputs.values",
                )
            )
            values_list: list[str] = []
        else:
            values_list = values

        if not isinstance(source_col, str) or not source_col:
            warnings.append(
                issue(
                    "TOKEN_COL_002",
                    "WARNING",
                    "presence_columns missing inputs.source_col",
                    f"tokens[{spec.token_id}].inputs.source_col",
                )
            )
            source_col = None

        col_names: list[str] = []
        for value in values_list:
            base = _presence_base_name(source_col, value)
            col_name = make_unique_against(existing=used, proposed=[base], max_len=_TOKEN_COL_MAX_LEN)[0]
            used.add(col_name)
            col_names.append(col_name)
            produced_col_to_value[col_name] = value

        if source_col and source_col in out_df.columns:
            series = out_df[source_col].fillna("").astype(str)
            if separator:
                split_series = series.str.split(separator)
            else:
                split_series = series.str.split(r"\s+")

            for value, col_name in zip(values_list, col_names):
                out_df[col_name] = split_series.apply(lambda parts: 1 if value in parts else 0)
        else:
            if source_col:
                warnings.append(
                    issue(
                        "TOKEN_COL_003",
                        "WARNING",
                        "presence_columns source_col not found in sheet columns",
                        f"tokens[{spec.token_id}].inputs.source_col",
                    )
                )
            for col_name in col_names:
                out_df[col_name] = 0

        produced_cols.extend(col_names)
        produced_specs.append(TokenColumnSpec(token_id=spec.token_id, produced_cols=col_names))

    return out_df, warnings, produced_cols, produced_specs, produced_col_to_value

TOKEN_REGISTRY: Dict[str, TokenHandler] = {}


def _token_row_count(df: pd.DataFrame, spec: TokenSpec, sheet_name: str | None) -> TokenArtifact:
    data = pd.DataFrame([{"metric": "row_count", "value": int(len(df))}])
    return TokenArtifact(token_id=spec.token_id, scope=spec.scope, data=data)


def _token_presence_columns_artifact(
    df: pd.DataFrame, spec: TokenSpec, sheet_name: str | None
) -> TokenArtifact:
    inputs = spec.inputs or {}
    values = inputs.get("values")
    if not isinstance(values, list) or not all(isinstance(v, str) for v in values):
        empty = pd.DataFrame(columns=["token_column", "token_value", "count"])
        return TokenArtifact(token_id=spec.token_id, scope=spec.scope, data=empty)

    # Legacy implementation recomputed names; that becomes incorrect once collision
    # suffixing is enabled. The execution pipeline should supply produced column
    # names via run_tokens(..., token_column_specs=...).
    empty = pd.DataFrame(columns=["token_column", "token_value", "count"])
    return TokenArtifact(token_id=spec.token_id, scope=spec.scope, data=empty)


def _iter_text_parts(value: object, delimiter: str | None, pattern: str | None) -> Iterable[str]:
    if value is None:
        return []
    text = str(value)
    if pattern:
        parts = re.split(pattern, text)
    else:
        parts = text.split(delimiter or ",")
    return [part.strip() for part in parts if part.strip()]


def _token_text_split_count(df: pd.DataFrame, spec: TokenSpec, sheet_name: str | None) -> TokenArtifact:
    source_col = spec.inputs.get("source_col")
    allowed_values = spec.inputs.get("allowed_values")
    delimiter = spec.inputs.get("delimiter")
    pattern = spec.inputs.get("pattern")

    if not isinstance(source_col, str) or not source_col:
        empty = pd.DataFrame(columns=["token_value", "count"])
        return TokenArtifact(token_id=spec.token_id, scope=spec.scope, data=empty)
    if not isinstance(allowed_values, list) or not all(isinstance(v, str) for v in allowed_values):
        empty = pd.DataFrame(columns=["token_value", "count"])
        return TokenArtifact(token_id=spec.token_id, scope=spec.scope, data=empty)

    counts = {value: 0 for value in allowed_values}
    series = df[source_col] if source_col in df.columns else pd.Series([], dtype="object")
    for value in series.fillna("").astype(str):
        for part in _iter_text_parts(value, delimiter, pattern):
            if part in counts:
                counts[part] += 1

    rows = [{"token_value": value, "count": counts[value]} for value in sorted(counts.keys())]
    data = pd.DataFrame(rows)
    return TokenArtifact(token_id=spec.token_id, scope=spec.scope, data=data)


TOKEN_REGISTRY = {
    "presence_columns": _token_presence_columns_artifact,
    "row_count": _token_row_count,
    "text_split_count": _token_text_split_count,
}


def validate_token_specs(token_specs: list[TokenSpec], df_columns: list[str]) -> ValidationReport:
    """Validate token specs for missing inputs; return warnings only."""
    warnings: list[ValidationIssue] = []
    column_set = set(df_columns)

    for spec in token_specs:
        if spec.token_type == "text_split_count":
            source_col = spec.inputs.get("source_col")
            allowed_values = spec.inputs.get("allowed_values")

            if not isinstance(source_col, str) or not source_col:
                warnings.append(
                    issue(
                        "TOKEN_001",
                        "WARNING",
                        "text_split_count missing inputs.source_col",
                        f"tokens[{spec.token_id}].inputs.source_col",
                    )
                )
            elif source_col not in column_set:
                warnings.append(
                    issue(
                        "TOKEN_002",
                        "WARNING",
                        "text_split_count source_col not found in sheet columns",
                        f"tokens[{spec.token_id}].inputs.source_col",
                    )
                )

            if not isinstance(allowed_values, list) or not all(isinstance(v, str) for v in allowed_values):
                warnings.append(
                    issue(
                        "TOKEN_003",
                        "WARNING",
                        "text_split_count missing inputs.allowed_values",
                        f"tokens[{spec.token_id}].inputs.allowed_values",
                    )
                )

    return ValidationReport(valid=compute_valid(warnings, []), warnings=warnings, fatals=[])


def run_tokens(
    df: pd.DataFrame,
    token_specs: list[TokenSpec],
    scope: str,
    sheet_name: str | None = None,
    token_column_specs: list[TokenColumnSpec] | None = None,
) -> list[TokenArtifact]:
    """Run token specs to produce artifacts (does not add output columns)."""
    before_df = df.copy(deep=False)
    results: list[TokenArtifact] = []

    cols_by_token_id: dict[str, list[str]] = {}
    if token_column_specs:
        cols_by_token_id = {s.token_id: list(s.produced_cols) for s in token_column_specs}

    for spec in token_specs:
        if spec.scope != scope:
            continue

        if spec.token_type == "presence_columns":
            produced_cols = cols_by_token_id.get(spec.token_id)
            inputs = spec.inputs or {}
            values = inputs.get("values")
            if not isinstance(values, list) or not all(isinstance(v, str) for v in values):
                empty = pd.DataFrame(columns=["token_column", "token_value", "count"])
                artifact = TokenArtifact(token_id=spec.token_id, scope=spec.scope, data=empty)
            elif not produced_cols or len(produced_cols) != len(values):
                empty = pd.DataFrame(columns=["token_column", "token_value", "count"])
                artifact = TokenArtifact(token_id=spec.token_id, scope=spec.scope, data=empty)
            else:
                rows: list[dict[str, object]] = []
                for value, col_name in zip(values, produced_cols):
                    if col_name in df.columns:
                        count = int(pd.to_numeric(df[col_name], errors="coerce").fillna(0).sum())
                    else:
                        count = 0
                    rows.append({"token_column": col_name, "token_value": value, "count": count})
                artifact = TokenArtifact(token_id=spec.token_id, scope=spec.scope, data=pd.DataFrame(rows))
        else:
            handler = TOKEN_REGISTRY.get(spec.token_type)
            if handler is None:
                raise ValueError(f"Unknown token_type: {spec.token_type}")
            artifact = handler(df, spec, sheet_name)

        results.append(artifact)
        assert_no_df_mutation(before_df, df, f"token {spec.token_id}")

    return results
