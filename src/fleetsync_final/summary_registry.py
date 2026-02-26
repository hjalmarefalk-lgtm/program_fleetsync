"""Phase 3 summary registry: deterministic artifacts; do not mutate inputs in-place."""

from __future__ import annotations

from typing import Callable, Dict, TYPE_CHECKING, List, Tuple

import pandas as pd

from .artifacts import TokenArtifact
from .models import SummarySpec, SummaryMetric
from .models import ValidationIssue
from .validation import issue
from .naming_columns import (
    build_summary_metric_col,
    make_unique_against,
    normalize_col_segment,
)

if TYPE_CHECKING:
    import pandas as pd

SummaryHandler = Callable[
    ["pd.DataFrame", SummarySpec, list[TokenArtifact], str | None],
    Tuple[Dict[str, "pd.DataFrame"], List[ValidationIssue]],
]

SUMMARY_REGISTRY: Dict[str, SummaryHandler] = {}
_COUNT_ROWS_HEADER = "Antal"


def _ensure_unique_output_columns(table: "pd.DataFrame") -> "pd.DataFrame":
    """Ensure deterministic uniqueness of output headers without reordering.

    Policy: preserve the first occurrence of a name; suffix later collisions.
    """
    unique_cols = make_unique_against(existing=set(), proposed=list(table.columns))
    if unique_cols != list(table.columns):
        table = table.copy()
        table.columns = unique_cols
    return table


def register_summary(summary_type: str) -> Callable[[SummaryHandler], SummaryHandler]:
    """Decorator to register a summary handler by summary_type."""

    def decorator(func: SummaryHandler) -> SummaryHandler:
        SUMMARY_REGISTRY[summary_type] = func
        return func

    return decorator


def get_summary_registry() -> Dict[str, SummaryHandler]:
    """Return the summary registry mapping."""
    return dict(SUMMARY_REGISTRY)


def _metric_col_name(metric: SummaryMetric) -> str:
    if metric.op == "count_rows":
        return _COUNT_ROWS_HEADER
    return build_summary_metric_col(metric.op, metric.col)


def _token_col_to_value_mapping(tokens: list[TokenArtifact]) -> dict[str, str]:
    """Build a produced token column -> token_value mapping from token artifacts.

    Lock: mapping-driven only (no heuristics). Only presence_columns artifacts that
    include both 'token_column' and 'token_value' are considered.
    """

    mapping: dict[str, str] = {}
    for artifact in tokens:
        data = artifact.data
        if not isinstance(data, pd.DataFrame) or data.empty:
            continue
        if "token_column" not in data.columns or "token_value" not in data.columns:
            continue
        for _, row in data[["token_column", "token_value"]].iterrows():
            col = row.get("token_column")
            value = row.get("token_value")
            if isinstance(col, str) and col and isinstance(value, str):
                # Deterministic: first seen wins for a given produced column.
                mapping.setdefault(col, value)
    return mapping


def _metric_col_name_group_aggregate(metric: SummaryMetric, token_col_to_value: dict[str, str]) -> str:
    """Group_aggregate metric output header naming (Style C).

    Rules (LOCK):
    - count_rows uses display label configured by _COUNT_ROWS_HEADER
    - otherwise op_field, where field is:
      - token value (normalized) if metric.col is proven token-derived via mapping
      - else metric.col (normalized)
    """

    if metric.op == "count_rows":
        return _COUNT_ROWS_HEADER

    op_part = normalize_col_segment(metric.op)
    if metric.col and metric.col in token_col_to_value:
        short_value = normalize_col_segment(token_col_to_value[metric.col])
        return f"{op_part}_{short_value}"

    field_part = normalize_col_segment(metric.col or "")
    return f"{op_part}_{field_part}"


def _empty_group_aggregate_table(group_by: str, metrics: list[SummaryMetric]) -> "pd.DataFrame":
    group_out = normalize_col_segment(group_by)
    columns = [group_out] + [_metric_col_name(metric) for metric in metrics]
    columns = make_unique_against(existing=set(), proposed=columns)
    return pd.DataFrame(columns=columns)


def _sort_grouped_table(table: "pd.DataFrame", group_by: str) -> "pd.DataFrame":
    if group_by not in table.columns or table.empty:
        return table

    series = table[group_by]
    non_null = series.dropna()
    type_count = non_null.map(type).nunique() if not non_null.empty else 0
    if type_count > 1:
        table = table.assign(_sort_key=series.astype(str))
        table = table.sort_values(by="_sort_key", kind="mergesort").drop(columns=["_sort_key"])
    else:
        table = table.sort_values(by=group_by, kind="mergesort")
    return table


def _append_group_aggregate_total_row(table: "pd.DataFrame") -> "pd.DataFrame":
    if table.empty:
        return table

    columns = list(table.columns)
    if not columns:
        return table

    first_col = columns[0]
    row: dict[str, object] = {first_col: "Total"}

    for col in columns[1:]:
        col_name = str(col)
        if col_name == _COUNT_ROWS_HEADER or col_name.startswith("sum_"):
            series = pd.to_numeric(table[col], errors="coerce")
            total_value = float(series.sum())
            row[col] = int(total_value) if float(total_value).is_integer() else total_value
        else:
            row[col] = None

    return pd.concat([table, pd.DataFrame([row], columns=columns)], ignore_index=True)


@register_summary("group_aggregate")
def _summary_group_aggregate(
    df: "pd.DataFrame", spec: SummarySpec, tokens: list[TokenArtifact], sheet_name: str | None
) -> Tuple[Dict[str, "pd.DataFrame"], List[ValidationIssue]]:
    warnings: List[ValidationIssue] = []
    group_by = spec.group_by
    metrics = spec.metrics or []

    if not isinstance(group_by, str) or not group_by:
        warnings.append(
            issue(
                "SUMMARY_MISSING_INPUT",
                "WARNING",
                "group_aggregate missing group_by",
                f"summaries[{spec.summary_id}].group_by",
            )
        )
        empty = _empty_group_aggregate_table("group_by", metrics)
        return {spec.summary_id: empty}, warnings

    group_by_out = normalize_col_segment(group_by)

    if group_by not in df.columns:
        warnings.append(
            issue(
                "SUMMARY_MISSING_COLUMN",
                "WARNING",
                "group_aggregate group_by not found in sheet columns",
                f"summaries[{spec.summary_id}].group_by",
            )
        )
        empty = _empty_group_aggregate_table(group_by, metrics)
        return {spec.summary_id: empty}, warnings

    _ops_require_col = ("sum", "max", "min", "avg", "distinct_count", "null_rate")

    for m_idx, metric in enumerate(metrics):
        if metric.op in _ops_require_col and not metric.col:
            warnings.append(
                issue(
                    "SUMMARY_MISSING_INPUT",
                    "WARNING",
                    "group_aggregate metric col missing for op",
                    f"summaries[{spec.summary_id}].metrics[{m_idx}].col",
                )
            )
        elif metric.op in _ops_require_col and metric.col not in df.columns:
            warnings.append(
                issue(
                    "SUMMARY_MISSING_COLUMN",
                    "WARNING",
                    f"group_aggregate metric column not found: {metric.col}",
                    f"summaries[{spec.summary_id}].metrics[{m_idx}].col",
                )
            )

    if warnings:
        empty = _empty_group_aggregate_table(group_by, metrics)
        return {spec.summary_id: empty}, warnings

    metric_cols: List[str] = []
    for metric in metrics:
        if metric.op in _ops_require_col and metric.col and metric.col not in metric_cols:
            metric_cols.append(metric.col)

    temp = df[[group_by] + metric_cols].copy()
    _numeric_ops = ("sum", "max", "min", "avg")
    coercion_warned: set[str] = set()
    for metric in metrics:
        if metric.op not in _numeric_ops or not metric.col:
            continue
        series = temp[metric.col]
        converted = pd.to_numeric(series, errors="coerce")
        if metric.col not in coercion_warned and (converted.isna() & series.notna()).any():
            warnings.append(
                issue(
                    "SUMMARY_COERCED_NON_NUMERIC",
                    "WARNING",
                    f"group_aggregate coerced non-numeric values in {metric.col}",
                    f"summaries[{spec.summary_id}].metrics",
                )
            )
            coercion_warned.add(metric.col)
        temp[metric.col] = converted

    grouped = temp.groupby(group_by, dropna=False)

    token_col_to_value = _token_col_to_value_mapping(tokens)
    metric_columns = [_metric_col_name_group_aggregate(metric, token_col_to_value) for metric in metrics]

    # Critical: metric output names must not collide with the group-by source column name,
    # or pandas will fail during reset_index / column insertion.
    metric_columns_unique = make_unique_against(existing={group_by}, proposed=metric_columns, max_len=64)

    def _null_rate(series: "pd.Series") -> float:
        # Fraction of nulls in the group for this column. Deterministic given the input.
        # Returns float in [0, 1].
        try:
            return float(series.isna().mean())
        except Exception:
            return float("nan")

    agg_spec: Dict[str, tuple] = {}
    count_col_name: str | None = None
    for metric, out_name in zip(metrics, metric_columns_unique):
        if metric.op == "count_rows":
            # group_aggregate produces at most one count column (matches prior behavior).
            if count_col_name is None:
                count_col_name = out_name
            continue

        if metric.op in ("sum", "max", "min"):
            agg_spec[out_name] = (metric.col, metric.op)
        elif metric.op == "avg":
            agg_spec[out_name] = (metric.col, "mean")
        elif metric.op == "distinct_count":
            agg_spec[out_name] = (metric.col, "nunique")
        elif metric.op == "null_rate":
            agg_spec[out_name] = (metric.col, _null_rate)
        else:
            # Unknown ops should never reach this point due to schema validation.
            warnings.append(
                issue(
                    "SUMMARY_UNKNOWN_OP",
                    "WARNING",
                    f"group_aggregate unknown metric op: {metric.op}",
                    f"summaries[{spec.summary_id}].metrics",
                )
            )

    result = None
    if agg_spec:
        result = grouped.agg(**agg_spec).reset_index()

    if count_col_name is not None:
        counts = grouped.size().reset_index(name=_COUNT_ROWS_HEADER)
        if count_col_name != _COUNT_ROWS_HEADER:
            counts = counts.rename(columns={_COUNT_ROWS_HEADER: count_col_name})
        if result is None:
            result = counts
        else:
            result = result.merge(counts, on=group_by, how="left")

    if result is None:
        result = pd.DataFrame(columns=[group_by] + metric_columns)
    else:
        result = _sort_grouped_table(result, group_by)
        result = result.reindex(columns=[group_by] + metric_columns_unique)

    # Rename group-by header to Style C normalized form.
    if group_by in result.columns:
        result = result.rename(columns={group_by: group_by_out})

    # Ensure deterministic uniqueness if any names collide after normalization.
    result = _ensure_unique_output_columns(result)

    if spec.scope in ("workbook", "sheet"):
        result = _append_group_aggregate_total_row(result)

    return {spec.summary_id: result}, warnings


@register_summary("trips_per_day")
def _summary_trips_per_day(
    df: "pd.DataFrame", spec: SummarySpec, tokens: list[TokenArtifact], sheet_name: str | None
) -> Tuple[Dict[str, "pd.DataFrame"], List[ValidationIssue]]:
    date_col = spec.inputs.get("date_col")
    warnings: List[ValidationIssue] = []
    if not isinstance(date_col, str) or not date_col:
        warnings.append(
            issue(
                "SUMMARY_MISSING_INPUT",
                "WARNING",
                "trips_per_day missing inputs.date_col",
                f"summaries[{spec.summary_id}].inputs.date_col",
            )
        )
        empty = pd.DataFrame(columns=["date_col", _COUNT_ROWS_HEADER])
        return {spec.summary_id: empty}, warnings
    if date_col not in df.columns:
        warnings.append(
            issue(
                "SUMMARY_MISSING_COLUMN",
                "WARNING",
                "trips_per_day date_col not found in sheet columns",
                f"summaries[{spec.summary_id}].inputs.date_col",
            )
        )
        empty = pd.DataFrame(columns=[normalize_col_segment(date_col), _COUNT_ROWS_HEADER])
        return {spec.summary_id: empty}, warnings
    grouped = df.groupby(date_col, dropna=False).size().reset_index(name=_COUNT_ROWS_HEADER)
    grouped = grouped.sort_values(by=date_col, kind="mergesort")

    grouped = grouped.rename(columns={date_col: normalize_col_segment(date_col)})
    grouped = _ensure_unique_output_columns(grouped)
    return {spec.summary_id: grouped}, warnings


@register_summary("grouped_sum")
def _summary_grouped_sum(
    df: "pd.DataFrame", spec: SummarySpec, tokens: list[TokenArtifact], sheet_name: str | None
) -> Tuple[Dict[str, "pd.DataFrame"], List[ValidationIssue]]:
    group_col = spec.inputs.get("group_col")
    value_col = spec.inputs.get("value_col")
    warnings: List[ValidationIssue] = []
    if not isinstance(group_col, str) or not group_col:
        warnings.append(
            issue(
                "SUMMARY_MISSING_INPUT",
                "WARNING",
                "grouped_sum missing inputs.group_col",
                f"summaries[{spec.summary_id}].inputs.group_col",
            )
        )
        empty = pd.DataFrame(columns=["group_col", build_summary_metric_col("sum", "value_col")])
        return {spec.summary_id: empty}, warnings
    if not isinstance(value_col, str) or not value_col:
        warnings.append(
            issue(
                "SUMMARY_MISSING_INPUT",
                "WARNING",
                "grouped_sum missing inputs.value_col",
                f"summaries[{spec.summary_id}].inputs.value_col",
            )
        )
        group_out = normalize_col_segment(group_col) if isinstance(group_col, str) and group_col else "group_col"
        empty = pd.DataFrame(columns=[group_out, build_summary_metric_col("sum", "value_col")])
        return {spec.summary_id: empty}, warnings
    if group_col not in df.columns:
        warnings.append(
            issue(
                "SUMMARY_MISSING_COLUMN",
                "WARNING",
                "grouped_sum group_col not found in sheet columns",
                f"summaries[{spec.summary_id}].inputs.group_col",
            )
        )
        empty = pd.DataFrame(columns=[normalize_col_segment(group_col), build_summary_metric_col("sum", value_col)])
        return {spec.summary_id: empty}, warnings
    if value_col not in df.columns:
        warnings.append(
            issue(
                "SUMMARY_MISSING_COLUMN",
                "WARNING",
                "grouped_sum value_col not found in sheet columns",
                f"summaries[{spec.summary_id}].inputs.value_col",
            )
        )
        empty = pd.DataFrame(columns=[normalize_col_segment(group_col), build_summary_metric_col("sum", value_col)])
        return {spec.summary_id: empty}, warnings

    values = pd.to_numeric(df[value_col], errors="coerce").fillna(0)
    grouped = df.assign(_value=values).groupby(group_col, dropna=False)["_value"].sum().reset_index()
    group_out = normalize_col_segment(group_col)
    metric_out = build_summary_metric_col("sum", value_col)
    grouped = grouped.rename(columns={group_col: group_out, "_value": metric_out})

    # Must dedupe headers before sorting; pandas cannot sort with non-unique labels.
    grouped = _ensure_unique_output_columns(grouped)
    sort_key = grouped.columns[0]
    grouped = grouped.sort_values(by=sort_key, kind="mergesort")
    return {spec.summary_id: grouped}, warnings


@register_summary("basic_counts")
def _summary_basic_counts(
    df: "pd.DataFrame", spec: SummarySpec, tokens: list[TokenArtifact], sheet_name: str | None
) -> Tuple[Dict[str, "pd.DataFrame"], List[ValidationIssue]]:
    distinct_cols = spec.inputs.get("distinct_cols", [])
    warnings: List[ValidationIssue] = []
    if distinct_cols is None:
        distinct_cols = []
    if not isinstance(distinct_cols, list):
        warnings.append(
            issue(
                "SUMMARY_MISSING_INPUT",
                "WARNING",
                "basic_counts inputs.distinct_cols must be list[str] if provided",
                f"summaries[{spec.summary_id}].inputs.distinct_cols",
            )
        )
        distinct_cols = []

    row = {"row_count": int(len(df))}
    for col in distinct_cols:
        if isinstance(col, str) and col:
            if col in df.columns:
                row[f"distinct_{normalize_col_segment(col)}"] = int(df[col].nunique(dropna=False))
            else:
                warnings.append(
                    issue(
                        "SUMMARY_MISSING_COLUMN",
                        "WARNING",
                        f"basic_counts distinct column not found: {col}",
                        f"summaries[{spec.summary_id}].inputs.distinct_cols",
                    )
                )
    data = pd.DataFrame([row])
    data = _ensure_unique_output_columns(data)
    return {spec.summary_id: data}, warnings
