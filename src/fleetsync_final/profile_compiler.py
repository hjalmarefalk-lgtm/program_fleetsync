"""Single canonical profile->models compiler. UI + scripts must use this."""

from __future__ import annotations

from typing import Any

from .models import (
    ColumnPolicy,
    ContractFilterSpec,
    JobSpec,
    RowDropRule,
    RowOrderSpec,
    SplitSpec,
    SummaryMetric,
    SummarySpec,
    TokenSpec,
    WorkbookSpec,
)


def _sanitize_id_component(value: str) -> str:
    cleaned = value.strip().replace("/", "_").replace("\\", "_").replace(" ", "_")
    return cleaned or "none"


def _build_summary_id(scope: str, group_by: str | None, metrics: list[SummaryMetric]) -> str:
    group_part = _sanitize_id_component(group_by or "none")
    metric_parts = []
    for metric in metrics:
        col_part = _sanitize_id_component(metric.col or "none")
        metric_parts.append(f"{metric.op}:{col_part}")
    metrics_part = "_".join(metric_parts) if metric_parts else "none"
    return f"group_aggregate__{scope}__{group_part}__{metrics_part}"


def _summary_metrics_from_legacy(summary_type: str, inputs: dict) -> list[SummaryMetric]:
    if summary_type == "trips_per_day":
        return [SummaryMetric(op="count_rows")]
    if summary_type == "grouped_sum":
        return [SummaryMetric(op="sum", col=inputs.get("value_col"))]
    if summary_type == "basic_counts":
        return [SummaryMetric(op="count_rows")]
    return []


def _summary_group_by_from_legacy(summary_type: str, inputs: dict) -> str | None:
    if summary_type == "trips_per_day":
        return inputs.get("date_col")
    if summary_type == "grouped_sum":
        return inputs.get("group_col")
    return None


def _compile_summary_spec(summary: dict) -> SummarySpec:
    summary_type = summary.get("summary_type", "")
    scope = summary.get("scope", "workbook")
    inputs = summary.get("inputs", {}) if isinstance(summary.get("inputs"), dict) else {}

    group_by = summary.get("group_by") if isinstance(summary.get("group_by"), str) else None
    metrics_raw = summary.get("metrics")
    metrics: list[SummaryMetric] = []
    if isinstance(metrics_raw, list):
        for metric in metrics_raw:
            if not isinstance(metric, dict):
                continue
            op = metric.get("op")
            col = metric.get("col")
            if isinstance(op, str):
                metrics.append(SummaryMetric(op=op, col=col if isinstance(col, str) else None))

    # Legacy fallback to maintain compatibility until v2 summary engine is in place.
    if not metrics and summary_type:
        group_by = group_by or _summary_group_by_from_legacy(summary_type, inputs)
        metrics = _summary_metrics_from_legacy(summary_type, inputs)

    summary_id = summary.get("summary_id", "")
    if not isinstance(summary_id, str) or not summary_id:
        summary_id = _build_summary_id(str(scope), group_by, metrics)

    return SummarySpec(
        summary_id=summary_id,
        summary_type=summary_type,
        inputs=inputs,
        scope=scope,
        group_by=group_by,
        metrics=metrics,
    )


def job_spec_from_profile_dict(profile: dict) -> JobSpec:
    """Build JobSpec from a profile dict with safe defaults."""
    workbooks: list[WorkbookSpec] = []
    for wb in profile.get("workbooks", []):
        split = wb.get("split", {})
        contract_filter = wb.get("contract_filter", {})
        column_policy = wb.get("column_policy", {})
        row_order = wb.get("row_order", {})
        drop_rows = wb.get("drop_rows", [])

        compiled_contract_filter: ContractFilterSpec | None = None
        if isinstance(contract_filter, dict):
            col = contract_filter.get("col", "")
            values = contract_filter.get("values", [])
            if isinstance(col, str) and isinstance(values, list):
                compiled_contract_filter = ContractFilterSpec(
                    col=col,
                    values=[v for v in values if isinstance(v, str)],
                )

        tokens = [
            TokenSpec(
                token_id=t.get("token_id", ""),
                token_type=t.get("token_type", ""),
                inputs=t.get("inputs", {}),
                scope=t.get("scope", "workbook"),
            )
            for t in wb.get("tokens", [])
        ]
        summaries = []
        for summary in wb.get("summaries", []):
            if isinstance(summary, dict):
                summaries.append(_compile_summary_spec(summary))
        drop_rules = [
            RowDropRule(
                col=r.get("col", ""),
                drop_values=r.get("drop_values", []),
            )
            for r in drop_rows
            if isinstance(r, dict)
        ]

        client_value = wb.get("client") or wb.get("workbook_id", "")

        workbooks.append(
            WorkbookSpec(
                workbook_id=wb.get("workbook_id", ""),
                client=client_value,
                referenced_sheet=wb.get("referenced_sheet", ""),
                workbook_name_template=wb.get("workbook_name_template", ""),
                split=SplitSpec(
                    split_col=split.get("split_col", ""),
                    selected_values=split.get("selected_values", []),
                ),
                column_policy=ColumnPolicy(
                    keep_cols=column_policy.get("keep_cols", []),
                    drop_cols=column_policy.get("drop_cols", []),
                    order_cols=column_policy.get("order_cols", []),
                ),
                row_order=RowOrderSpec(
                    sort_keys=row_order.get("sort_keys", []),
                    ascending=row_order.get("ascending", True),
                ),
                tokens=tokens,
                summaries=summaries,
                contract_filter=compiled_contract_filter,
                drop_rows=drop_rules,
            )
        )

    return JobSpec(
        job_id=profile.get("job_id", ""),
        user_date=profile.get("user_date", ""),
        export_label=profile.get("export_label", ""),
        workbooks=workbooks,
    )
