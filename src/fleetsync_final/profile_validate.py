"""Profile validation (schema + metadata-only semantic checks)."""

from __future__ import annotations

from typing import Any, Dict, List

from .metadata import InputMetadata
from .models import TokenSpec, ValidationIssue, ValidationReport
from .tokens import predict_token_output_columns
from .validation import compute_valid, issue


def _add_fatal(issues: List[ValidationIssue], code: str, message: str, path: str | None = None) -> None:
    issues.append(issue(code=code, severity="FATAL", message=message, path=path))


def _add_warning(issues: List[ValidationIssue], code: str, message: str, path: str | None = None) -> None:
    issues.append(issue(code=code, severity="WARNING", message=message, path=path))


def validate_profile_schema(profile: Dict[str, Any]) -> ValidationReport:
    """Validate profile structure and required fields (schema-only)."""
    issues: List[ValidationIssue] = []

    required_fields = ["job_id", "export_label", "workbooks"]
    for field in required_fields:
        if field not in profile:
            _add_fatal(issues, "SCHEMA_001", f"Missing required field: {field}", field)

    workbooks = profile.get("workbooks")
    if workbooks is None:
        _add_fatal(issues, "SCHEMA_002", "workbooks is required", "workbooks")
    elif not isinstance(workbooks, list):
        _add_fatal(issues, "SCHEMA_003", "workbooks must be a list", "workbooks")
    else:
        for idx, wb in enumerate(workbooks):
            if not isinstance(wb, dict):
                _add_fatal(issues, "SCHEMA_004", "workbook item must be an object", f"workbooks[{idx}]")
                continue

            wb_required = [
                "workbook_id",
                "referenced_sheet",
                "workbook_name_template",
                "split",
                "column_policy",
                "row_order",
                "tokens",
                "summaries",
            ]
            for field in wb_required:
                if field not in wb:
                    _add_fatal(issues, "SCHEMA_005", f"Missing required field: {field}", f"workbooks[{idx}].{field}")

            split = wb.get("split")
            if isinstance(split, dict):
                if "split_col" not in split:
                    _add_fatal(issues, "SCHEMA_006", "Missing split.split_col", f"workbooks[{idx}].split.split_col")
                if "selected_values" not in split:
                    _add_fatal(
                        issues,
                        "SCHEMA_007",
                        "Missing split.selected_values",
                        f"workbooks[{idx}].split.selected_values",
                    )
                elif not isinstance(split.get("selected_values"), list):
                    _add_fatal(
                        issues,
                        "SCHEMA_008",
                        "split.selected_values must be a list",
                        f"workbooks[{idx}].split.selected_values",
                    )
            elif split is not None:
                _add_fatal(issues, "SCHEMA_009", "split must be an object", f"workbooks[{idx}].split")

            contract_filter = wb.get("contract_filter")
            if contract_filter is not None:
                if not isinstance(contract_filter, dict):
                    _add_fatal(
                        issues,
                        "SCHEMA_040",
                        "contract_filter must be an object",
                        f"workbooks[{idx}].contract_filter",
                    )
                else:
                    col = contract_filter.get("col")
                    values = contract_filter.get("values")
                    if not isinstance(col, str) or not col:
                        _add_fatal(
                            issues,
                            "SCHEMA_041",
                            "contract_filter.col must be a non-empty string",
                            f"workbooks[{idx}].contract_filter.col",
                        )
                    if not isinstance(values, list):
                        _add_fatal(
                            issues,
                            "SCHEMA_042",
                            "contract_filter.values must be a list",
                            f"workbooks[{idx}].contract_filter.values",
                        )
                    elif not all(isinstance(v, str) for v in values):
                        _add_fatal(
                            issues,
                            "SCHEMA_043",
                            "contract_filter.values must contain only strings",
                            f"workbooks[{idx}].contract_filter.values",
                        )

            column_policy = wb.get("column_policy")
            if isinstance(column_policy, dict):
                for key in ("keep_cols", "drop_cols", "order_cols"):
                    if key not in column_policy:
                        _add_fatal(
                            issues,
                            "SCHEMA_010",
                            f"Missing column_policy.{key}",
                            f"workbooks[{idx}].column_policy.{key}",
                        )
                    elif not isinstance(column_policy.get(key), list):
                        _add_fatal(
                            issues,
                            "SCHEMA_011",
                            f"column_policy.{key} must be a list",
                            f"workbooks[{idx}].column_policy.{key}",
                        )
            elif column_policy is not None:
                _add_fatal(issues, "SCHEMA_012", "column_policy must be an object", f"workbooks[{idx}].column_policy")

            row_order = wb.get("row_order")
            if isinstance(row_order, dict):
                if "sort_keys" not in row_order:
                    _add_fatal(issues, "SCHEMA_013", "Missing row_order.sort_keys", f"workbooks[{idx}].row_order.sort_keys")
                elif not isinstance(row_order.get("sort_keys"), list):
                    _add_fatal(
                        issues,
                        "SCHEMA_014",
                        "row_order.sort_keys must be a list",
                        f"workbooks[{idx}].row_order.sort_keys",
                    )

                # If ascending is missing, accept it with a warning (default is acceptable in Phase 1).
                if "ascending" not in row_order:
                    _add_warning(
                        issues,
                        "SCHEMA_015",
                        "row_order.ascending missing; default will be applied",
                        f"workbooks[{idx}].row_order.ascending",
                    )
                else:
                    ascending = row_order.get("ascending")
                    if not isinstance(ascending, (bool, list)):
                        _add_fatal(
                            issues,
                            "SCHEMA_016",
                            "row_order.ascending must be a bool or list of bools",
                            f"workbooks[{idx}].row_order.ascending",
                        )
                    elif isinstance(ascending, list) and not all(isinstance(v, bool) for v in ascending):
                        _add_fatal(
                            issues,
                            "SCHEMA_017",
                            "row_order.ascending list must contain only bools",
                            f"workbooks[{idx}].row_order.ascending",
                        )
            elif row_order is not None:
                _add_fatal(issues, "SCHEMA_018", "row_order must be an object", f"workbooks[{idx}].row_order")

            if "tokens" in wb and not isinstance(wb.get("tokens"), list):
                _add_fatal(issues, "SCHEMA_019", "tokens must be a list", f"workbooks[{idx}].tokens")

            if "summaries" in wb and not isinstance(wb.get("summaries"), list):
                _add_fatal(issues, "SCHEMA_020", "summaries must be a list", f"workbooks[{idx}].summaries")
            elif isinstance(wb.get("summaries"), list):
                summaries = wb.get("summaries", [])
                for s_idx, summary in enumerate(summaries):
                    path = f"workbooks[{idx}].summaries[{s_idx}]"
                    if not isinstance(summary, dict):
                        _add_fatal(issues, "SCHEMA_026", "summary item must be an object", path)
                        continue

                    summary_type = summary.get("summary_type")
                    scope = summary.get("scope")
                    has_v2_fields = any(key in summary for key in ("group_by", "metrics")) or summary_type == "group_aggregate"
                    has_legacy_fields = "inputs" in summary

                    if not isinstance(summary_type, str) or not summary_type:
                        _add_fatal(issues, "SCHEMA_027", "summary_type must be a non-empty string", f"{path}.summary_type")
                        continue

                    if not isinstance(scope, str) or scope not in ("workbook", "sheet", "sheets"):
                        _add_fatal(issues, "SCHEMA_028", "summary scope must be workbook|sheet|sheets", f"{path}.scope")

                    summary_id = summary.get("summary_id")
                    if summary_id is not None and not isinstance(summary_id, str):
                        _add_fatal(issues, "SCHEMA_029", "summary_id must be a string if provided", f"{path}.summary_id")

                    if has_v2_fields:
                        if summary_type != "group_aggregate":
                            _add_fatal(
                                issues,
                                "SCHEMA_030",
                                "summary_type must be group_aggregate for v2 summaries",
                                f"{path}.summary_type",
                            )
                        group_by = summary.get("group_by")
                        if not isinstance(group_by, str) or not group_by:
                            _add_fatal(
                                issues,
                                "SCHEMA_031",
                                "group_by must be a non-empty string",
                                f"{path}.group_by",
                            )
                        metrics = summary.get("metrics")
                        if not isinstance(metrics, list):
                            _add_fatal(issues, "SCHEMA_032", "metrics must be a list", f"{path}.metrics")
                        else:
                            for m_idx, metric in enumerate(metrics):
                                metric_path = f"{path}.metrics[{m_idx}]"
                                if not isinstance(metric, dict):
                                    _add_fatal(issues, "SCHEMA_033", "metric must be an object", metric_path)
                                    continue
                                op = metric.get("op")
                                if op not in ("count_rows", "sum", "max", "min", "avg", "distinct_count", "null_rate"):
                                    _add_fatal(
                                        issues,
                                        "SCHEMA_034",
                                        "metric op must be count_rows|sum|max|min|avg|distinct_count|null_rate",
                                        f"{metric_path}.op",
                                    )
                                    continue
                                if op in ("sum", "max", "min", "avg", "distinct_count", "null_rate"):
                                    col = metric.get("col")
                                    if not isinstance(col, str) or not col:
                                        _add_fatal(
                                            issues,
                                            "SCHEMA_035",
                                            "metric col required for sum|max|min|avg|distinct_count|null_rate",
                                            f"{metric_path}.col",
                                        )
                                elif "col" in metric and metric.get("col") is not None and not isinstance(metric.get("col"), str):
                                    _add_fatal(
                                        issues,
                                        "SCHEMA_036",
                                        "metric col must be a string when provided",
                                        f"{metric_path}.col",
                                    )
                    elif has_legacy_fields:
                        _add_warning(
                            issues,
                            "SCHEMA_037",
                            "legacy summary spec detected; use group_aggregate with metrics",
                            path,
                        )
                        inputs = summary.get("inputs")
                        if inputs is not None and not isinstance(inputs, dict):
                            _add_fatal(
                                issues,
                                "SCHEMA_038",
                                "legacy summary inputs must be an object",
                                f"{path}.inputs",
                            )
                    else:
                        _add_fatal(
                            issues,
                            "SCHEMA_039",
                            "summary spec missing v2 fields (group_by/metrics) or legacy inputs",
                            path,
                        )

            drop_rows = wb.get("drop_rows")
            if drop_rows is not None:
                if not isinstance(drop_rows, list):
                    _add_fatal(issues, "SCHEMA_022", "drop_rows must be a list", f"workbooks[{idx}].drop_rows")
                else:
                    for r_idx, rule in enumerate(drop_rows):
                        path = f"workbooks[{idx}].drop_rows[{r_idx}]"
                        if not isinstance(rule, dict):
                            _add_fatal(issues, "SCHEMA_023", "drop_rows rule must be an object", path)
                            continue
                        col = rule.get("col")
                        if not isinstance(col, str) or not col:
                            _add_fatal(issues, "SCHEMA_024", "drop_rows.col must be a string", f"{path}.col")
                        drop_values = rule.get("drop_values")
                        if not isinstance(drop_values, list):
                            _add_fatal(
                                issues,
                                "SCHEMA_025",
                                "drop_rows.drop_values must be a list",
                                f"{path}.drop_values",
                            )

    warnings = [i for i in issues if i.severity == "WARNING"]
    fatals = [i for i in issues if i.severity == "FATAL"]
    return ValidationReport(valid=compute_valid(warnings, fatals), warnings=warnings, fatals=fatals)


def validate_profile_semantic(profile: Dict[str, Any], meta: InputMetadata) -> ValidationReport:
    """Validate profile semantics against InputMetadata (metadata-only)."""
    issues: List[ValidationIssue] = []

    workbooks = profile.get("workbooks", [])
    if not isinstance(workbooks, list):
        _add_fatal(issues, "SEMANTIC_000", "workbooks must be a list", "workbooks")
        warnings = [i for i in issues if i.severity == "WARNING"]
        fatals = [i for i in issues if i.severity == "FATAL"]
        return ValidationReport(valid=compute_valid(warnings, fatals), warnings=warnings, fatals=fatals)

    for idx, wb in enumerate(workbooks):
        if not isinstance(wb, dict):
            _add_fatal(issues, "SEMANTIC_001", "workbook item must be an object", f"workbooks[{idx}]")
            continue

        referenced_sheet = wb.get("referenced_sheet")
        if not referenced_sheet or referenced_sheet not in meta.sheet_names:
            _add_fatal(
                issues,
                "SEMANTIC_002",
                "referenced_sheet not found in input metadata",
                f"workbooks[{idx}].referenced_sheet",
            )
            continue

        available_cols = set(meta.get_columns(referenced_sheet))

        token_specs_raw = wb.get("tokens", [])
        if isinstance(token_specs_raw, list):
            try:
                token_specs = [
                    TokenSpec(
                        token_id=t.get("token_id", ""),
                        token_type=t.get("token_type", ""),
                        inputs=t.get("inputs", {}),
                        scope=t.get("scope", "workbook"),
                    )
                    for t in token_specs_raw
                    if isinstance(t, dict)
                ]
                available_cols.update(predict_token_output_columns(token_specs, existing_columns=available_cols))
            except Exception:
                # Semantic validation must remain metadata-only and non-fatal.
                pass

        split = wb.get("split", {})
        split_col = split.get("split_col") if isinstance(split, dict) else None
        if split_col and split_col not in available_cols:
            _add_warning(
                issues,
                "SEMANTIC_003",
                "split.split_col missing in referenced_sheet",
                f"workbooks[{idx}].split.split_col",
            )

        contract_filter = wb.get("contract_filter")
        if isinstance(contract_filter, dict):
            contract_col = contract_filter.get("col")
            contract_values = contract_filter.get("values")
            if isinstance(contract_col, str) and contract_col and contract_col not in available_cols:
                _add_warning(
                    issues,
                    "CONTRACT_COL_MISSING",
                    f"contract_filter col missing in referenced_sheet: {contract_col}",
                    f"workbooks[{idx}].contract_filter.col",
                )
            if isinstance(contract_values, list) and len(contract_values) == 0:
                _add_warning(
                    issues,
                    "CONTRACT_VALUES_EMPTY",
                    "contract_filter.values is empty",
                    f"workbooks[{idx}].contract_filter.values",
                )

        column_policy = wb.get("column_policy", {})
        if isinstance(column_policy, dict):
            for key, code in (
                ("keep_cols", "SEMANTIC_004"),
                ("drop_cols", "SEMANTIC_005"),
                ("order_cols", "SEMANTIC_006"),
            ):
                cols = column_policy.get(key, [])
                if isinstance(cols, list):
                    for col in cols:
                        if col not in available_cols:
                            _add_warning(
                                issues,
                                code,
                                f"column_policy.{key} missing column: {col}",
                                f"workbooks[{idx}].column_policy.{key}",
                            )

        row_order = wb.get("row_order", {})
        if isinstance(row_order, dict):
            sort_keys = row_order.get("sort_keys", [])
            if isinstance(sort_keys, list):
                for key in sort_keys:
                    if key not in available_cols:
                        _add_warning(
                            issues,
                            "SEMANTIC_007",
                            f"row_order missing column: {key}",
                            f"workbooks[{idx}].row_order.sort_keys",
                        )

        drop_rows = wb.get("drop_rows", [])
        if isinstance(drop_rows, list):
            for r_idx, rule in enumerate(drop_rows):
                if not isinstance(rule, dict):
                    continue
                col = rule.get("col")
                if isinstance(col, str) and col and col not in available_cols:
                    _add_warning(
                        issues,
                        "ROWDROP_COL_MISSING",
                        f"drop_rows col missing in referenced_sheet: {col}",
                        f"workbooks[{idx}].drop_rows[{r_idx}].col",
                    )
                drop_values = rule.get("drop_values")
                if isinstance(drop_values, list) and len(drop_values) == 0:
                    _add_warning(
                        issues,
                        "ROWDROP_VALUES_EMPTY",
                        "drop_rows.drop_values is empty",
                        f"workbooks[{idx}].drop_rows[{r_idx}].drop_values",
                    )

        summaries = wb.get("summaries", [])
        if isinstance(summaries, list):
            for s_idx, summary in enumerate(summaries):
                if not isinstance(summary, dict):
                    continue
                summary_type = summary.get("summary_type")
                has_v2_fields = any(key in summary for key in ("group_by", "metrics")) or summary_type == "group_aggregate"
                if not has_v2_fields:
                    continue
                group_by = summary.get("group_by")
                if isinstance(group_by, str) and group_by and group_by not in available_cols:
                    _add_warning(
                        issues,
                        "SUMMARY_GROUPBY_MISSING",
                        f"summary group_by missing in referenced_sheet: {group_by}",
                        f"workbooks[{idx}].summaries[{s_idx}].group_by",
                    )
                metrics = summary.get("metrics")
                if isinstance(metrics, list):
                    for m_idx, metric in enumerate(metrics):
                        if not isinstance(metric, dict):
                            continue
                        op = metric.get("op")
                        col = metric.get("col")
                        if (
                            op in ("sum", "max", "min", "avg", "distinct_count", "null_rate")
                            and isinstance(col, str)
                            and col
                            and col not in available_cols
                        ):
                            _add_warning(
                                issues,
                                "SUMMARY_METRIC_COL_MISSING",
                                f"summary metric column missing in referenced_sheet: {col}",
                                f"workbooks[{idx}].summaries[{s_idx}].metrics[{m_idx}].col",
                            )

    warnings = [i for i in issues if i.severity == "WARNING"]
    fatals = [i for i in issues if i.severity == "FATAL"]
    return ValidationReport(valid=compute_valid(warnings, fatals), warnings=warnings, fatals=fatals)
