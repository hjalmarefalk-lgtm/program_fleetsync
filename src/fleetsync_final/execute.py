"""Phase 4 execution pipeline (no export/UI): apply policies, split, tokens, summaries."""

from __future__ import annotations

from typing import Dict, List

import pandas as pd

from .artifacts import SummaryArtifact, TokenArtifact
from .columns import apply_column_policy
from .models import WorkbookSpec
from .rows import apply_contract_filter, apply_drop_rows, apply_row_order
from .split import split_by_selected_values
from .summaries import run_summaries
from .tokens import TokenColumnSpec, apply_token_columns, run_tokens, validate_token_specs
from .validation import ValidationReport, compute_valid, merge_reports


def execute_workbook(df: pd.DataFrame, wb_spec: WorkbookSpec) -> Dict[str, object]:
    """Execute workbook-scoped transforms and build artifacts (no export)."""
    base_df = apply_drop_rows(df, wb_spec.drop_rows)
    base_df = apply_contract_filter(base_df, wb_spec.contract_filter)

    split_dfs = split_by_selected_values(base_df, wb_spec.split)
    if wb_spec.split.selected_values:
        ordered_keys = list(split_dfs.keys())
        total_df = pd.concat([split_dfs[key] for key in ordered_keys], ignore_index=True)
    else:
        total_df = base_df

    token_report = validate_token_specs(wb_spec.tokens, list(total_df.columns))
    total_df, token_warnings, token_cols, total_token_specs, _ = apply_token_columns(total_df, wb_spec.tokens)
    sheet_token_specs: Dict[str, list[TokenColumnSpec]] = {}
    for sheet_name in sorted(split_dfs.keys()):
        split_df, sheet_warnings, _, sheet_specs, _ = apply_token_columns(split_dfs[sheet_name], wb_spec.tokens)
        split_dfs[sheet_name] = split_df
        sheet_token_specs[sheet_name] = sheet_specs
        token_warnings.extend(sheet_warnings)

    total_df = apply_column_policy(total_df, wb_spec.column_policy, token_cols=token_cols)
    total_df = apply_row_order(total_df, wb_spec.row_order)
    for sheet_name in sorted(split_dfs.keys()):
        sheet_df = apply_column_policy(split_dfs[sheet_name], wb_spec.column_policy, token_cols=token_cols)
        sheet_df = apply_row_order(sheet_df, wb_spec.row_order)
        split_dfs[sheet_name] = sheet_df

    token_artifacts: List[TokenArtifact] = []
    token_artifacts.extend(run_tokens(total_df, wb_spec.tokens, scope="workbook", token_column_specs=total_token_specs))
    for sheet_name in sorted(split_dfs.keys()):
        token_artifacts.extend(
            run_tokens(
                split_dfs[sheet_name],
                wb_spec.tokens,
                scope="sheet",
                sheet_name=sheet_name,
                token_column_specs=sheet_token_specs.get(sheet_name),
            )
        )

    workbook_summaries = run_summaries(total_df, wb_spec.summaries, token_artifacts, scope="workbook")
    sheet_summaries = run_summaries(
        total_df, wb_spec.summaries, token_artifacts, scope="sheet", per_sheet_map=split_dfs
    )
    summary_artifacts: List[SummaryArtifact] = list(workbook_summaries) + list(sheet_summaries)

    token_columns_report = ValidationReport(
        valid=compute_valid(token_warnings, []),
        warnings=token_warnings,
        fatals=[],
    )

    validation: ValidationReport = merge_reports(
        token_report,
        token_columns_report,
        workbook_summaries.report,
        sheet_summaries.report,
    )

    return {
        "total_df": total_df,
        "split_dfs": split_dfs,
        "token_artifacts": token_artifacts,
        "summary_artifacts": summary_artifacts,
        "validation": validation,
    }
