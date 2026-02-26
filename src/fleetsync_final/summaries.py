"""Phase 3 summaries: deterministic artifacts; do not mutate inputs in-place."""

from __future__ import annotations

from typing import Dict, Iterable, List, TYPE_CHECKING

from .artifacts import SummaryArtifact, TokenArtifact
from .models import SummarySpec
from .models import ValidationReport, ValidationIssue
from .summary_registry import SUMMARY_REGISTRY
from .validation import compute_valid
from .naming_columns import make_unique_against, normalize_col_segment

if TYPE_CHECKING:
    import pandas as pd


class SummaryRunResult(list):
    """List-like summary artifacts with attached validation report."""

    def __init__(self, artifacts: list[SummaryArtifact], report: ValidationReport) -> None:
        super().__init__(artifacts)
        self.report = report


def _normalize_tables(tables: Dict[str, "pd.DataFrame"]) -> Dict[str, "pd.DataFrame"]:
    """Return tables as-is; handlers must ensure deterministic ordering."""
    return dict(tables)


def run_summaries(
    df: "pd.DataFrame",
    summary_specs: list[SummarySpec],
    tokens: list[TokenArtifact],
    scope: str,
    per_sheet_map: Dict[str, "pd.DataFrame"] | None = None,
) -> SummaryRunResult:
    """Run summary specs deterministically in spec order."""
    results: list[SummaryArtifact] = []
    warnings: List[ValidationIssue] = []

    for spec in summary_specs:
        if scope == "workbook" and spec.scope != "workbook":
            continue
        if scope == "sheet" and spec.scope not in ("sheet", "sheets"):
            continue

        handler = SUMMARY_REGISTRY.get(spec.summary_type)
        if handler is None:
            raise ValueError(f"Unknown summary_type: {spec.summary_type}")

        if scope == "workbook":
            tables, handler_warnings = handler(df, spec, tokens, None)
            warnings.extend(handler_warnings)
            tables = _normalize_tables(tables)
            results.append(SummaryArtifact(summary_id=spec.summary_id, scope=spec.scope, tables=tables))
        elif scope == "sheet" and spec.scope == "sheet":
            if not per_sheet_map:
                raise ValueError("per_sheet_map is required for sheet-scoped summaries")
            tables_by_sheet: Dict[str, "pd.DataFrame"] = {}
            for sheet_name in sorted(per_sheet_map.keys()):
                per_sheet_tables, handler_warnings = handler(per_sheet_map[sheet_name], spec, tokens, sheet_name)
                warnings.extend(handler_warnings)
                per_sheet_tables = _normalize_tables(per_sheet_tables)
                if len(per_sheet_tables) == 1:
                    tables_by_sheet[sheet_name] = next(iter(per_sheet_tables.values()))
                else:
                    for table_name in sorted(per_sheet_tables.keys()):
                        key = f"{sheet_name}::{table_name}"
                        tables_by_sheet[key] = per_sheet_tables[table_name]
            results.append(
                SummaryArtifact(summary_id=spec.summary_id, scope=spec.scope, tables=tables_by_sheet)
            )
        elif scope == "sheet" and spec.scope == "sheets":
            sheet_map = per_sheet_map or {"Total": df}
            combined_tables: List["pd.DataFrame"] = []
            base_tables: List[tuple[str, "pd.DataFrame"]] = []
            for sheet_name in sorted(sheet_map.keys()):
                per_sheet_tables, handler_warnings = handler(sheet_map[sheet_name], spec, tokens, sheet_name)
                warnings.extend(handler_warnings)
                per_sheet_tables = _normalize_tables(per_sheet_tables)
                if not per_sheet_tables:
                    continue
                table_name = sorted(per_sheet_tables.keys())[0]
                table = per_sheet_tables[table_name].copy()
                base_tables.append((sheet_name, table))

            # LOCK (P9.2): injected column name must be exactly 'sheet_name' consistently.
            injected_col = "sheet_name"

            def _rename_if_column_exists(table_in: "pd.DataFrame", col: str) -> "pd.DataFrame":
                if col not in table_in.columns:
                    return table_in
                # Deterministically rename the existing column away from the injected name.
                # (We preserve ordering by renaming in place.)
                new_name = make_unique_against(existing=set(map(str, table_in.columns)), proposed=[col], max_len=64)[0]
                if new_name == col:
                    # Should not happen because col is in existing, but keep safe.
                    new_name = make_unique_against(existing=set(map(str, table_in.columns)), proposed=[f"{col}__2"], max_len=64)[0]
                return table_in.rename(columns={col: new_name})

            if base_tables:
                for sheet_name, table in base_tables:
                    table = table.copy()
                    table = _rename_if_column_exists(table, injected_col)
                    table.insert(0, injected_col, sheet_name)
                    combined_tables.append(table)

            if combined_tables:
                combined = combined_tables[0]
                if len(combined_tables) > 1:
                    import pandas as pd

                    combined = pd.concat(combined_tables, ignore_index=True)
                group_col = spec.group_by
                if not group_col and len(combined.columns) > 1:
                    group_col = combined.columns[1]
                group_col_out = normalize_col_segment(group_col) if group_col else group_col
                # If group_col_out collides with injected 'sheet_name', suffix deterministically.
                if group_col_out == injected_col:
                    # Rename the non-injected column to keep injected name stable.
                    group_col_out = make_unique_against(existing={injected_col}, proposed=[group_col_out], max_len=64)[0]
                sort_cols = [c for c in (group_col_out, injected_col) if c in combined.columns]
                if sort_cols:
                    combined = combined.sort_values(by=sort_cols, kind="mergesort")
            else:
                columns: list[str] = [injected_col]
                if spec.group_by:
                    group_out = normalize_col_segment(spec.group_by)
                    group_unique = make_unique_against(existing={injected_col}, proposed=[group_out], max_len=64)[0]
                    columns.append(group_unique)
                combined = df.head(0)[[]].reindex(columns=columns)

            results.append(
                SummaryArtifact(summary_id=spec.summary_id, scope=spec.scope, tables={spec.summary_id: combined})
            )
        else:
            raise ValueError(f"Unknown scope: {scope}")

    report = ValidationReport(valid=compute_valid(warnings, []), warnings=warnings, fatals=[])
    return SummaryRunResult(results, report)
