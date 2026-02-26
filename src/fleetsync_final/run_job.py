"""Phase 4 run_job: single backend entrypoint (no UI)."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List

from .execute import execute_workbook
from .export_writer import write_workbook_xlsx
from .loader import load_input_xlsx
from .models import JobSpec, RunReport, ValidationIssue
from .path_utils import build_output_dir, get_default_downloads_dir
from .post_write_validator import validate_written_workbook
from .validation import compute_valid, issue, merge_reports


def _normalize_user_date(user_date: str) -> str:
    """Normalize user_date to YYYY_MM_DD style without using system time."""
    return user_date.strip().replace("-", "_").replace("/", "_")


def _format_workbook_name(template: str, user_date: str, client: str | None) -> str:
    date_token = _normalize_user_date(user_date)
    client_token = client or ""
    return template.replace("{YYYY_MM_DD}", date_token).replace("{client}", client_token)


def run_job(input_path: str, job_spec: JobSpec, output_dir: str | Path | None = None) -> RunReport:
    """Load, execute, write, and validate a job; returns RunReport."""
    referenced_sheets: list[str] = []
    for wb_spec in job_spec.workbooks:
        if wb_spec.referenced_sheet not in referenced_sheets:
            referenced_sheets.append(wb_spec.referenced_sheet)
    metadata, sheets = load_input_xlsx(input_path, sheet_names=referenced_sheets)
    if output_dir is None:
        base_dir = get_default_downloads_dir()
    else:
        base_dir = Path(output_dir)
    label = job_spec.export_label or job_spec.job_id or "FleetSync"
    output_root = build_output_dir(base_dir, job_spec.user_date, label)

    warnings: List[ValidationIssue] = []
    fatals: List[ValidationIssue] = []
    outputs: List[str] = []

    for wb_spec in job_spec.workbooks:
        if wb_spec.referenced_sheet not in sheets:
            fatals.append(
                issue(
                    "RUN_001",
                    "FATAL",
                    f"Referenced sheet not found: {wb_spec.referenced_sheet}",
                    f"workbooks[{wb_spec.workbook_id}].referenced_sheet",
                )
            )
            continue

        df = sheets[wb_spec.referenced_sheet]
        result = execute_workbook(df, wb_spec)

        summary_tables: Dict[str, object] = {}
        for artifact in result["summary_artifacts"]:
            for table_name, table_df in artifact.tables.items():
                key = f"{artifact.summary_id}::{table_name}"
                summary_tables[key] = table_df

        workbook_name = _format_workbook_name(
            wb_spec.workbook_name_template, job_spec.user_date, wb_spec.client
        )
        output_path = write_workbook_xlsx(
            output_root,
            workbook_name,
            result["total_df"],
            result["split_dfs"],
            summary_tables,
            main_sheet_name=wb_spec.referenced_sheet,
        )
        outputs.append(str(output_path))

        post_report = validate_written_workbook(output_path)
        merged = merge_reports(result["validation"], post_report)
        warnings.extend(merged.warnings)
        fatals.extend(merged.fatals)

    report = RunReport(
        version="v0.1.0",
        warnings=warnings,
        fatals=fatals,
        outputs=outputs,
        timings=None,
    )
    report_valid = compute_valid(warnings, fatals)
    if not report_valid:
        return report
    return report
