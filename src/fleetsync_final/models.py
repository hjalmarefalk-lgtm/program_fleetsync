"""Phase 1 data models and contracts (no behavior)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Union, Literal


@dataclass(frozen=True)
class SplitSpec:
    """Split definition for selected values."""

    split_col: str
    selected_values: List[str]


@dataclass(frozen=True)
class ContractFilterSpec:
    """Workbook foundation filter by contract column and allowed values."""

    col: str
    values: List[str]


@dataclass(frozen=True)
class ColumnPolicy:
    """Column keep/drop/order policy (workbook-scoped only)."""

    keep_cols: List[str]
    drop_cols: List[str]
    order_cols: List[str]


@dataclass(frozen=True)
class RowOrderSpec:
    """Row ordering policy using stable sort keys."""

    sort_keys: List[str]
    ascending: Union[bool, List[bool]] = True


@dataclass(frozen=True)
class RowDropRule:
    """Row drop rule applied before split and ordering (workbook-scoped)."""

    col: str
    drop_values: List[str]


@dataclass(frozen=True)
class RowFilterSpec:
    """Row filter spec (metadata-only use in Phase 1)."""

    keep_values: Optional[Dict[str, List[str]]] = None


@dataclass(frozen=True)
class TokenSpec:
    """Token spec for deterministic tokenization (may add output columns)."""

    token_id: str
    token_type: str
    inputs: Dict[str, Union[str, int, float, bool, List[str]]]
    scope: Literal["workbook", "sheet"]


@dataclass(frozen=True)
class SummarySpec:
    """Summary spec for generic group aggregates (legacy inputs supported)."""

    summary_id: str
    summary_type: str
    inputs: Dict[str, Union[str, int, float, bool, List[str]]]
    scope: Literal["workbook", "sheet", "sheets"]
    group_by: Optional[str] = None
    metrics: List["SummaryMetric"] = field(default_factory=list)


@dataclass(frozen=True)
class SummaryMetric:
    """Metric definition for group_aggregate summaries."""

    op: Literal["count_rows", "sum", "max", "min", "avg", "distinct_count", "null_rate"]
    col: Optional[str] = None


@dataclass(frozen=True)
class WorkbookSpec:
    """Workbook-scoped rules only; summary sheet is first by contract."""

    workbook_id: str
    client: Optional[str]
    referenced_sheet: str
    workbook_name_template: str
    split: SplitSpec
    column_policy: ColumnPolicy
    row_order: RowOrderSpec
    tokens: List[TokenSpec]
    summaries: List[SummarySpec]
    contract_filter: Optional[ContractFilterSpec] = None
    drop_rows: List[RowDropRule] = field(default_factory=list)


@dataclass(frozen=True)
class JobSpec:
    """Job-level inputs for a single run (user-provided date required)."""

    job_id: str
    user_date: str
    export_label: str
    workbooks: List[WorkbookSpec]


@dataclass(frozen=True)
class ValidationIssue:
    """Validation issue with explicit severity."""

    code: str
    severity: Literal["WARNING", "FATAL"]
    message: str
    path: Optional[str] = None


@dataclass(frozen=True)
class ValidationReport:
    """Validation report with warnings vs fatals separation."""

    valid: bool
    warnings: List[ValidationIssue] = field(default_factory=list)
    fatals: List[ValidationIssue] = field(default_factory=list)


@dataclass(frozen=True)
class RunReport:
    """Run report for a completed job (no UI or Excel behavior here)."""

    version: str
    warnings: List[ValidationIssue] = field(default_factory=list)
    fatals: List[ValidationIssue] = field(default_factory=list)
    outputs: List[str] = field(default_factory=list)
    timings: Optional[Dict[str, float]] = None
