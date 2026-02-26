"""UI-side DTOs.

DTOs are the only objects allowed to cross Qt signals.

Deterministic ordering policy (LOCK):
- Preserve backend-reported message order as-is.
- Do not sort unless the source is inherently unordered.
- If sorting is required (e.g., building from a dict), sort stably by:
  (level_rank, code, message) where level_rank is info=0, warning=1, fatal=2.

Security note:
- `message` is considered untrusted and will be sanitized in P4.3.
- DTOs must remain pure-Python (no Qt imports).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, Optional

from .xray_models import XRayResult


MessageLevel = Literal["warning", "fatal", "info"]
RunStatus = Literal["success", "failed"]
BatchRunStatus = Literal["DONE", "DONE_WITH_FAILURES", "FAILED"]

# Stage names are UI/worker milestones (LOCK).
# Canonical vocabulary (P4.8): keep this list exact and consistent across DTO + worker + UI.
CANONICAL_STAGES: tuple[str, ...] = (
    "VALIDATING_PROFILE",
    "COMPILING_JOBSPEC",
    "RUNNING",
    "WRITING_OUTPUT",
    "VALIDATING_OUTPUT",
    "DONE",
    "FAILED",
)

StageName = Literal[
    "VALIDATING_PROFILE",
    "COMPILING_JOBSPEC",
    "RUNNING",
    "WRITING_OUTPUT",
    "VALIDATING_OUTPUT",
    "DONE",
    "FAILED",
]


@dataclass
class ProfileRef:
    path: str
    name: str


@dataclass
class MessageItem:
    level: MessageLevel
    code: str
    message: str


@dataclass
class ValidationReport:
    is_valid: bool
    warnings: list[MessageItem]
    errors: list[MessageItem]


@dataclass
class ProfilePreview:
    """Workbook-free, safe preview summary.

    Must be computed from profile JSON only (no workbook I/O).
    """

    profile_name: str
    job_id: str
    export_label: str
    workbooks_count: int
    referenced_sheet: str
    split_configured: bool
    split_col: str
    tokens_count: int
    summaries_count: int
    placeholders_supported: list[str]
    placeholders_used: list[str]


@dataclass
class RunResult:
    status: RunStatus
    output_dir: str
    outputs: list[str]
    warnings: list[MessageItem]
    fatals: list[MessageItem]


@dataclass
class BatchRunItem:
    """Per-profile batch run result.

    Ordering (LOCK): items are stored in deterministic profile filename order.
    """

    profile_name: str
    status: RunStatus
    output_dir: str
    outputs: list[str]
    warnings_count: int
    fatals_count: int
    messages: list[MessageItem] | None = None
    # Deterministic short category, e.g. "schema errors", "compile failed", "run fatals", "run warnings".
    reason: str = ""
    # Optional stored messages for successful run results.
    warnings: list[MessageItem] | None = None
    fatals: list[MessageItem] | None = None


@dataclass
class BatchRunSummary:
    batch_status: BatchRunStatus
    total_profiles: int
    succeeded: int
    failed: int
    items: list[BatchRunItem]


def compute_batch_status(*, total_profiles: int, succeeded: int, failed: int) -> BatchRunStatus:
    """Compute overall batch status.

    LOCK:
    - If at least one success: batch_status = DONE_WITH_FAILURES if any failed else DONE
    - If all failed: batch_status = FAILED
    """

    total_profiles = int(total_profiles)
    succeeded = int(succeeded)
    failed = int(failed)
    if total_profiles <= 0:
        return "FAILED"
    if succeeded > 0:
        return "DONE_WITH_FAILURES" if failed > 0 else "DONE"
    return "FAILED"


@dataclass
class StageEvent:
    stage: StageName
    detail: str
    # Optional UI-local timestamp for display only.
    # Leave as None by default to avoid time-based nondeterminism.
    timestamp: Optional[str] = None


@dataclass
class CompileResult:
    """Result of the backend JobSpec compilation pre-run gate.

    This DTO is safe to cross Qt signals.
    It must not contain the backend JobSpec object itself.
    """

    attempted: bool
    success: bool
    job_spec_summary: dict[str, Any]
    messages: list[MessageItem]


InputFileKind = Literal["xlsx", "csv"]
XRayStatus = Literal["idle", "loading", "ready", "empty", "error"]


@dataclass
class ProbeXRayPayload:
    """Result payload for the UI probe worker.

    This DTO is safe to cross Qt signals.
    It must not contain workbook objects or sampled values.
    """

    input_path: str
    file_kind: InputFileKind
    file_key: tuple[str, int, int] | None
    workbook_sheets: list[str]
    preview_sheet_name: str | None
    xray_status: XRayStatus
    xray_result: XRayResult | None
    ui_notice: str | None = None
