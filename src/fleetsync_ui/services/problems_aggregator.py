"""Deterministic Problems box aggregation (P7).

UI-only helper. No backend imports. No Qt imports.

LOCKS:
- Sources (in this order): schema/static validation → compile/prepare → run-result
- Severity grouping: Errors/Fatals first, then Warnings
- Preserve source-provided order within each group (no sorting)
- Display cap: top N items + “+M more”
- Messages must be sanitized before display; this helper re-sanitizes defensively.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Literal

from .dto import CompileResult, MessageItem, RunResult, ValidationReport
from .safe_errors import sanitize_text


ProblemSource = Literal["schema", "compile", "run"]
ProblemSeverity = Literal["fatal", "warning"]


@dataclass(frozen=True)
class ProblemItem:
    source: ProblemSource
    severity: ProblemSeverity
    code: str
    message: str

    def display_text(self) -> str:
        code = sanitize_text((self.code or "").strip())
        msg = sanitize_text((self.message or "").strip())
        if code and msg:
            return f"{code}: {msg}"
        if code:
            return code
        return msg or "(message)"


@dataclass(frozen=True)
class ProblemsSummary:
    fatals_total: int
    warnings_total: int
    display_items: list[ProblemItem]
    hidden_count: int


def _truncate(text: str, max_len: int) -> str:
    t = sanitize_text((text or "").strip())
    max_len = int(max_len)
    if max_len <= 0:
        return ""
    if len(t) <= max_len:
        return t
    # Deterministic truncation.
    return (t[: max(0, max_len - 1)] + "…") if max_len >= 2 else t[:1]


def _as_problem_items(
    *,
    source: ProblemSource,
    severity: ProblemSeverity,
    items: Iterable[MessageItem] | None,
    message_max_len: int,
) -> list[ProblemItem]:
    out: list[ProblemItem] = []
    for m in list(items or []):
        code = _truncate(getattr(m, "code", "") or "", 24)
        msg = _truncate(getattr(m, "message", "") or "", message_max_len)
        out.append(ProblemItem(source=source, severity=severity, code=code, message=msg))
    return out


def aggregate_problems(
    *,
    schema_report: ValidationReport | None,
    compile_report: CompileResult | None,
    run_report: RunResult | None,
    cap: int = 3,
    message_max_len: int = 120,
) -> ProblemsSummary:
    """Aggregate problems deterministically for the always-visible Problems box."""

    # Build fatal group (Errors/Fatals first) preserving source order.
    fatals: list[ProblemItem] = []
    warnings: list[ProblemItem] = []

    if schema_report is not None:
        fatals.extend(
            _as_problem_items(
                source="schema",
                severity="fatal",
                items=getattr(schema_report, "errors", None),
                message_max_len=message_max_len,
            )
        )
        warnings.extend(
            _as_problem_items(
                source="schema",
                severity="warning",
                items=getattr(schema_report, "warnings", None),
                message_max_len=message_max_len,
            )
        )

    if compile_report is not None and bool(getattr(compile_report, "attempted", False)):
        msgs = list(getattr(compile_report, "messages", None) or [])
        fatals.extend(
            _as_problem_items(
                source="compile",
                severity="fatal",
                items=[m for m in msgs if getattr(m, "level", None) == "fatal"],
                message_max_len=message_max_len,
            )
        )
        warnings.extend(
            _as_problem_items(
                source="compile",
                severity="warning",
                items=[m for m in msgs if getattr(m, "level", None) == "warning"],
                message_max_len=message_max_len,
            )
        )

    # No-run-yet lock applies to correctness, not to problems display.
    # Problems box may still show "no run yet" as empty; run messages only exist if a run exists.
    if run_report is not None:
        fatals.extend(
            _as_problem_items(
                source="run",
                severity="fatal",
                items=getattr(run_report, "fatals", None),
                message_max_len=message_max_len,
            )
        )
        warnings.extend(
            _as_problem_items(
                source="run",
                severity="warning",
                items=getattr(run_report, "warnings", None),
                message_max_len=message_max_len,
            )
        )

    cap = max(0, int(cap))
    all_items = list(fatals) + list(warnings)
    display_items = all_items[:cap] if cap else []
    hidden = max(0, len(all_items) - len(display_items))

    return ProblemsSummary(
        fatals_total=len(fatals),
        warnings_total=len(warnings),
        display_items=display_items,
        hidden_count=hidden,
    )
