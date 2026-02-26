"""Deterministic UX status model (P7).

This module is UI-side only and must remain pure:
- No Qt imports
- No backend imports
- No time/polling

Two-layer model (LOCK):
1) Correctness (mutually exclusive): FAIL / WARN / OK
2) Overlay (0..n): NOT_READY, RUNNING, DONE(optional)

"""

from __future__ import annotations

from typing import Iterable, Literal, Protocol

from .dto import CompileResult, MessageItem, RunResult, ValidationReport


Correctness = Literal["FAIL", "WARN", "OK"]
Overlay = Literal["NOT_READY", "RUNNING", "DONE"]


class _HasRunSignals(Protocol):
    """Minimal protocol to read in-memory UI state.

    Intentionally small to keep this module testable without Qt.
    """

    input_path: str
    selected_profile_path: str
    user_date_valid: bool
    run_busy: bool


def _iter_messages(items: Iterable[MessageItem] | None) -> Iterable[MessageItem]:
    return list(items or [])


def compute_correctness(
    schema_report: ValidationReport | None,
    compile_report: CompileResult | None,
    run_report: RunResult | None,
) -> Correctness:
    """Compute correctness from available reports.

    LOCKS:
    - Correctness is mutually exclusive: FAIL / WARN / OK
    - Correctness derives from schema/static validation + compile/prepare + run result.
    - "No run yet": if run_report is None, ignore run warnings/fatals.
    - FAIL beats WARN.
    """

    has_fail = False
    has_warn = False

    if schema_report is not None:
        # Prefer explicit message lists, but stay conservative if is_valid is False.
        if (not bool(getattr(schema_report, "is_valid", False))) or list(getattr(schema_report, "errors", []) or []):
            has_fail = True
        if list(getattr(schema_report, "warnings", []) or []):
            has_warn = True

    if compile_report is not None and bool(getattr(compile_report, "attempted", False)):
        if not bool(getattr(compile_report, "success", False)):
            has_fail = True
        for msg in _iter_messages(getattr(compile_report, "messages", None)):
            if getattr(msg, "level", None) == "fatal":
                has_fail = True
            elif getattr(msg, "level", None) == "warning":
                has_warn = True

    if run_report is not None:
        # RunResult is only considered once a run exists.
        if getattr(run_report, "status", None) == "failed":
            has_fail = True
        if list(getattr(run_report, "fatals", []) or []):
            has_fail = True
        if list(getattr(run_report, "warnings", []) or []):
            has_warn = True

    if has_fail:
        return "FAIL"
    if has_warn:
        return "WARN"
    return "OK"


def compute_overlays(
    app_state: _HasRunSignals,
    is_running: bool,
    has_valid_inputs: bool,
    last_run_ok: bool | None = None,
) -> set[Overlay]:
    """Compute overlay indicators.

    LOCKS:
    - RUNNING overlay is derived only from explicit state (no timers/polling).
    - NOT_READY overlay is derived from global gating.
    - Overlays do not replace correctness.
    - DONE overlay is optional and must not use timestamps.
    """

    overlays: set[Overlay] = set()

    # NOT_READY is an overlay: it indicates gating, but never replaces correctness.
    if not bool(has_valid_inputs):
        overlays.add("NOT_READY")

    # RUNNING is event-driven: derived from the explicit is_running flag.
    if bool(is_running):
        overlays.add("RUNNING")

    # DONE is optional. Only show if the caller tracks last run success in-memory.
    if last_run_ok is True and ("RUNNING" not in overlays):
        overlays.add("DONE")

    return overlays
