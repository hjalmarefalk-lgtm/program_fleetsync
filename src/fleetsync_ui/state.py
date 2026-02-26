"""Application state (Qt signals).

Single source of truth for:
- selected input path
- probe status
- current X-ray result

And (P4.4/P4.5):
- profiles directory
- discovered profiles list
- selected profile path
- selected profile preview/validation (schema-only)

And (P4.6):
- required user_date (raw + normalized) + validity
- deterministic run readiness + hint
"""

from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import Optional

from PySide6.QtCore import QObject, Signal

from .services.dto import (
    BatchRunSummary,
    CompileResult,
    ProbeXRayPayload,
    ProfilePreview,
    ProfileRef,
    RunResult,
    StageEvent,
    ValidationReport,
)
from .services.safe_errors import sanitize_text
from .services.user_date import normalize_user_date
from .services.xray_models import XRayResult


class ProbeStatus(str, Enum):
    NO_FILE = "NO_FILE"
    LOADING = "LOADING"
    LOADED = "LOADED"
    ERROR = "ERROR"


class AppState(QObject):
    input_path_changed = Signal(str)
    status_changed = Signal(str)
    xray_changed = Signal(object)

    # P5.x: X-ray sheet picker (UI-only)
    input_file_kind_changed = Signal(object)
    input_file_key_changed = Signal(object)
    workbook_sheets_changed = Signal(object)
    preview_sheet_name_changed = Signal(object)

    profiles_dir_changed = Signal(str)
    profiles_list_changed = Signal(object)
    selected_profile_path_changed = Signal(str)

    selected_profile_dict_changed = Signal(object)
    selected_profile_validation_changed = Signal(object)
    selected_profile_preview_changed = Signal(object)

    # P9.5a: Profile Creator -> Profile Runner refresh/select (UI-only)
    profiles_refresh_requested = Signal(object)

    user_date_text_changed = Signal(str)
    user_date_valid_changed = Signal(bool)
    run_ready_changed = Signal(bool)
    run_ready_hint_changed = Signal(str)

    compile_result_changed = Signal(object)

    run_stage_changed = Signal(object)
    run_busy_changed = Signal(bool)
    run_result_changed = Signal(object)
    batch_run_result_changed = Signal(object)

    notices_changed = Signal(object)

    def __init__(self) -> None:
        super().__init__()

        # Input probing / X-ray
        self._input_path: str = ""
        self._status: ProbeStatus = ProbeStatus.NO_FILE
        self._xray: Optional[XRayResult] = None
        self._last_error: str = ""

        # X-ray sheet picker (UI-only)
        self._input_file_kind: str | None = None
        self._input_file_key: tuple[str, int, int] | None = None
        self._workbook_sheets: list[str] = []
        self._preview_sheet_name: str | None = None

        # In-memory, per-file-key state (LOCK: no disk persistence)
        self._sheets_by_file_key: dict[tuple[str, int, int], list[str]] = {}
        self._preview_sheet_by_file_key: dict[tuple[str, int, int], str | None] = {}

        # Profiles (P4.4)
        self._profiles_dir: str = ""
        self._profiles_list: list[ProfileRef] = []
        self._selected_profile_path: str = ""

        # Selected profile (P4.5)
        self._selected_profile_dict: dict = {}
        self._selected_profile_validation: ValidationReport = ValidationReport(is_valid=False, warnings=[], errors=[])
        self._selected_profile_preview: ProfilePreview = ProfilePreview(
            profile_name="",
            job_id="",
            export_label="",
            workbooks_count=0,
            referenced_sheet="",
            split_configured=False,
            split_col="",
            tokens_count=0,
            summaries_count=0,
            placeholders_supported=["{YYYY_MM_DD}", "{client}"],
            placeholders_used=[],
        )

        # user_date + gating (P4.6)
        self._user_date_text: str = ""  # raw
        self._user_date_normalized: str | None = None  # canonical YYYY-MM-DD
        self._user_date_valid: bool = False
        self._run_ready: bool = False
        self._run_ready_hint: str = ""

        # P4.7: compilation pre-run gate state
        self._compile_result: CompileResult = CompileResult(
            attempted=False,
            success=False,
            job_spec_summary={},
            messages=[],
        )
        self._compiled_job_spec_handle: object | None = None

        # P4.8: run stage (milestone-only) display state
        self._run_stage: StageEvent | None = None

        # P4.9: run busy + last result
        self._run_busy: bool = False
        self._run_result: RunResult | None = None

        # P4.10: batch run result
        self._batch_run_result: BatchRunSummary | None = None

        # P5.4: non-blocking UX notices (sanitized strings only)
        self._notices: list[str] = []

        self._recompute_run_ready()

    @property
    def notices(self) -> list[str]:
        return list(self._notices)

    def clear_notices(self) -> None:
        if not self._notices:
            return
        self._notices = []
        self.notices_changed.emit(self.notices)

    def add_notice(self, message: str) -> None:
        msg = sanitize_text((message or "").strip())
        if not msg:
            return
        # Keep bounded to avoid unbounded growth.
        self._notices.append(msg)
        if len(self._notices) > 20:
            self._notices = self._notices[-20:]
        self.notices_changed.emit(self.notices)

    @property
    def compile_result(self) -> CompileResult:
        return self._compile_result

    @property
    def compiled_job_spec_handle(self) -> object | None:
        # IMPORTANT: This is an opaque handle. Do not send it over Qt signals.
        return self._compiled_job_spec_handle

    @property
    def run_stage(self) -> StageEvent | None:
        return self._run_stage

    @property
    def run_busy(self) -> bool:
        return bool(self._run_busy)

    def set_run_busy(self, busy: bool) -> None:
        busy = bool(busy)
        if busy == self._run_busy:
            return
        self._run_busy = busy
        self.run_busy_changed.emit(self._run_busy)

    @property
    def run_result(self) -> RunResult | None:
        return self._run_result

    def clear_run_result(self) -> None:
        if self._run_result is None:
            return
        self._run_result = None
        self.run_result_changed.emit(self._run_result)

        # Clear batch result as well to avoid stale summary.
        self.clear_batch_run_result()

    def set_run_result(self, result: RunResult | None) -> None:
        self._run_result = result
        self.run_result_changed.emit(self._run_result)

    @property
    def batch_run_result(self) -> BatchRunSummary | None:
        return self._batch_run_result

    def clear_batch_run_result(self) -> None:
        if self._batch_run_result is None:
            return
        self._batch_run_result = None
        self.batch_run_result_changed.emit(self._batch_run_result)

    def set_batch_run_result(self, result: BatchRunSummary | None) -> None:
        self._batch_run_result = result
        self.batch_run_result_changed.emit(self._batch_run_result)

    def clear_run_stage(self) -> None:
        if self._run_stage is None:
            return
        self._run_stage = None
        self.run_stage_changed.emit(self._run_stage)

    def set_run_stage(self, event: StageEvent | None) -> None:
        self._run_stage = event
        self.run_stage_changed.emit(self._run_stage)

    def clear_compile_result(self) -> None:
        if not self._compile_result.attempted and self._compiled_job_spec_handle is None:
            return
        self._compile_result = CompileResult(attempted=False, success=False, job_spec_summary={}, messages=[])
        self._compiled_job_spec_handle = None
        self.compile_result_changed.emit(self._compile_result)

    def set_compile_result(self, result: CompileResult, job_spec_handle: object | None) -> None:
        # Store the opaque JobSpec handle in-memory only.
        self._compile_result = result
        self._compiled_job_spec_handle = job_spec_handle
        self.compile_result_changed.emit(self._compile_result)

    # --- Probe state ---
    @property
    def input_path(self) -> str:
        return self._input_path

    @property
    def status(self) -> ProbeStatus:
        return self._status

    @property
    def xray(self) -> Optional[XRayResult]:
        return self._xray

    @property
    def last_error(self) -> str:
        return self._last_error

    def set_input_path(self, input_path: str) -> None:
        input_path = input_path or ""
        changed = input_path != self._input_path
        self._input_path = input_path
        self.input_path_changed.emit(self._input_path)
        if changed:
            self.clear_compile_result()
            self.clear_run_result()
            self.clear_batch_run_result()
        self._recompute_run_ready()
        if not self._input_path:
            self._set_status(ProbeStatus.NO_FILE)
            self._set_xray(None)

            self.set_input_file_kind(None)
            self.set_input_file_key(None)
            self.set_workbook_sheets([])
            self.set_preview_sheet_name(None)

    @property
    def input_file_kind(self) -> str | None:
        return self._input_file_kind

    def set_input_file_kind(self, kind: str | None) -> None:
        kind = (kind or None)
        if kind == self._input_file_kind:
            return
        self._input_file_kind = kind
        self.input_file_kind_changed.emit(self._input_file_kind)

    @property
    def input_file_key(self) -> tuple[str, int, int] | None:
        return self._input_file_key

    def set_input_file_key(self, key: tuple[str, int, int] | None) -> None:
        if key == self._input_file_key:
            return
        self._input_file_key = key
        self.input_file_key_changed.emit(self._input_file_key)

    @property
    def workbook_sheets(self) -> list[str]:
        return list(self._workbook_sheets)

    def set_workbook_sheets(self, sheets: list[str]) -> None:
        self._workbook_sheets = list(sheets or [])
        self.workbook_sheets_changed.emit(self.workbook_sheets)

    @property
    def preview_sheet_name(self) -> str | None:
        return self._preview_sheet_name

    def set_preview_sheet_name(self, name: str | None) -> None:
        name = (name or None)
        if name == self._preview_sheet_name:
            return
        self._preview_sheet_name = name
        self.preview_sheet_name_changed.emit(self._preview_sheet_name)

        # Remember selection per-file-key (LOCK: in-memory only).
        if self._input_file_key is not None:
            self._preview_sheet_by_file_key[self._input_file_key] = name
            if len(self._preview_sheet_by_file_key) > 20:
                # Bound memory; deterministic trimming by insertion order is not guaranteed for dict,
                # but this is UX-only and not part of deterministic outputs.
                keys = list(self._preview_sheet_by_file_key.keys())
                for k in keys[:-20]:
                    self._preview_sheet_by_file_key.pop(k, None)

    def remember_workbook_sheets(self, key: tuple[str, int, int], sheets: list[str]) -> None:
        if key is None:
            return
        self._sheets_by_file_key[key] = list(sheets or [])
        if len(self._sheets_by_file_key) > 20:
            keys = list(self._sheets_by_file_key.keys())
            for k in keys[:-20]:
                self._sheets_by_file_key.pop(k, None)

    def get_remembered_workbook_sheets(self, key: tuple[str, int, int] | None) -> list[str] | None:
        if key is None:
            return None
        v = self._sheets_by_file_key.get(key)
        return list(v) if v is not None else None

    def get_remembered_preview_sheet(self, key: tuple[str, int, int] | None) -> str | None:
        if key is None:
            return None
        return self._preview_sheet_by_file_key.get(key)

    def reload(self) -> None:
        if not self._input_path:
            self._set_status(ProbeStatus.NO_FILE)
            return
        self.request_probe(self._input_path)

    def request_probe(self, input_path: str) -> None:
        """Request a probe run.

        The actual work is done in `FileProbeWorker` off-thread.
        """

        self.set_input_path(input_path)
        if not self._input_path:
            self._set_status(ProbeStatus.NO_FILE)
            return
        self._last_error = ""
        self._set_status(ProbeStatus.LOADING)

    def on_probe_success(self, result: XRayResult) -> None:
        self._last_error = ""
        self._set_xray(result)
        self._set_status(ProbeStatus.LOADED)

    def on_probe_error(self, message: str) -> None:
        self._last_error = sanitize_text((message or "Error").strip()) or "Error"
        self._set_xray(None)
        self._set_status(ProbeStatus.ERROR)

    def on_probe_payload(self, payload: object) -> None:
        if isinstance(payload, ProbeXRayPayload):
            self._last_error = ""
            # Minimal UI-only metadata.
            self.set_input_file_kind(payload.file_kind)
            self.set_input_file_key(payload.file_key)

            if payload.file_kind == "csv":
                self.set_workbook_sheets([])
                self.set_preview_sheet_name(None)
            else:
                # XLSX: update sheet list if provided.
                if payload.workbook_sheets:
                    self.set_workbook_sheets(payload.workbook_sheets)
                    if payload.file_key is not None:
                        self.remember_workbook_sheets(payload.file_key, payload.workbook_sheets)
                # Update preview selection (None allowed for empty workbooks).
                self.set_preview_sheet_name(payload.preview_sheet_name)

            if payload.ui_notice:
                self.add_notice(payload.ui_notice)

            self._set_xray(payload.xray_result)
            # Treat empty as LOADED (file load succeeded).
            self._set_status(ProbeStatus.LOADED)
            return

        # Backwards compatibility: accept raw XRayResult.
        if isinstance(payload, XRayResult):
            self.on_probe_success(payload)

    def _set_status(self, status: ProbeStatus) -> None:
        self._status = status
        self.status_changed.emit(self._status.value)

    def _set_xray(self, xray: Optional[XRayResult]) -> None:
        self._xray = xray
        self.xray_changed.emit(xray)

    # --- Profiles / selection ---
    @property
    def profiles_dir(self) -> str:
        return self._profiles_dir

    @property
    def profiles_list(self) -> list[ProfileRef]:
        return list(self._profiles_list)

    @property
    def selected_profile_path(self) -> str:
        return self._selected_profile_path

    @property
    def selected_profile_dict(self) -> dict:
        return dict(self._selected_profile_dict)

    @property
    def selected_profile_validation(self) -> ValidationReport:
        return self._selected_profile_validation

    @property
    def selected_profile_preview(self) -> ProfilePreview:
        return self._selected_profile_preview

    def set_profiles_dir(self, profiles_dir: str) -> None:
        profiles_dir = profiles_dir or ""
        self._profiles_dir = profiles_dir
        self.profiles_dir_changed.emit(self._profiles_dir)

    def set_profiles_list(self, profiles: list[ProfileRef]) -> None:
        self._profiles_list = list(profiles or [])
        self.profiles_list_changed.emit(self._profiles_list)

    def request_profiles_refresh(self, *, select_basename: str | None = None) -> None:
        """Request the Profile Runner to refresh its profiles list.

        UI-only signal so the creator tab can refresh the runner list without
        coupling views directly.
        """

        self.profiles_refresh_requested.emit((select_basename or "").strip() or None)

    def set_selected_profile_path(self, profile_path: str) -> None:
        profile_path = profile_path or ""
        self._selected_profile_path = profile_path
        self.selected_profile_path_changed.emit(self._selected_profile_path)
        self.clear_compile_result()
        self.clear_run_result()
        self.clear_batch_run_result()
        self._recompute_run_ready()

    def set_selected_profile_dict(self, profile_dict: dict) -> None:
        self._selected_profile_dict = dict(profile_dict or {})
        self.selected_profile_dict_changed.emit(self._selected_profile_dict)
        self.clear_compile_result()
        self.clear_run_result()
        self.clear_batch_run_result()

    def set_selected_profile_validation(self, report: ValidationReport) -> None:
        self._selected_profile_validation = report
        self.selected_profile_validation_changed.emit(report)
        self.clear_compile_result()
        self.clear_run_result()
        self.clear_batch_run_result()
        self._recompute_run_ready()

    def set_selected_profile_preview(self, preview: ProfilePreview) -> None:
        self._selected_profile_preview = preview
        self.selected_profile_preview_changed.emit(preview)

    # --- user_date + gating ---
    @property
    def user_date_text(self) -> str:
        return self._user_date_text

    @property
    def user_date_normalized(self) -> str | None:
        return self._user_date_normalized

    @property
    def user_date_valid(self) -> bool:
        return self._user_date_valid

    @property
    def run_ready(self) -> bool:
        return self._run_ready

    @property
    def run_ready_hint(self) -> str:
        return self._run_ready_hint

    def set_user_date_text(self, text: str) -> None:
        ud = normalize_user_date(text)
        changed = (ud.raw != self._user_date_text) or (ud.normalized != self._user_date_normalized)
        self._user_date_text = ud.raw
        self._user_date_normalized = ud.normalized
        if changed:
            self.user_date_text_changed.emit(self._user_date_text)

        valid = bool(ud.valid)
        if valid != self._user_date_valid:
            self._user_date_valid = valid
            self.user_date_valid_changed.emit(self._user_date_valid)
        # LOCK (P9.3): date changes invalidate compiled handle only.
        if changed:
            self.clear_compile_result()
        self._recompute_run_ready()
        self._recompute_run_ready()

    def _recompute_run_ready(self) -> None:
        # LOCK gating conditions (deterministic order):
        # 1) input_path selected and exists
        # 2) selected_profile_path exists
        # 3) profile schema validation is_valid == True
        # 4) user_date_valid == True
        missing: list[str] = []

        input_ok = bool(self._input_path) and Path(self._input_path).exists()
        if not input_ok:
            missing.append("input")

        profile_ok = bool(self._selected_profile_path) and Path(self._selected_profile_path).exists()
        if not profile_ok:
            missing.append("profile")

        schema_ok = bool(getattr(self._selected_profile_validation, "is_valid", False))
        if not schema_ok:
            missing.append("schema")

        date_ok = bool(self._user_date_valid)
        if not date_ok:
            missing.append("user_date")

        ready = not missing
        hint = "Ready" if ready else ("Not ready: " + ", ".join(missing))

        if ready != self._run_ready:
            self._run_ready = ready
            self.run_ready_changed.emit(self._run_ready)

        if hint != self._run_ready_hint:
            self._run_ready_hint = hint
            self.run_ready_hint_changed.emit(self._run_ready_hint)
