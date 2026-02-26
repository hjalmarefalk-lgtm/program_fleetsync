"""Worker for running a job.

P4.9 scope:
- Real backend run execution off the UI thread.
- DTO-only across Qt signals (RunResult + MessageItem + StageEvent).
- Best-effort milestone stages only (no progress bar).
"""

from __future__ import annotations

from typing import Any, Optional

from PySide6.QtCore import QObject, Signal

from ..services import backend_facade
from ..services.dto import MessageItem, RunResult, StageEvent, StageName
from ..services.safe_errors import safe_user_error


class RunJobWorker(QObject):
    """Run worker that emits coarse milestone stages.
    """

    started = Signal()
    stage_changed = Signal(object)  # StageEvent
    finished = Signal(object)  # RunResult
    failed = Signal(object)  # MessageItem

    def __init__(
        self,
        *,
        input_path: str,
        compiled_job_spec_handle: object | None,
        precompiled: bool | None = None,
        profile_dict: dict[str, Any] | None = None,
        user_date: str | None = None,
        output_base_dir: Optional[str] = None,
        simulate: bool = False,
        dry_run: bool | None = None,
    ) -> None:
        super().__init__()
        self._input_path = input_path or ""
        self._job_spec_handle = compiled_job_spec_handle
        self._precompiled = bool(precompiled) if precompiled is not None else (compiled_job_spec_handle is not None)
        self._profile_dict = profile_dict
        self._user_date = (user_date or "").strip()
        self._output_base_dir = output_base_dir or ""
        self._failed_emitted = False
        # Back-compat for P4.8 tests/callers: `dry_run=True` means simulate.
        if dry_run is not None:
            self._simulate = bool(dry_run)
        else:
            self._simulate = bool(simulate)

    def run(self) -> None:
        self.started.emit()
        try:
            # Deterministic milestone emissions (no percent/progress promises).
            # These are best-effort: backend does not provide fine-grained hooks.
            if not self._precompiled:
                # Truthfulness lock: only emit stages for actions this worker actually performs.
                # If the caller did not provide a compiled handle, the worker must be provided
                # the profile dict + user_date so it can validate+compile.
                if self._profile_dict is None or not self._user_date:
                    self._emit_failed_once("Missing compile inputs")
                    self.finished.emit(
                        RunResult(
                            status="failed",
                            output_dir="",
                            outputs=[],
                            warnings=[],
                            fatals=[safe_user_error("RUN_GATE", "Prepare (compile) is required")],
                        )
                    )
                    return

                self._emit_stage("VALIDATING_PROFILE", "Schema")
                report = backend_facade.validate_profile_schema(self._profile_dict)
                if (not getattr(report, "is_valid", False)) or list(getattr(report, "errors", []) or []):
                    fatals = [m for m in list(getattr(report, "errors", []) or []) if isinstance(m, MessageItem)]
                    if not fatals:
                        fatals = [safe_user_error("PROFILE_SCHEMA", "Profile is invalid")]
                    self._emit_failed_once("Profile invalid")
                    self.finished.emit(
                        RunResult(
                            status="failed",
                            output_dir="",
                            outputs=[],
                            warnings=[m for m in list(getattr(report, "warnings", []) or []) if isinstance(m, MessageItem)],
                            fatals=fatals,
                        )
                    )
                    return

                self._emit_stage("COMPILING_JOBSPEC", "Compile")
                compile_result, handle = backend_facade.compile_job_spec_with_handle(self._profile_dict, self._user_date)
                if (not getattr(compile_result, "success", False)) or handle is None:
                    msgs = [m for m in list(getattr(compile_result, "messages", []) or []) if isinstance(m, MessageItem)]
                    if not msgs:
                        msgs = [safe_user_error("COMPILE", "Compile failed")]
                    self._emit_failed_once("Compile failed")
                    self.finished.emit(
                        RunResult(
                            status="failed",
                            output_dir="",
                            outputs=[],
                            warnings=[],
                            fatals=msgs,
                        )
                    )
                    return

                self._job_spec_handle = handle

            if self._simulate:
                self._emit_stage("RUNNING", "Simulate")
                self._emit_stage("WRITING_OUTPUT", "Simulate")
                self._emit_stage("VALIDATING_OUTPUT", "Simulate")
                self._emit_stage("DONE", "Finished")
                self.finished.emit(
                    RunResult(
                        status="success",
                        output_dir=str(self._output_base_dir or ""),
                        outputs=[],
                        warnings=[],
                        fatals=[],
                    )
                )
                return

            self._emit_stage("RUNNING", "Execute")
            result = backend_facade.run_job(
                input_path=self._input_path,
                job_spec_handle=self._job_spec_handle,
                output_base_dir=(self._output_base_dir or None),
            )

            # Backend run includes write + post-write validation.
            self._emit_stage("WRITING_OUTPUT", "Write")
            self._emit_stage("VALIDATING_OUTPUT", "Post-write")

            if getattr(result, "status", "failed") == "success":
                self._emit_stage("DONE", "Finished")
            else:
                self._emit_failed_once("Finished")

            self.finished.emit(result)
        except Exception:
            # Do not emit raw exception text; sanitize.
            self._emit_failed_once("Failed")
            self.failed.emit(safe_user_error("RUN_WORKER", "Run failed"))

    def _emit_stage(self, stage: StageName, detail: str) -> None:
        self.stage_changed.emit(StageEvent(stage=stage, detail=str(detail or "")))

    def _emit_failed_once(self, detail: str) -> None:
        if self._failed_emitted:
            return
        self._failed_emitted = True
        self._emit_stage("FAILED", str(detail or ""))
