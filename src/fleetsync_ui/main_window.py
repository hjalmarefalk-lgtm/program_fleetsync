"""Main window.

Layout: Top banner + (Left banner | Main stack).
"""

from __future__ import annotations

from typing import Optional
from pathlib import Path

from PySide6.QtCore import QThread
from PySide6.QtGui import QCloseEvent
from PySide6.QtWidgets import QMainWindow, QSplitter, QWidget, QVBoxLayout

from .state import AppState
from .services.file_probe import detect_file_kind
from .services.probe_cache import compute_file_key
from .widgets.left_banner import LeftBanner
from .widgets.main_stack import MainStack
from .widgets.top_banner import TopBanner
from .workers.file_probe_worker import FileProbeWorker


class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()

        # UX Review Checklist (P3_UX_REVIEW_MODE)
        # - File selection clarity: basename shown, full path available (tooltip)
        # - X-ray readability: grouped headers only, scrollable, metadata visible
        # - State transitions: NO_FILE -> LOADING -> LOADED/ERROR
        # - Density: left panel readable; main area padded with clear headings
        # - Non-blocking: LOADING disables Browse/Reload until done
        self.setWindowTitle("FleetSync UI")
        # ~15% bigger than previous default (1100x700)
        self.resize(1265, 805)

        self.state = AppState()

        self._top = TopBanner(title="FleetSync UI")
        self._top.setFixedHeight(52)
        self._left = LeftBanner()
        # Left panel keeps ample room for file context + X-Ray readability.
        self._left.setMinimumWidth(320)
        self._main = MainStack(self.state)

        splitter = QSplitter()
        splitter.addWidget(self._left)
        splitter.addWidget(self._main)
        splitter.setStretchFactor(0, 1)
        splitter.setStretchFactor(1, 2)
        splitter.setSizes([380, 885])

        root = QWidget()
        layout = QVBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)
        layout.addWidget(self._top)
        layout.addWidget(splitter, 1)
        root.setLayout(layout)
        self.setCentralWidget(root)

        self._probe_thread: Optional[QThread] = None
        self._probe_seq: int = 0

        # Widget -> actions
        self._left.file_selected.connect(self.request_probe)
        self._left.preview_sheet_selected.connect(self._on_preview_sheet_selected)
        self._left.user_date_changed.connect(self.state.set_user_date_text)
        self._top.reload_requested.connect(self._on_reload)
        self._top.exit_requested.connect(self.close)
        self._top.theme_changed.connect(self._on_theme_changed)

        # State -> UI
        self.state.input_path_changed.connect(self._left.set_path)
        self.state.status_changed.connect(self._on_status_changed)
        self.state.xray_changed.connect(self._left.set_xray)
        self.state.input_file_kind_changed.connect(lambda _v: self._sync_sheet_picker())
        self.state.workbook_sheets_changed.connect(lambda _v: self._sync_sheet_picker())
        self.state.preview_sheet_name_changed.connect(lambda _v: self._sync_sheet_picker())
        self.state.run_busy_changed.connect(self._on_run_busy_changed)
        self.state.notices_changed.connect(self._on_notices_changed)

        # user_date UI wiring (P9.3)
        self.state.user_date_text_changed.connect(self._left.set_user_date_text)
        self.state.user_date_text_changed.connect(lambda _t: self._sync_user_date_hint())
        self.state.user_date_valid_changed.connect(lambda _b: self._sync_user_date_hint())

        # Left banner readiness line (UI-only)
        self.state.input_path_changed.connect(lambda _p: self._sync_readiness())
        self.state.selected_profile_path_changed.connect(lambda _p: self._sync_readiness())
        self.state.selected_profile_validation_changed.connect(lambda _r: self._sync_readiness())
        self.state.user_date_valid_changed.connect(lambda _b: self._sync_readiness())

        self._sync_user_date_hint()
        self._sync_readiness()

        self._on_status_changed(self.state.status.value)

    def _sync_user_date_hint(self) -> None:
        self._left.set_user_date_valid(
            raw=self.state.user_date_text,
            normalized=self.state.user_date_normalized,
            valid=self.state.user_date_valid,
        )

    def _sync_readiness(self) -> None:
        file_ok = bool(self.state.input_path) and Path(self.state.input_path).exists()

        profile_ok = bool(self.state.selected_profile_path) and Path(self.state.selected_profile_path).exists()
        profile_ok = bool(profile_ok and getattr(self.state.selected_profile_validation, "is_valid", False))

        date_ok = bool(self.state.user_date_valid)
        self._left.set_readiness(file_ok=file_ok, profile_ok=profile_ok, date_ok=date_ok)

    def _sync_sheet_picker(self) -> None:
        self._left.set_sheet_picker(
            file_kind=self.state.input_file_kind,
            sheets=self.state.workbook_sheets,
            selected=self.state.preview_sheet_name,
        )

    def _on_preview_sheet_selected(self, sheet_name: str) -> None:
        if not self.state.input_path:
            return
        # UI-only: changing X-ray preview must not clear compile/run results.
        self.state.set_preview_sheet_name(sheet_name)
        self.request_probe(self.state.input_path)

    def _on_notices_changed(self, notices: object) -> None:
        try:
            items = list(notices or [])
        except Exception:
            items = []
        if not items:
            self.statusBar().clearMessage()
            return
        latest = str(items[-1])
        # Non-modal, transient, safe message.
        self.statusBar().showMessage(latest, 8000)

    def _on_status_changed(self, status: str) -> None:
        self._left.set_status(status)
        self._update_busy(status)

        if status == "ERROR":
            self._left.set_error(self.state.last_error)
        else:
            self._left.set_error("")

    def _on_run_busy_changed(self, _busy: bool) -> None:
        # Keep file/user_date controls disabled while a run is active.
        self._update_busy(self.state.status.value)

    def _update_busy(self, status: str) -> None:
        busy = (status == "LOADING") or bool(self.state.run_busy)
        self._left.set_busy(busy)
        self._top.set_busy(busy)

    def request_probe(self, input_path: str) -> None:
        if self._probe_thread is not None and self._probe_thread.isRunning():
            # Keep behavior simple and deterministic: ignore while busy.
            return

        self._probe_seq += 1
        seq = self._probe_seq

        self.state.request_probe(input_path)
        if not self.state.input_path:
            return

        # Compute deterministic file identity key (used for in-memory selection).
        fk = compute_file_key(self.state.input_path)
        kind = detect_file_kind(self.state.input_path)
        if kind in ("xlsx", "csv"):
            self.state.set_input_file_kind(kind)
        else:
            self.state.set_input_file_kind(None)
        self.state.set_input_file_key(fk)

        # Restore sheet list + selection for unchanged files.
        if kind == "xlsx" and fk is not None:
            remembered_sheets = self.state.get_remembered_workbook_sheets(fk)
            if remembered_sheets is not None:
                self.state.set_workbook_sheets(remembered_sheets)
            remembered_sheet = self.state.get_remembered_preview_sheet(fk)
            if remembered_sheet:
                self.state.set_preview_sheet_name(remembered_sheet)
        elif kind == "csv":
            self.state.set_workbook_sheets([])
            self.state.set_preview_sheet_name(None)

        thread = QThread(self)
        worker = FileProbeWorker(
            input_path=self.state.input_path,
            sheet_name=(self.state.preview_sheet_name if kind == "xlsx" else None),
        )
        worker.moveToThread(thread)

        def _cleanup() -> None:
            worker.deleteLater()
            thread.deleteLater()
            if self._probe_thread is thread:
                self._probe_thread = None

        def _on_result(result: object) -> None:
            if seq != self._probe_seq:
                return
            self.state.on_probe_payload(result)
            thread.quit()

        def _on_error(message: str) -> None:
            if seq != self._probe_seq:
                return
            self.state.on_probe_error(message)
            thread.quit()

        thread.started.connect(worker.run)
        worker.result_ready.connect(_on_result)
        worker.error.connect(_on_error)
        thread.finished.connect(_cleanup)

        self._probe_thread = thread
        thread.start()

    def _on_reload(self) -> None:
        if not self.state.input_path:
            self.state.reload()
            return
        self.request_probe(self.state.input_path)

    def _on_theme_changed(self, theme: str) -> None:
        # Minimal implementation: store the value only.
        # (Palette switching can be added later without changing UX.)
        _ = theme

    def closeEvent(self, event: QCloseEvent) -> None:
        if self._probe_thread is not None and self._probe_thread.isRunning():
            self._probe_thread.quit()
            self._probe_thread.wait(300)
        super().closeEvent(event)
