"""Left navigation/banner widget.

Contains:
- File selector (Browse…)
- Status chip
- X-Ray output
"""

from __future__ import annotations

from typing import Optional

from PySide6.QtCore import Signal
import os

from PySide6.QtWidgets import (
    QFileDialog,
    QComboBox,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QVBoxLayout,
    QWidget,
)

from .. import strings_sv as sv
from ..services.xray_models import XRayResult
from .xray_view import XRayView


class LeftBanner(QWidget):
    file_selected = Signal(str)
    preview_sheet_selected = Signal(str)
    user_date_changed = Signal(str)

    def __init__(self) -> None:
        super().__init__()

        self._file_label = QLabel("No file selected")
        self._file_label.setWordWrap(True)
        self._path_label = QLabel("")
        self._path_label.setWordWrap(True)
        self._browse_btn = QPushButton("Browse…")
        self._browse_btn.setAutoDefault(True)
        self._browse_btn.setDefault(True)

        self._status_chip = QLabel("NO_FILE")
        self._status_chip.setStyleSheet("padding: 2px 8px; border: 1px solid #999; border-radius: 10px;")

        # P9.3: user_date lives with the input file (left banner).
        self._date_label = QLabel(sv.LEFT_INPUT_DATE_LABEL)
        self._date_input = QLineEdit()
        self._date_input.setPlaceholderText("YYYY-MM-DD or YYYY_MM_DD")
        self._date_input.setMaximumWidth(160)
        self._date_input.setClearButtonEnabled(True)

        self._date_hint = QLabel("")
        self._date_hint.setWordWrap(True)
        self._date_hint.setStyleSheet("color: #666;")

        # Compact gating line.
        self._ready_line = QLabel("Ready: File ✗  Profile ✗  Date ✗")
        self._ready_line.setWordWrap(True)
        self._ready_line.setStyleSheet("color: #666;")

        self._error_label = QLabel("")
        self._error_label.setWordWrap(True)
        self._error_label.hide()

        self._sheet_label = QLabel(sv.LEFT_XRAY_PREVIEW_SHEET_LABEL)
        self._sheet_combo = QComboBox()
        self._sheet_combo.setEnabled(False)
        self._sheet_label.hide()
        self._sheet_combo.hide()

        self._xray_view = XRayView()

        top_row = QHBoxLayout()
        top_row.setContentsMargins(0, 0, 0, 0)
        top_row.setSpacing(8)
        top_row.addWidget(self._browse_btn)
        top_row.addStretch(1)
        top_row.addWidget(self._status_chip)

        source_box = QGroupBox("Källa")
        source_layout = QVBoxLayout()
        source_layout.setContentsMargins(8, 10, 8, 8)
        source_layout.setSpacing(8)
        source_layout.addLayout(top_row)
        source_layout.addWidget(self._file_label)
        source_layout.addWidget(self._path_label)
        source_box.setLayout(source_layout)

        date_row = QHBoxLayout()
        date_row.setContentsMargins(0, 0, 0, 0)
        date_row.setSpacing(8)
        date_row.addWidget(self._date_label)
        date_row.addWidget(self._date_input)
        date_row.addStretch(1)

        context_box = QGroupBox("Körkontext")
        context_layout = QVBoxLayout()
        context_layout.setContentsMargins(8, 10, 8, 8)
        context_layout.setSpacing(8)
        context_layout.addLayout(date_row)
        context_layout.addWidget(self._date_hint)
        context_layout.addWidget(self._ready_line)
        context_layout.addWidget(self._error_label)
        context_box.setLayout(context_layout)

        xray_box = QGroupBox("X-Ray")
        xray_layout = QVBoxLayout()
        xray_layout.setContentsMargins(8, 10, 8, 8)
        xray_layout.setSpacing(8)
        xray_layout.addWidget(self._sheet_label)
        xray_layout.addWidget(self._sheet_combo)
        xray_layout.addWidget(self._xray_view, 1)
        xray_box.setLayout(xray_layout)

        layout = QVBoxLayout()
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(10)
        layout.addWidget(source_box, 0)
        layout.addWidget(context_box, 0)
        layout.addWidget(xray_box, 1)
        self.setLayout(layout)

        self._browse_btn.clicked.connect(self._on_browse)
        self._sheet_combo.currentTextChanged.connect(self._on_sheet_changed)
        self._date_input.textChanged.connect(lambda t: self.user_date_changed.emit((t or "").strip()))

    def set_user_date_text(self, text: str) -> None:
        prev = self._date_input.blockSignals(True)
        try:
            self._date_input.setText((text or "").strip())
        finally:
            self._date_input.blockSignals(prev)

    def set_user_date_valid(self, *, raw: str, normalized: str | None, valid: bool) -> None:
        raw = (raw or "").strip()
        if not raw:
            self._date_hint.setText("Date required (no default).")
            self._date_hint.setStyleSheet("color: #666;")
            return
        if valid and normalized:
            self._date_hint.setText(f"Using: {normalized}")
            self._date_hint.setStyleSheet("color: #1e7a34;")
        else:
            self._date_hint.setText("Invalid date. Use YYYY-MM-DD or YYYY_MM_DD")
            self._date_hint.setStyleSheet("color: #a1262f;")

    def set_readiness(self, *, file_ok: bool, profile_ok: bool, date_ok: bool) -> None:
        def _mark(ok: bool) -> str:
            return "✓" if ok else "✗"

        text = f"Ready: File {_mark(bool(file_ok))}  Profile {_mark(bool(profile_ok))}  Date {_mark(bool(date_ok))}"
        self._ready_line.setText(text)
        if bool(file_ok and profile_ok and date_ok):
            self._ready_line.setStyleSheet("color: #1e7a34;")
        else:
            self._ready_line.setStyleSheet("color: #666;")

    def _on_sheet_changed(self, name: str) -> None:
        n = (name or "").strip()
        if not n:
            return
        self.preview_sheet_selected.emit(n)

    def _on_browse(self) -> None:
        path, _ = QFileDialog.getOpenFileName(
            self,
            "Select input file",
            "",
            "Data files (*.csv *.xlsx *.xlsm);;All files (*.*)",
        )
        if path:
            self.file_selected.emit(path)

    def set_path(self, path: str) -> None:
        if not path:
            self._file_label.setText("No file selected")
            self._path_label.setText("")
            self._path_label.setToolTip("")
            return

        self._file_label.setText(os.path.basename(path))
        self._path_label.setText(path)
        self._path_label.setToolTip(path)

    def set_status(self, status: str) -> None:
        self._status_chip.setText(status)

    def set_error(self, message: str) -> None:
        msg = (message or "").strip()
        if not msg:
            self._error_label.hide()
            self._error_label.setText("")
            return
        self._error_label.setText(f"Error: {msg}")
        self._error_label.show()

    def set_busy(self, busy: bool) -> None:
        self._browse_btn.setEnabled(not busy)
        self._date_input.setEnabled(not busy)

    def set_xray(self, result: Optional[XRayResult]) -> None:
        self._xray_view.set_result(result)

    def set_sheet_picker(self, *, file_kind: str | None, sheets: list[str], selected: str | None) -> None:
        kind = (file_kind or "").strip().lower()
        if kind != "xlsx" or not sheets:
            self._sheet_label.hide()
            self._sheet_combo.hide()
            self._sheet_combo.setEnabled(False)
            self._sheet_combo.clear()
            return

        self._sheet_label.show()
        self._sheet_combo.show()
        self._sheet_combo.setEnabled(True)

        # Determinism: preserve workbook order as provided.
        prev_block = self._sheet_combo.blockSignals(True)
        try:
            self._sheet_combo.clear()
            self._sheet_combo.addItems(list(sheets))
            if selected and selected in sheets:
                self._sheet_combo.setCurrentText(selected)
            else:
                # Default to first item (UI should supply selected previewable sheet).
                self._sheet_combo.setCurrentIndex(0)
        finally:
            self._sheet_combo.blockSignals(prev_block)
