"""Top banner widget.

Contains program title and basic actions.
"""

from __future__ import annotations

from PySide6.QtCore import Signal
from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QComboBox,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QMessageBox,
    QPushButton,
    QWidget,
)


class TopBanner(QWidget):
    reload_requested = Signal()
    exit_requested = Signal()
    theme_changed = Signal(str)

    def __init__(self, *, title: str = "FleetSync UI") -> None:
        super().__init__()

        self._title_label = QLabel(title)
        self._title_label.setAlignment(Qt.AlignmentFlag.AlignHCenter | Qt.AlignmentFlag.AlignVCenter)
        self._reload_btn = QPushButton("Reload")
        self._theme_combo = QComboBox()
        self._settings_btn = QPushButton("Settings")
        self._exit_btn = QPushButton("Exit")

        self._theme_combo.addItems(["System", "Light", "Dark"])

        right = QWidget()
        right_layout = QHBoxLayout()
        right_layout.setContentsMargins(0, 0, 0, 0)
        right_layout.setSpacing(8)
        right_layout.addWidget(self._reload_btn)
        right_layout.addWidget(QLabel("Theme:"))
        right_layout.addWidget(self._theme_combo)
        right_layout.addWidget(self._settings_btn)
        right_layout.addWidget(self._exit_btn)
        right.setLayout(right_layout)

        layout = QGridLayout()
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setHorizontalSpacing(8)
        layout.setVerticalSpacing(0)

        # Column 0 and 2 expand symmetrically, keeping the title centered
        # regardless of how wide the right-side controls are.
        layout.setColumnStretch(0, 1)
        layout.setColumnStretch(1, 0)
        layout.setColumnStretch(2, 1)

        layout.addWidget(QWidget(), 0, 0)  # left spacer
        layout.addWidget(self._title_label, 0, 1)
        layout.addWidget(right, 0, 2, alignment=Qt.AlignmentFlag.AlignRight)
        self.setLayout(layout)

        self._reload_btn.clicked.connect(self.reload_requested.emit)
        self._exit_btn.clicked.connect(self.exit_requested.emit)
        self._theme_combo.currentTextChanged.connect(self.theme_changed.emit)
        self._settings_btn.clicked.connect(self._show_settings_placeholder)

    def set_busy(self, busy: bool) -> None:
        self._reload_btn.setEnabled(not busy)
        self._theme_combo.setEnabled(not busy)
        self._settings_btn.setEnabled(not busy)
        # Exit always enabled.
        self._exit_btn.setEnabled(True)

    def _show_settings_placeholder(self) -> None:
        QMessageBox.information(self, "Settings", "Settings are not implemented yet.")
