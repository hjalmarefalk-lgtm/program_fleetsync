"""Main content area with simple tabs.

This remains placeholder-only (no backend actions).
"""

from __future__ import annotations

from PySide6.QtWidgets import QStackedWidget, QTabBar, QVBoxLayout, QWidget

from ..state import AppState
from ..views.profile_creator_view import ProfileCreatorView
from ..views.profile_runner_view import ProfileRunnerView


class MainStack(QWidget):
    def __init__(self, state: AppState) -> None:
        super().__init__()

        self._tabs = QTabBar()
        self._tabs.addTab("Profile Runner")
        creator_tab = self._tabs.addTab("Create new profile…")
        self._tabs.setTabVisible(creator_tab, False)
        self._tabs.setExpanding(False)

        self._stack = QStackedWidget()
        self._runner = ProfileRunnerView(state)
        self._creator = ProfileCreatorView(state)
        self._stack.addWidget(self._runner)
        self._stack.addWidget(self._creator)
        self._stack.setCurrentWidget(self._runner)

        layout = QVBoxLayout()
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(10)
        layout.addWidget(self._tabs)
        layout.addWidget(self._stack, 1)
        self.setLayout(layout)

        self._tabs.currentChanged.connect(self._stack.setCurrentIndex)
