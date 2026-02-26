"""Profile runner view.

Scope (P4.4 → P4.10):
- Settings + deterministic profile discovery.
- Schema-only preview/validation + compile gate.
- Run selected / run all: backend execution off-thread; sanitized results.

LOCKS:
- Backend access only via backend_facade.
- Never display cell values or dataframe dumps.
"""

from __future__ import annotations

import os
import re
import unicodedata
from pathlib import Path
from typing import Any

from PySide6.QtWidgets import QAbstractItemView, QFileDialog
from PySide6.QtWidgets import QComboBox
from PySide6.QtWidgets import QGroupBox, QHBoxLayout, QLabel, QListWidget, QListWidgetItem, QPushButton
from PySide6.QtWidgets import QVBoxLayout, QWidget
from PySide6.QtCore import Qt
from PySide6.QtCore import QThread
from PySide6.QtCore import QTimer

from .. import strings_sv as sv
from ..services import backend_facade
from ..services.dto import (
    BatchRunItem,
    BatchRunSummary,
    CompileResult,
    MessageItem,
    ProfilePreview,
    ProfileRef,
    RunResult,
    StageEvent,
    ValidationReport,
    compute_batch_status,
)
from ..services.safe_errors import sanitize_message_item, sanitize_text, safe_user_error
from ..services.result_formatters import MAX_VISIBLE_MESSAGES, format_message_lines, truncate_messages
from ..services.settings_store import SettingsDTO, load_settings, save_settings
from ..services.problems_aggregator import ProblemItem, ProblemsSummary, aggregate_problems
from ..services.status_model import compute_correctness, compute_overlays
from ..services.batch_summary import compute_batch_counts, deterministic_reason
from ..services.workspace_root import find_workspace_root
from ..state import AppState
from ..workers.run_job_worker import RunJobWorker


class _ProfileListRowWidget(QWidget):
    """Two-line profile row with chip + overlay.

    P7 lock: only the highlighted row gets computed status/summary.
    Non-highlighted rows must show summary as "—".
    """

    def __init__(self, filename: str) -> None:
        super().__init__()

        # Important: this widget is installed into a QListWidget row via setItemWidget.
        # If it consumes mouse events, Ctrl/Shift multi-selection becomes unreliable.
        # Make it transparent so the underlying list view handles selection.
        self.setAttribute(Qt.WA_TransparentForMouseEvents, True)

        self._name = QLabel(filename)
        self._name.setStyleSheet("font-weight: 600;")

        self._summary = QLabel("—")
        self._summary.setStyleSheet("color: #666;")

        self._correctness = QLabel("—")
        self._correctness.setStyleSheet(
            "padding: 2px 6px; border-radius: 8px; border: 1px solid #bbb; color: #333;"
        )

        self._overlay = QLabel("")
        self._overlay.setStyleSheet(
            "padding: 2px 6px; border-radius: 8px; border: 1px solid #bbb; color: #333;"
        )

        left = QVBoxLayout()
        left.setContentsMargins(0, 0, 0, 0)
        left.setSpacing(2)
        left.addWidget(self._name)
        left.addWidget(self._summary)

        chips = QVBoxLayout()
        chips.setContentsMargins(0, 0, 0, 0)
        chips.setSpacing(4)
        chips.addWidget(self._correctness, 0)
        chips.addWidget(self._overlay, 0)
        chips.addStretch(1)

        root = QHBoxLayout()
        root.setContentsMargins(4, 2, 4, 2)
        root.setSpacing(10)
        root.addLayout(left, 1)
        root.addLayout(chips, 0)
        self.setLayout(root)

    def summary_text(self) -> str:
        return self._summary.text()

    def set_non_highlighted(self) -> None:
        self._summary.setText("—")
        self._correctness.setText("—")
        self._correctness.setStyleSheet(
            "padding: 2px 6px; border-radius: 8px; border: 1px solid #bbb; color: #333;"
        )
        self._overlay.setText("")
        self._overlay.setStyleSheet("padding: 2px 6px; border-radius: 8px; border: 1px solid #bbb; color: #333;")

    def set_highlighted(self, *, summary: str, correctness: str, overlay: str) -> None:
        self._summary.setText(summary)

        c = (correctness or "—").upper()
        self._correctness.setText(c)
        if c == "OK":
            self._correctness.setStyleSheet(
                "padding: 2px 6px; border-radius: 8px; background: #e8f5e9; border: 1px solid #66bb6a; color: #1b5e20;"
            )
        elif c == "WARN":
            self._correctness.setStyleSheet(
                "padding: 2px 6px; border-radius: 8px; background: #fff8e1; border: 1px solid #ffb300; color: #6d4c41;"
            )
        elif c == "FAIL":
            self._correctness.setStyleSheet(
                "padding: 2px 6px; border-radius: 8px; background: #ffebee; border: 1px solid #e57373; color: #b71c1c;"
            )
        else:
            self._correctness.setStyleSheet(
                "padding: 2px 6px; border-radius: 8px; border: 1px solid #bbb; color: #333;"
            )

        o = (overlay or "").strip().upper()
        self._overlay.setText(o)
        if o == "RUNNING":
            self._overlay.setStyleSheet(
                "padding: 2px 6px; border-radius: 8px; background: #e3f2fd; border: 1px solid #64b5f6; color: #0d47a1;"
            )
        elif o == "NOT_READY":
            self._overlay.setStyleSheet(
                "padding: 2px 6px; border-radius: 8px; background: #eeeeee; border: 1px solid #9e9e9e; color: #424242;"
            )
        elif o:
            self._overlay.setStyleSheet(
                "padding: 2px 6px; border-radius: 8px; border: 1px solid #bbb; color: #333;"
            )
        else:
            self._overlay.setStyleSheet("padding: 2px 6px; border-radius: 8px; border: 1px solid #bbb; color: #333;")


class ProfileRunnerView(QWidget):
    _DETAILS_VISIBLE_CAP = 10
    _COLUMN_MATCH_WORD_ALIASES: dict[str, str] = {
        "ink moms": "inkl moms",
    }

    def __init__(self, state: AppState) -> None:
        super().__init__()
        self._state = state
        self._run_thread: QThread | None = None
        self._batch_thread: QThread | None = None
        self._batch_queue: list[ProfileRef] = []
        self._batch_index: int = 0
        self._batch_items: list[BatchRunItem] = []
        self._last_run_warnings: list[MessageItem] = []
        self._last_run_fatals: list[MessageItem] = []

        # P8 Failure guidance: apply once per result object.
        self._last_guided_run_result: object | None = None
        self._last_guided_batch_result: object | None = None

        # P7 Phase 1 health dashboard: selected-only row updates.
        self._profile_row_widgets: dict[int, _ProfileListRowWidget] = {}
        self._highlighted_profile_row: int | None = None

        # P7 outcome-first Problems box (UI-only; derived from existing DTO state).
        self._problems_display_items: list[ProblemItem] = []
        self._details_problem_lines: list[str] = []
        self._profile_json_cache: dict[str, dict[str, Any]] = {}
        self._details_mode: str = "overview"
        self._run_follow_items: list[dict[str, str]] = []
        self._run_follow_active_index: int | None = None
        self._run_follow_done: bool = False

        # UI-only: suppress backend work triggered by programmatic selection changes.
        self._suppress_profile_selected_backend: bool = False

        self._workspace = find_workspace_root()
        self._default_profiles_dir = (self._workspace.root / "profiles").resolve()

        # P10.2: one-level profile group navigation (UI-only; not persisted).
        self._suppress_group_changed: bool = False
        self._current_group_dir: Path | None = None

        self._settings = load_settings(on_notice=self._state.add_notice)

        root = QVBoxLayout()
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(10)

        title = QLabel(sv.PROFILE_RUNNER_TITLE)
        title.setStyleSheet("font-weight: 600;")
        subtitle = QLabel(sv.PROFILE_RUNNER_SUBTITLE)
        subtitle.setWordWrap(True)

        content = QHBoxLayout()
        content.setSpacing(10)

        profiles_box = QGroupBox(sv.PROFILES_GROUP_TITLE)
        profiles_layout = QVBoxLayout()
        profiles_layout.setSpacing(8)

        dir_row = QHBoxLayout()
        dir_row.setSpacing(8)

        self._profiles_dir_label = QLabel(sv.PROFILES_DIR_UNSET)
        self._profiles_dir_label.setWordWrap(False)

        self._choose_profiles_btn = QPushButton(sv.CHOOSE_PROFILES_FOLDER)
        self._reset_profiles_btn = QPushButton(sv.RESET_PROFILES_DEFAULT)

        dir_row.addWidget(self._profiles_dir_label, 1)
        dir_row.addWidget(self._choose_profiles_btn)
        dir_row.addWidget(self._reset_profiles_btn)

        group_row = QHBoxLayout()
        group_row.setSpacing(8)
        group_row.addWidget(QLabel("Group"))
        self._group_combo = QComboBox()
        self._group_combo.setEditable(False)
        self._group_combo.setMinimumContentsLength(18)
        self._back_to_root_btn = QPushButton("Back to root")
        self._back_to_root_btn.setEnabled(False)
        group_row.addWidget(self._group_combo, 1)
        group_row.addWidget(self._back_to_root_btn)

        self._profiles_note = QLabel("")
        self._profiles_note.setWordWrap(True)
        self._profiles_note.setStyleSheet("color: #666;")

        self._profiles_list = QListWidget()
        # Allow selecting N profiles for "Run selected profiles".
        # The highlighted profile (current row) still drives the right-pane preview.
        self._profiles_list.setSelectionMode(QAbstractItemView.ExtendedSelection)

        profiles_layout.addLayout(dir_row)
        profiles_layout.addLayout(group_row)
        profiles_layout.addWidget(self._profiles_note)
        profiles_layout.addWidget(self._profiles_list, 1)
        profiles_box.setLayout(profiles_layout)

        preview_box = QGroupBox("Details")
        preview_layout = QVBoxLayout()
        preview_layout.setContentsMargins(8, 8, 8, 8)
        preview_layout.setSpacing(8)

        # P7: outcome-first header above tabs (UI-only).
        self._outcome_header = QWidget()
        _oh = QVBoxLayout()
        _oh.setContentsMargins(0, 0, 0, 0)
        _oh.setSpacing(6)

        # A) Status line
        status_row = QHBoxLayout()
        status_row.setSpacing(8)

        self._outcome_correctness_chip = QLabel("—")
        self._outcome_correctness_chip.setStyleSheet(
            "padding: 3px 8px; border-radius: 10px; background: #eee; color: #333;"
        )
        self._outcome_overlays = QLabel("")
        self._outcome_overlays.setStyleSheet(
            "padding: 3px 8px; border-radius: 10px; background: #eee; color: #333;"
        )
        self._outcome_status_text = QLabel(sv.STATUS_READY)
        self._outcome_status_text.setStyleSheet("color: #333;")

        status_row.addWidget(self._outcome_correctness_chip)
        status_row.addWidget(self._outcome_overlays)
        status_row.addWidget(self._outcome_status_text, 1)
        _oh.addLayout(status_row)

        # Compact selected-profile details (top in right pane).
        self._details_box = QGroupBox(sv.DETAILS_SELECTED_TITLE)
        _db = QVBoxLayout()
        _db.setContentsMargins(8, 8, 8, 8)
        _db.setSpacing(6)

        self._details_list = QListWidget()
        self._details_list.setSelectionMode(QAbstractItemView.NoSelection)
        _db.addWidget(self._details_list, 1)

        self._details_more = QLabel("")
        self._details_more.setStyleSheet("color: #666;")
        _db.addWidget(self._details_more)

        self._details_ok_btn = QPushButton(sv.DETAILS_RUNFOLLOW_OK)
        self._details_ok_btn.setVisible(False)
        _db.addWidget(self._details_ok_btn, 0)

        self._details_box.setLayout(_db)
        _oh.addWidget(self._details_box)

        # Problems box (always visible)
        self._problems_box = QGroupBox(sv.PROBLEMS_TITLE)
        _pb = QVBoxLayout()
        _pb.setContentsMargins(8, 8, 8, 8)
        _pb.setSpacing(6)

        counts_row = QHBoxLayout()
        counts_row.setSpacing(8)
        self._problems_counts = QLabel(sv.PROBLEMS_COUNTS_FMT.format(fatals=0, warnings=0))
        self._problems_counts.setStyleSheet("color: #333;")
        counts_row.addWidget(self._problems_counts, 1)
        _pb.addLayout(counts_row)

        self._problems_list = QListWidget()
        self._problems_list.setSelectionMode(QAbstractItemView.NoSelection)
        _pb.addWidget(self._problems_list, 1)

        self._problems_more = QLabel("")
        self._problems_more.setStyleSheet("color: #666;")
        _pb.addWidget(self._problems_more)

        self._problems_box.setLayout(_pb)
        _oh.addWidget(self._problems_box)

        self._outcome_header.setLayout(_oh)
        preview_layout.addWidget(self._outcome_header, 0)
        preview_box.setLayout(preview_layout)

        content.addWidget(profiles_box, 1)
        content.addWidget(preview_box, 1)

        actions = QHBoxLayout()
        self._run_hint = QLabel("")
        self._run_hint.setWordWrap(False)
        self._run_hint.setStyleSheet("color: #666;")

        self._stage_label = QLabel("stage: —")
        self._stage_label.setStyleSheet("color: #333;")

        self._run_selected_multi_btn = QPushButton(sv.RUN_SELECTED_PROFILES)
        self._run_all_btn = QPushButton(sv.RUN_ALL_PROFILES)
        self._run_selected_multi_btn.setEnabled(False)
        self._run_all_btn.setEnabled(False)

        actions.addWidget(self._run_hint, 0)
        actions.addWidget(self._stage_label, 0)
        actions.addStretch(1)
        actions.addWidget(self._run_selected_multi_btn)
        actions.addWidget(self._run_all_btn)

        root.addWidget(title)
        root.addWidget(subtitle)
        root.addLayout(content, 1)
        root.addLayout(actions)
        self.setLayout(root)

        self._choose_profiles_btn.clicked.connect(self._on_choose_profiles_dir)
        self._reset_profiles_btn.clicked.connect(self._on_reset_profiles_dir)
        self._group_combo.currentIndexChanged.connect(lambda _i: self._on_group_changed())
        self._back_to_root_btn.clicked.connect(self._on_back_to_root_clicked)
        self._profiles_list.currentRowChanged.connect(self._on_profile_selected)
        self._run_selected_multi_btn.clicked.connect(self._on_run_selected_multi_clicked)
        self._run_all_btn.clicked.connect(self._on_run_all_clicked)
        self._details_ok_btn.clicked.connect(self._on_run_follow_ok_clicked)

        self._apply_profiles_dir_from_settings_or_default(initial=True)

        # Initialize group selector UI for the effective profiles root.
        try:
            root_dir = Path((self._state.profiles_dir or "").strip())
            if not str(root_dir).strip():
                root_dir = self._default_profiles_dir
        except Exception:
            root_dir = self._default_profiles_dir
        self._refresh_group_selector(root_dir)
        self._set_group_root_view()

        # State -> highlighted row status only
        self._state.selected_profile_preview_changed.connect(lambda _p: self._refresh_highlighted_profile_row_ui())
        self._state.selected_profile_validation_changed.connect(lambda _r: self._refresh_highlighted_profile_row_ui())
        self._state.compile_result_changed.connect(lambda _r: self._refresh_highlighted_profile_row_ui())
        self._state.run_busy_changed.connect(lambda _b: self._refresh_highlighted_profile_row_ui())
        self._state.run_result_changed.connect(lambda _r: self._refresh_highlighted_profile_row_ui())
        self._state.run_ready_changed.connect(lambda _b: self._refresh_highlighted_profile_row_ui())

        # P4.6: readiness gating (no runs yet)
        self._state.run_ready_changed.connect(self._on_run_ready_changed)
        self._state.run_ready_hint_changed.connect(self._on_run_ready_hint_changed)
        self._on_run_ready_changed(self._state.run_ready)
        self._on_run_ready_hint_changed(self._state.run_ready_hint)

        self._state.run_stage_changed.connect(self._render_stage)
        self._render_stage(self._state.run_stage)

        # Outcome header refresh (UI-only; no backend calls).
        self._state.input_path_changed.connect(lambda _p: self._refresh_outcome_header_ui())
        self._state.selected_profile_path_changed.connect(lambda _p: self._refresh_outcome_header_ui())
        self._state.selected_profile_validation_changed.connect(lambda _r: self._refresh_outcome_header_ui())

        # P9.5a: creator requests runner list refresh + optional select.
        self._state.profiles_refresh_requested.connect(self._on_profiles_refresh_requested)
        self._state.compile_result_changed.connect(lambda _r: self._refresh_outcome_header_ui())
        self._state.run_busy_changed.connect(lambda _b: self._refresh_outcome_header_ui())
        self._state.run_result_changed.connect(lambda _r: self._refresh_outcome_header_ui())
        self._state.batch_run_result_changed.connect(lambda _r: self._refresh_outcome_header_ui())
        self._state.run_ready_changed.connect(lambda _b: self._refresh_outcome_header_ui())
        self._state.user_date_valid_changed.connect(lambda _b: self._refresh_outcome_header_ui())
        self._refresh_outcome_header_ui()

        self._state.run_result_changed.connect(self._render_run_result)
        self._render_run_result(self._state.run_result)

        # Enable/disable Run All based on minimal required inputs.
        self._state.input_path_changed.connect(lambda _p: self._refresh_run_all_enabled())
        self._state.user_date_valid_changed.connect(lambda _b: self._refresh_run_all_enabled())
        self._state.profiles_list_changed.connect(lambda _l: self._refresh_run_all_enabled())
        self._state.run_busy_changed.connect(lambda _b: self._refresh_run_all_enabled())
        self._refresh_run_all_enabled()

        # Enable/disable Run Selected Profiles based on selection + required inputs.
        self._profiles_list.itemSelectionChanged.connect(lambda: self._refresh_run_selected_multi_enabled())
        self._profiles_list.itemSelectionChanged.connect(lambda: self._refresh_outcome_header_ui())
        self._state.input_path_changed.connect(lambda _p: self._refresh_run_selected_multi_enabled())
        self._state.user_date_valid_changed.connect(lambda _b: self._refresh_run_selected_multi_enabled())
        self._state.profiles_list_changed.connect(lambda _l: self._refresh_run_selected_multi_enabled())
        self._state.run_busy_changed.connect(lambda _b: self._refresh_run_selected_multi_enabled())
        self._refresh_run_selected_multi_enabled()

        self._state.selected_profile_dict_changed.connect(lambda _d: self._refresh_outcome_header_ui())
        self._state.xray_changed.connect(lambda _x: self._refresh_outcome_header_ui())

    def _on_run_ready_changed(self, ready: bool) -> None:
        # LOCK: Run buttons disabled until all readiness conditions are met.
        self._refresh_run_all_enabled()

    def _on_run_ready_hint_changed(self, hint: str) -> None:
        hint = (hint or "").strip()
        self._run_hint.setText(sanitize_text(hint))
        if hint == "Ready":
            self._run_hint.setStyleSheet("color: #1e7a34;")
        else:
            self._run_hint.setStyleSheet("color: #666;")

    def _set_controls_running(self, running: bool) -> None:
        running = bool(running)
        # Disable profile selection + controls while running.
        self._choose_profiles_btn.setEnabled(not running)
        self._reset_profiles_btn.setEnabled(not running)
        # P10.3: prevent group scope changes mid-run.
        self._group_combo.setEnabled(not running)
        self._back_to_root_btn.setEnabled((not running) and (self._current_group_dir is not None))
        self._profiles_list.setEnabled(not running)
        self._refresh_run_all_enabled(running=running)

    def _refresh_run_all_enabled(self, *, running: bool | None = None) -> None:
        if running is None:
            running = bool(self._state.run_busy)

        has_input = bool((self._state.input_path or "").strip())
        has_user_date = bool(self._state.user_date_valid)
        has_profiles = len(self._state.profiles_list) > 0

        # P4.10 lock: Run-all is allowed even if a selected profile is invalid,
        # but we require file + user_date and at least one discovered profile.
        can = bool((not running) and has_input and has_user_date and has_profiles)
        self._run_all_btn.setEnabled(can)

    def _refresh_run_selected_multi_enabled(self, *, running: bool | None = None) -> None:
        if running is None:
            running = bool(self._state.run_busy)

        has_input = bool((self._state.input_path or "").strip())
        has_user_date = bool(self._state.user_date_valid)
        has_profiles = len(self._state.profiles_list) > 0
        has_selection = len(self._profiles_list.selectedItems()) > 0
        can = bool((not running) and has_input and has_user_date and has_profiles and has_selection)
        self._run_selected_multi_btn.setEnabled(can)

    def _focus_profile_row_ui_only(self, row: int) -> None:
        """Visually focus a row without triggering backend work.

        Used during sequential runs to keep the right pane aligned to the currently running profile.
        """

        if row < 0:
            return
        prev = self._suppress_profile_selected_backend
        self._suppress_profile_selected_backend = True
        try:
            self._profiles_list.setCurrentRow(row)
        finally:
            self._suppress_profile_selected_backend = prev

    def _on_run_all_clicked(self) -> None:
        # P4.10: sequential, continue-on-failure.
        if self._batch_thread is not None and self._batch_thread.isRunning():
            return
        if self._state.run_busy:
            return

        user_date = (self._state.user_date_text or "").strip()
        if not self._state.user_date_valid or not user_date:
            self._state.set_batch_run_result(
                BatchRunSummary(
                    batch_status="FAILED",
                    total_profiles=0,
                    succeeded=0,
                    failed=0,
                    items=[],
                )
            )
            return

        # Freeze deterministic queue at click time.
        # P10.3: Run-all must execute the profiles currently displayed (current group)
        # in the same deterministic order as shown.
        refs = list(self._state.profiles_list)

        self._batch_queue = refs
        self._batch_index = 0
        self._batch_items = []
        self._enter_run_follow_mode(refs)

        self._state.clear_batch_run_result()
        self._state.clear_run_stage()
        self._state.set_run_busy(True)
        self._set_controls_running(True)

        self._start_next_batch_item()

    def _on_run_selected_multi_clicked(self) -> None:
        # Run only selected profiles (order: list order for determinism).
        if self._batch_thread is not None and self._batch_thread.isRunning():
            return
        if self._state.run_busy:
            return

        user_date = (self._state.user_date_text or "").strip()
        if not self._state.user_date_valid or not user_date:
            return

        selected_rows = sorted({self._profiles_list.row(it) for it in self._profiles_list.selectedItems()})
        refs = list(self._state.profiles_list)
        picked: list[ProfileRef] = []
        for r in selected_rows:
            if 0 <= r < len(refs):
                picked.append(refs[r])
        if not picked:
            return

        self._batch_queue = picked
        self._batch_index = 0
        self._batch_items = []
        self._enter_run_follow_mode(picked)

        self._state.clear_batch_run_result()
        self._state.clear_run_stage()
        self._state.set_run_busy(True)
        self._set_controls_running(True)

        self._start_next_batch_item()

    def _start_next_batch_item(self) -> None:
        # Finish condition.
        if self._batch_index >= len(self._batch_queue):
            total = len(self._batch_queue)
            succeeded = sum(1 for it in self._batch_items if it.status == "success")
            failed = total - succeeded
            status = compute_batch_status(total_profiles=total, succeeded=succeeded, failed=failed)
            self._state.set_batch_run_result(
                BatchRunSummary(
                    batch_status=status,
                    total_profiles=total,
                    succeeded=succeeded,
                    failed=failed,
                    items=list(self._batch_items),
                )
            )
            self._state.set_run_busy(False)
            self._set_controls_running(False)
            self._batch_queue = []
            self._batch_index = 0
            self._mark_run_follow_done()
            return

        current_index = self._batch_index
        ref = self._batch_queue[self._batch_index]
        self._batch_index += 1

        # UI-only: focus the currently running profile in the list and right pane.
        # Do not trigger backend work from the selection change.
        try:
            idx = next((i for i, r in enumerate(list(self._state.profiles_list or [])) if r.path == ref.path), -1)
        except Exception:
            idx = -1
        if idx >= 0:
            self._focus_profile_row_ui_only(idx)
            self._profile_list_set_highlighted_row(idx)

        profile_name = str(ref.name or Path(ref.path).name)
        self._set_run_follow_active(current_index=current_index, profile_name=profile_name)

        # Stage: validating/compiling (best-effort milestones).
        self._state.set_run_stage(StageEvent(stage="VALIDATING_PROFILE", detail=f"{profile_name}"))

        # Step 1: load profile.
        try:
            profile_dict = backend_facade.load_profile(Path(ref.path))
        except backend_facade.BackendFacadeError:
            msg = sanitize_message_item(safe_user_error("PROFILE_LOAD", "Profile could not be loaded"))
            self._batch_items.append(
                BatchRunItem(
                    profile_name=profile_name,
                    status="failed",
                    output_dir="",
                    warnings_count=0,
                    fatals_count=1,
                    outputs=[],
                    messages=[msg],
                    reason="profile load",
                )
            )
            self._complete_run_follow_active(status="FAILED", output_dir="", reason="profile load")
            QTimer.singleShot(0, self._start_next_batch_item)
            return

        # Update the right pane preview for the currently running profile.
        try:
            preview = self._compute_preview(profile_name, profile_dict)
            self._state.set_selected_profile_path(ref.path)
            self._state.set_selected_profile_dict(profile_dict)
            self._state.set_selected_profile_preview(preview)
        except Exception:
            pass

        # Step 2: schema validate.
        report = backend_facade.validate_profile_schema(profile_dict)

        # Update the right pane validation (UI shows schema warnings/errors deterministically).
        try:
            self._state.set_selected_profile_validation(report)
        except Exception:
            pass
        if not report.is_valid or report.errors:
            messages: list[MessageItem] = []
            for m in list(report.warnings or []):
                messages.append(sanitize_message_item(m))
            for m in list(report.errors or []):
                messages.append(sanitize_message_item(m))

            self._batch_items.append(
                BatchRunItem(
                    profile_name=profile_name,
                    status="failed",
                    output_dir="",
                    warnings_count=len(list(report.warnings or [])),
                    fatals_count=len(list(report.errors or [])) or 1,
                    outputs=[],
                    messages=messages,
                    reason="schema errors",
                )
            )
            self._complete_run_follow_active(status="FAILED", output_dir="", reason="schema errors")
            QTimer.singleShot(0, self._start_next_batch_item)
            return

        # Step 3: compile.
        self._state.set_run_stage(StageEvent(stage="COMPILING_JOBSPEC", detail=f"{profile_name}"))
        compile_result, handle = backend_facade.compile_job_spec_with_handle(profile_dict, self._state.user_date_text)
        if not compile_result.success or handle is None:
            messages = [sanitize_message_item(m) for m in list(compile_result.messages or [])]
            if not messages:
                messages = [sanitize_message_item(safe_user_error("COMPILE", "Compile failed"))]
            self._batch_items.append(
                BatchRunItem(
                    profile_name=profile_name,
                    status="failed",
                    output_dir="",
                    warnings_count=0,
                    fatals_count=1,
                    outputs=[],
                    messages=messages,
                    reason="compile failed",
                )
            )
            self._complete_run_follow_active(status="FAILED", output_dir="", reason="compile failed")
            QTimer.singleShot(0, self._start_next_batch_item)
            return

        # Step 4: run job in a worker thread, then continue from callbacks.
        if self._batch_thread is not None and self._batch_thread.isRunning():
            return

        thread = QThread(self)
        worker = RunJobWorker(
            input_path=self._state.input_path,
            compiled_job_spec_handle=handle,
            output_base_dir=self._settings.output_base_dir,
            simulate=False,
        )
        worker.moveToThread(thread)

        def _cleanup() -> None:
            worker.deleteLater()
            thread.deleteLater()
            if self._batch_thread is thread:
                self._batch_thread = None

        def _on_stage(ev: object) -> None:
            # Prefix stage detail with profile name for clarity.
            try:
                detail = sanitize_text(getattr(ev, "detail", ""))
                stage = getattr(ev, "stage", "")
                self._update_run_follow_stage(stage=stage, detail=detail)
                self._state.set_run_stage(type(ev)(stage=stage, detail=f"{profile_name}: {detail}"))
            except Exception:
                self._state.set_run_stage(ev)  # type: ignore[arg-type]

        def _on_finished(result: object) -> None:
            rr: RunResult = result  # type: ignore[assignment]
            out_dir = str(getattr(rr, "output_dir", "") or "")
            outs = [p for p in list(getattr(rr, "outputs", []) or []) if isinstance(p, str) and p]
            warnings = list(getattr(rr, "warnings", []) or [])
            fatals = list(getattr(rr, "fatals", []) or [])

            warnings_msgs = [sanitize_message_item(m) for m in warnings]
            fatals_msgs = [sanitize_message_item(m) for m in fatals]
            reason = ""
            if fatals_msgs:
                reason = "run fatals"
            elif warnings_msgs:
                reason = "run warnings"

            self._batch_items.append(
                BatchRunItem(
                    profile_name=profile_name,
                    status=str(getattr(rr, "status", "failed")),
                    output_dir=out_dir,
                    outputs=outs,
                    warnings_count=len(warnings),
                    fatals_count=len(fatals),
                    messages=None,
                    reason=reason,
                    warnings=warnings_msgs,
                    fatals=fatals_msgs,
                )
            )
            final_status = "SUCCESS" if str(getattr(rr, "status", "failed")) == "success" else "FAILED"
            self._complete_run_follow_active(status=final_status, output_dir=out_dir, reason=reason)
            thread.quit()

        def _on_failed(msg: object) -> None:
            m: MessageItem = msg  # type: ignore[assignment]
            sm = sanitize_message_item(m)
            self._batch_items.append(
                BatchRunItem(
                    profile_name=profile_name,
                    status="failed",
                    output_dir="",
                    warnings_count=0,
                    fatals_count=1,
                    outputs=[],
                    messages=[sm],
                    reason="run fatals",
                )
            )
            self._complete_run_follow_active(status="FAILED", output_dir="", reason="run fatals")
            thread.quit()

        def _on_thread_finished() -> None:
            _cleanup()
            self._start_next_batch_item()

        def _on_stage_queued(ev: object) -> None:
            QTimer.singleShot(0, self, lambda ev=ev: _on_stage(ev))

        def _on_finished_queued(result: object) -> None:
            QTimer.singleShot(0, self, lambda result=result: _on_finished(result))

        def _on_failed_queued(msg: object) -> None:
            QTimer.singleShot(0, self, lambda msg=msg: _on_failed(msg))

        thread.started.connect(worker.run)
        worker.stage_changed.connect(_on_stage_queued)
        worker.finished.connect(_on_finished_queued)
        worker.failed.connect(_on_failed_queued)
        thread.finished.connect(_on_thread_finished)

        self._batch_thread = thread
        thread.start()

    def _enter_run_follow_mode(self, refs: list[ProfileRef]) -> None:
        self._details_mode = "run_follow"
        self._run_follow_done = False
        self._run_follow_active_index = None
        self._run_follow_items = [
            {
                "profile_name": str(ref.name or Path(ref.path).name),
                "status": "PENDING",
                "stage": "",
                "detail": "",
                "output_dir": "",
                "reason": "",
            }
            for ref in refs
        ]
        self._refresh_selected_profile_details_ui()

    def _set_run_follow_active(self, *, current_index: int, profile_name: str) -> None:
        self._run_follow_active_index = current_index
        if 0 <= current_index < len(self._run_follow_items):
            item = dict(self._run_follow_items[current_index])
            item["profile_name"] = sanitize_text(profile_name)
            item["status"] = "RUNNING"
            item["stage"] = "VALIDATING_PROFILE"
            item["detail"] = ""
            self._run_follow_items[current_index] = item
        self._refresh_selected_profile_details_ui()

    def _update_run_follow_stage(self, *, stage: object, detail: object) -> None:
        idx = self._run_follow_active_index
        if idx is None or idx < 0 or idx >= len(self._run_follow_items):
            return
        item = dict(self._run_follow_items[idx])
        item["stage"] = sanitize_text(str(stage or ""))
        item["detail"] = sanitize_text(str(detail or ""))
        self._run_follow_items[idx] = item
        self._refresh_selected_profile_details_ui()

    def _complete_run_follow_active(self, *, status: str, output_dir: str, reason: str) -> None:
        idx = self._run_follow_active_index
        if idx is None or idx < 0 or idx >= len(self._run_follow_items):
            return
        item = dict(self._run_follow_items[idx])
        item["status"] = sanitize_text(status or "FAILED")
        item["output_dir"] = sanitize_text(output_dir or "")
        item["reason"] = sanitize_text(reason or "")
        if item["status"] == "RUNNING":
            item["status"] = "SUCCESS"
        self._run_follow_items[idx] = item
        self._run_follow_active_index = None
        self._refresh_selected_profile_details_ui()

    def _mark_run_follow_done(self) -> None:
        if self._details_mode != "run_follow":
            return
        self._run_follow_done = True
        self._refresh_selected_profile_details_ui()

    def _on_run_follow_ok_clicked(self) -> None:
        self._details_mode = "overview"
        self._run_follow_done = False
        self._run_follow_active_index = None
        self._run_follow_items = []
        self._refresh_outcome_header_ui()

    def _selected_profile_refs(self) -> list[ProfileRef]:
        refs = list(self._state.profiles_list or [])
        if not refs:
            return []
        try:
            rows = sorted({self._profiles_list.row(it) for it in self._profiles_list.selectedItems()})
        except Exception:
            rows = []
        selected: list[ProfileRef] = []
        for row in rows:
            if 0 <= row < len(refs):
                selected.append(refs[row])
        return selected

    @staticmethod
    def _collect_keep_columns(workbooks: list[dict[str, Any]]) -> list[str]:
        required: set[str] = set()

        def _add(col: object) -> None:
            if isinstance(col, str):
                s = col.strip()
                if s:
                    required.add(s)

        for wb in workbooks:
            if not isinstance(wb, dict):
                continue

            column_policy = wb.get("column_policy")
            if not isinstance(column_policy, dict):
                continue
            keep_cols = column_policy.get("keep_cols")
            if isinstance(keep_cols, list):
                for col in keep_cols:
                    _add(col)

        return sorted(required)

    @classmethod
    def _normalize_column_for_match(cls, name: object) -> str:
        if not isinstance(name, str):
            return ""
        normalized = unicodedata.normalize("NFKC", name).replace("\u00A0", " ")
        normalized = " ".join(normalized.split()).casefold()
        for src, dst in cls._COLUMN_MATCH_WORD_ALIASES.items():
            normalized = normalized.replace(src, dst)
        normalized = re.sub(r"[\W_]+", "", normalized, flags=re.UNICODE)
        return normalized

    @classmethod
    def _compute_column_presence(cls, required: list[str], headers: set[str]) -> tuple[int, list[str]]:
        header_lookup = {cls._normalize_column_for_match(h) for h in headers if isinstance(h, str)}
        found = 0
        missing: list[str] = []
        for req in required:
            if cls._normalize_column_for_match(req) in header_lookup:
                found += 1
            else:
                missing.append(req)
        return found, missing

    @staticmethod
    def _format_contract_text(workbooks: list[dict[str, Any]]) -> str:
        for wb in workbooks:
            if not isinstance(wb, dict):
                continue
            contract_filter = wb.get("contract_filter")
            if not isinstance(contract_filter, dict):
                continue
            col = contract_filter.get("col")
            values = contract_filter.get("values")
            if not isinstance(col, str) or not col.strip():
                continue
            if not isinstance(values, list):
                values = []
            cleaned = [str(v) for v in values if isinstance(v, str)]
            values_text = ", ".join(cleaned) if cleaned else "—"
            return f"{sanitize_text(col.strip())}={sanitize_text(values_text)}"
        return "—"

    @staticmethod
    def _format_split_text(workbooks: list[dict[str, Any]]) -> str:
        for wb in workbooks:
            if not isinstance(wb, dict):
                continue
            split = wb.get("split")
            if not isinstance(split, dict):
                continue
            split_col = split.get("split_col")
            if not isinstance(split_col, str) or not split_col.strip():
                continue
            selected_values = split.get("selected_values")
            if isinstance(selected_values, list) and len(selected_values) > 0:
                return f"{sanitize_text(split_col.strip())} ({len(selected_values)})"
            return f"{sanitize_text(split_col.strip())} ({sv.DETAILS_SPLIT_FULL_LABEL})"
        return "—"

    @staticmethod
    def _format_missing_columns(missing: list[str]) -> str:
        capped = missing[:4]
        text = ", ".join(capped)
        extra = max(0, len(missing) - len(capped))
        if extra > 0:
            text = f"{text} +{extra}" if text else f"+{extra}"
        return text

    def _load_profile_dict_cached(self, ref: ProfileRef) -> dict[str, Any]:
        if ref.path == self._state.selected_profile_path and self._state.selected_profile_dict:
            return dict(self._state.selected_profile_dict)

        cached = self._profile_json_cache.get(ref.path)
        if isinstance(cached, dict):
            return dict(cached)

        try:
            loaded = backend_facade.load_profile(Path(ref.path))
        except backend_facade.BackendFacadeError:
            loaded = {}

        safe_loaded = loaded if isinstance(loaded, dict) else {}
        self._profile_json_cache[ref.path] = dict(safe_loaded)
        return dict(safe_loaded)

    def _refresh_selected_profile_details_ui(self) -> None:
        if self._details_mode == "run_follow":
            self._refresh_run_follow_details_ui()
            return

        selected = self._selected_profile_refs()
        self._details_list.clear()
        self._details_problem_lines = []
        self._details_box.setTitle(sv.DETAILS_SELECTED_TITLE)
        self._details_ok_btn.setVisible(False)

        if not selected:
            self._details_list.addItem(QListWidgetItem(sv.DETAILS_EMPTY))
            self._details_more.setText("")
            return

        headers = set(getattr(self._state.xray, "headers", ()) or ())
        visible = selected[: self._DETAILS_VISIBLE_CAP]
        hidden_count = max(0, len(selected) - len(visible))

        for ref in visible:
            profile_dict = self._load_profile_dict_cached(ref)
            workbooks_raw = profile_dict.get("workbooks") if isinstance(profile_dict, dict) else []
            workbooks = workbooks_raw if isinstance(workbooks_raw, list) else []

            sheet = "—"
            for wb in workbooks:
                if isinstance(wb, dict) and isinstance(wb.get("referenced_sheet"), str) and wb.get("referenced_sheet"):
                    sheet = sanitize_text(str(wb.get("referenced_sheet")))
                    break

            required = self._collect_keep_columns(workbooks)
            found, missing = self._compute_column_presence(required, headers)
            if missing:
                missing_text = self._format_missing_columns(missing)
                self._details_problem_lines.append(f"{ref.name}: {sv.DETAILS_MISSING_PREFIX} {missing_text}")

            tokens_count = 0
            summaries_count = 0
            for wb in workbooks:
                if not isinstance(wb, dict):
                    continue
                if isinstance(wb.get("tokens"), list):
                    tokens_count += len(wb.get("tokens") or [])
                if isinstance(wb.get("summaries"), list):
                    summaries_count += len(wb.get("summaries") or [])

            contract_text = self._format_contract_text(workbooks)
            split_text = self._format_split_text(workbooks)
            missing_count = max(0, len(required) - found)
            detail_line = "\n".join(
                [
                    f"{sanitize_text(ref.name)}",
                    f"  Kontrakt: {contract_text}",
                    f"  Split: {split_text}",
                    f"  Blad: {sheet}",
                    f"  Kolumner (keep_cols): {found}/{len(required)} ({missing_count} saknas)",
                    f"  Token: {tokens_count}    Summeringar: {summaries_count}",
                ]
            )
            self._details_list.addItem(QListWidgetItem(detail_line))

        self._details_more.setText(sv.DETAILS_MORE_FMT.format(count=hidden_count) if hidden_count > 0 else "")

    def _refresh_run_follow_details_ui(self) -> None:
        self._details_box.setTitle(sv.DETAILS_RUNFOLLOW_TITLE)
        self._details_list.clear()
        self._details_problem_lines = []

        if not self._run_follow_items:
            self._details_list.addItem(QListWidgetItem(sv.DETAILS_EMPTY))
            self._details_more.setText("")
            self._details_ok_btn.setVisible(False)
            return

        succeeded = sum(1 for it in self._run_follow_items if it.get("status") == "SUCCESS")
        failed = sum(1 for it in self._run_follow_items if it.get("status") == "FAILED")
        total = len(self._run_follow_items)
        done = succeeded + failed

        for idx, it in enumerate(self._run_follow_items, start=1):
            profile_name = sanitize_text(it.get("profile_name", "") or "") or "—"
            status = sanitize_text(it.get("status", "") or "") or "PENDING"
            stage = sanitize_text(it.get("stage", "") or "")
            detail = sanitize_text(it.get("detail", "") or "")
            output_dir = sanitize_text(it.get("output_dir", "") or "")
            reason = sanitize_text(it.get("reason", "") or "")

            output_hint = "—"
            if output_dir:
                output_hint = sanitize_text(os.path.basename(output_dir.rstrip("\\/")) or output_dir)

            stage_text = stage if not detail else f"{stage} ({detail})"
            lines = [
                f"{idx}. {profile_name}",
                f"  Status: {status}",
                f"  Stage: {stage_text or '—'}",
                f"  Output: {output_hint}",
            ]
            if reason:
                lines.append(f"  Reason: {reason}")

            item = QListWidgetItem("\n".join(lines))
            if output_dir:
                item.setToolTip(output_dir)
            self._details_list.addItem(item)

        if self._run_follow_done:
            self._details_more.setText(f"Klar: {done}/{total} • success: {succeeded} • failed: {failed}")
            self._details_ok_btn.setVisible(True)
        else:
            self._details_more.setText(f"Kör: {done}/{total} klara")
            self._details_ok_btn.setVisible(False)

    def _refresh_problems_box_ui(self) -> None:
        """Refresh the Problems box from selected-profile state only."""

        # Default behavior: aggregate from selected profile state.
        summary: ProblemsSummary = aggregate_problems(
            schema_report=self._state.selected_profile_validation,
            compile_report=self._state.compile_result,
            run_report=self._state.run_result,
            cap=3,
        )
        extra_warning_count = len(self._details_problem_lines)
        self._problems_counts.setText(
            sv.PROBLEMS_COUNTS_FMT.format(
                fatals=summary.fatals_total,
                warnings=summary.warnings_total + extra_warning_count,
            )
        )

        self._problems_list.clear()
        self._problems_display_items = list(summary.display_items)
        if not self._problems_display_items and not self._details_problem_lines:
            self._problems_list.addItem(QListWidgetItem(sv.PROBLEMS_EMPTY))
        else:
            for line in self._details_problem_lines:
                self._problems_list.addItem(QListWidgetItem(sanitize_text(line)))
            for p in self._problems_display_items:
                self._problems_list.addItem(QListWidgetItem(p.display_text()))

        self._problems_more.setText(
            sv.PROBLEMS_MORE_FMT.format(count=summary.hidden_count) if summary.hidden_count > 0 else ""
        )

    def _render_run_result(self, result: object) -> None:
        warnings = list(getattr(result, "warnings", []) or []) if result is not None else []
        fatals = list(getattr(result, "fatals", []) or []) if result is not None else []
        self._last_run_warnings = [sanitize_message_item(m) for m in warnings]
        self._last_run_fatals = [sanitize_message_item(m) for m in fatals]

        # P8 guidance: guide attention on success/failure.
        self._apply_failure_guidance_ui_only()

    def _set_problems_highlight(self, active: bool) -> None:
        if active:
            self._problems_box.setStyleSheet(
                "QGroupBox { border: 2px solid #a1262f; border-radius: 6px; }"
                "QGroupBox::title { color: #a1262f; font-weight: 600; }"
            )
        else:
            self._problems_box.setStyleSheet("")

    def _apply_failure_guidance_ui_only(self) -> None:
        """P8: Failure guidance (UI-only).

        Allowed: highlight Problems box.
        Forbidden: backend calls, implicit retry/compile/run, timers.
        """

        rr = self._state.run_result
        br = self._state.batch_run_result

        run_failed = bool(rr is not None and str(getattr(rr, "status", "")) != "success")
        batch_has_failures = False
        if br is not None:
            try:
                failed = int(getattr(br, "failed", 0) or 0)
                batch_has_failures = failed > 0
            except Exception:
                batch_has_failures = True

        # Highlight Problems if either run failure or batch failures exist.
        self._set_problems_highlight(bool(run_failed or batch_has_failures))
        if rr is not None:
            self._last_guided_run_result = rr
        if br is not None:
            self._last_guided_batch_result = br

    def _render_stage(self, stage: object) -> None:
        if stage is None:
            self._stage_label.setText("stage: —")
            return
        try:
            s = sanitize_text(getattr(stage, "stage", ""))
            detail = sanitize_text(getattr(stage, "detail", ""))
            if detail:
                text = f"stage: {s} ({detail})"
                self._stage_label.setText(text)
            else:
                text = f"stage: {s}"
                self._stage_label.setText(text)
        except Exception:
            self._stage_label.setText("stage: —")

    def _apply_profiles_dir_from_settings_or_default(self, *, initial: bool) -> None:
        # Determine effective profiles dir.
        configured = (self._settings.profiles_dir or "").strip()
        effective: Path
        if configured:
            p = Path(configured)
            if p.exists() and p.is_dir():
                effective = p.resolve()
            else:
                effective = self._default_profiles_dir
        else:
            effective = self._default_profiles_dir

        if self._workspace.used_fallback and initial:
            self._profiles_note.setText(sanitize_text("Workspace root marker not found; using current directory fallback."))
        else:
            self._profiles_note.setText("")

        self._set_profiles_dir(effective)

    def _set_profiles_dir(self, profiles_dir: Path) -> None:
        profiles_dir = profiles_dir.resolve()
        basename = profiles_dir.name or str(profiles_dir)
        self._profiles_dir_label.setText(f"Profiles folder: {basename}")
        self._profiles_dir_label.setToolTip(str(profiles_dir))

        self._state.set_profiles_dir(str(profiles_dir))

        # P10.2: update group selector and return to root view on root-dir changes.
        self._refresh_group_selector(profiles_dir)
        self._set_group_root_view()

    def _refresh_profiles_list(self, profiles_dir: Path) -> None:
        # No errors for missing folders; just show empty state.
        if not profiles_dir.exists() or not profiles_dir.is_dir():
            self._profiles_list.clear()
            self._state.set_profiles_list([])
            if not self._profiles_note.text():
                self._profiles_note.setText(sanitize_text("Profiles folder does not exist; showing empty list."))
            self._clear_selected_profile_state()
            return

        # UX note (no paths, no data). Only show when a group is selected.
        self._profiles_note.setText(self._profiles_note.text() if self._workspace.used_fallback else "")
        if (not self._workspace.used_fallback) and (self._current_group_dir is not None):
            self._profiles_note.setText(sanitize_text("Tip: Ctrl+Click selects multiple profiles. Shift+Click selects a range."))

        # P10.2 LOCK: profile list is profiles-only; caller passes the group dir.
        refs = backend_facade.list_profiles_in_dir(profiles_dir)
        self._state.set_profiles_list(refs)
        self._profile_json_cache = {}

        self._profiles_list.clear()
        self._profile_row_widgets = {}
        self._highlighted_profile_row = None
        for idx, ref in enumerate(refs):
            # Custom row widgets render the visible text. Keep underlying item text empty
            # to avoid duplicate rendering artifacts.
            item = QListWidgetItem("")
            # P7 privacy/UI lock: do not show paths beyond basenames.
            item.setToolTip(ref.name)
            self._profiles_list.addItem(item)

            row_widget = _ProfileListRowWidget(ref.name)
            row_widget.set_non_highlighted()
            item.setSizeHint(row_widget.sizeHint())
            self._profiles_list.setItemWidget(item, row_widget)
            self._profile_row_widgets[idx] = row_widget

        # Keep selection deterministic: no auto-select.
        self._clear_selected_profile_state()

    def _clear_selected_profile_state(self) -> None:
        # Mirror the empty-selection branch of _on_profile_selected.
        self._state.set_selected_profile_path("")
        self._state.set_selected_profile_dict({})
        self._state.set_selected_profile_validation(ValidationReport(is_valid=False, warnings=[], errors=[]))
        self._state.set_selected_profile_preview(
            ProfilePreview(
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
        )
        self._details_problem_lines = []
        self._refresh_highlighted_profile_row_ui()

    def _refresh_group_selector(self, profiles_root: Path) -> None:
        # Policy B (LOCK): root option is "(All groups)" and shows nothing.
        # No persistence: group selection is UI-only.
        self._suppress_group_changed = True
        try:
            self._group_combo.clear()
            self._group_combo.addItem("(All groups)", None)
            for g in backend_facade.list_profile_groups(profiles_root):
                # Display name is folder basename only.
                self._group_combo.addItem(g.name, g.path)
        finally:
            self._suppress_group_changed = False

    def _set_group_root_view(self) -> None:
        # P10.3: group navigation changes scope; clear stale batch summary.
        self._state.clear_batch_run_result()
        self._current_group_dir = None
        self._back_to_root_btn.setEnabled(False)
        # Ensure combo is on root option (index 0).
        self._suppress_group_changed = True
        try:
            if self._group_combo.count() > 0:
                self._group_combo.setCurrentIndex(0)
        finally:
            self._suppress_group_changed = False

        self._profiles_list.clear()
        self._state.set_profiles_list([])
        if not self._workspace.used_fallback:
            self._profiles_note.setText(sanitize_text("Select a group to view profiles."))
        self._clear_selected_profile_state()

    def _on_back_to_root_clicked(self) -> None:
        self._set_group_root_view()

    def _on_group_changed(self) -> None:
        if self._suppress_group_changed:
            return

        # P10.3: group changes during a run are not allowed (selector is disabled while running).
        if self._state.run_busy:
            return

        data = self._group_combo.currentData()
        if not isinstance(data, str) or not data.strip():
            self._set_group_root_view()
            return

        group_dir = Path(data).resolve()

        # P10.3: changing group changes scope; clear stale batch summary.
        self._state.clear_batch_run_result()
        self._current_group_dir = group_dir
        self._back_to_root_btn.setEnabled(True)
        self._refresh_profiles_list(group_dir)

    def _on_profiles_refresh_requested(self, select_basename: str | None) -> None:
        target = (select_basename or "").strip()

        # If a group was provided ("group/file.json"), switch group first.
        group_hint = ""
        file_hint = target
        if target and ("/" in target or "\\" in target):
            parts = [p for p in re.split(r"[\\/]", target) if p]
            if len(parts) >= 2:
                group_hint = parts[0]
                file_hint = parts[-1]

        if group_hint:
            wanted_group = group_hint.lower()
            for i in range(self._group_combo.count()):
                label = str(self._group_combo.itemText(i) or "").strip().lower()
                if label == wanted_group:
                    self._group_combo.setCurrentIndex(i)
                    break

        # Refresh current view.
        if self._current_group_dir is None:
            self._set_group_root_view()
        else:
            self._refresh_profiles_list(self._current_group_dir)

        # Selection is scoped to the current group; no background scanning across groups.
        if not file_hint:
            return

        refs = list(self._state.profiles_list or [])
        wanted = file_hint.lower()

        # Exact match by displayed name (basename in group view).
        for i, ref in enumerate(refs):
            if (ref.name or "").strip().lower() == wanted:
                self._profiles_list.setCurrentRow(i)
                return

    def _on_choose_profiles_dir(self) -> None:
        selected = QFileDialog.getExistingDirectory(self, "Choose Profiles Folder", str(self._default_profiles_dir))
        if not selected:
            return
        chosen = Path(selected).resolve()

        self._settings = SettingsDTO(profiles_dir=str(chosen), output_base_dir=self._settings.output_base_dir)
        save_settings(self._settings, on_notice=self._state.add_notice)
        self._set_profiles_dir(chosen)

    def _on_reset_profiles_dir(self) -> None:
        # Reset means: clear setting and use workspace_root/./profiles.
        self._settings = SettingsDTO(profiles_dir="", output_base_dir=self._settings.output_base_dir)
        save_settings(self._settings, on_notice=self._state.add_notice)
        self._set_profiles_dir(self._default_profiles_dir)

    def _on_profile_selected(self, row: int) -> None:
        self._profile_list_set_highlighted_row(row)

        # UI-only focus changes (e.g. during batch execution) must not trigger backend work.
        if self._suppress_profile_selected_backend:
            return

        profiles = self._state.profiles_list
        if row < 0 or row >= len(profiles):
            self._state.set_selected_profile_path("")
            self._state.set_selected_profile_dict({})
            self._state.set_selected_profile_validation(ValidationReport(is_valid=False, warnings=[], errors=[]))
            self._state.set_selected_profile_preview(
                ProfilePreview(
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
            )
            self._refresh_highlighted_profile_row_ui()
            return
        ref: ProfileRef = profiles[row]
        self._state.set_selected_profile_path(ref.path)

        # P4.5: profile JSON read only (no workbook touch).
        try:
            profile_dict = backend_facade.load_profile(Path(ref.path))
        except backend_facade.BackendFacadeError:
            profile_dict = {}
            report = ValidationReport(
                is_valid=False,
                warnings=[],
                errors=[safe_user_error("PROFILE_LOAD", "Profile could not be loaded")],
            )
            preview = self._compute_preview(ref.name, profile_dict)
            self._state.set_selected_profile_dict(profile_dict)
            self._state.set_selected_profile_validation(report)
            self._state.set_selected_profile_preview(preview)
            return

        report = backend_facade.validate_profile_schema(profile_dict)
        preview = self._compute_preview(ref.name, profile_dict)
        self._state.set_selected_profile_dict(profile_dict)
        self._state.set_selected_profile_validation(report)
        self._state.set_selected_profile_preview(preview)
        self._refresh_highlighted_profile_row_ui()

    def _profile_list_set_highlighted_row(self, row: int) -> None:
        """Update only the previously highlighted and newly highlighted rows.

        P7 lock: do not compute/update non-highlighted rows beyond deterministic defaults.
        """

        prev = self._highlighted_profile_row
        if prev is not None and prev in self._profile_row_widgets and prev != row:
            self._profile_row_widgets[prev].set_non_highlighted()

        profiles = self._state.profiles_list
        if row < 0 or row >= len(profiles):
            self._highlighted_profile_row = None
            return

        self._highlighted_profile_row = row

    def _refresh_highlighted_profile_row_ui(self) -> None:
        """Re-render the selected-only health dashboard for the highlighted row."""

        row = self._highlighted_profile_row
        if row is None:
            return
        widget = self._profile_row_widgets.get(row)
        if widget is None:
            return

        preview = self._state.selected_profile_preview
        validation = self._state.selected_profile_validation

        sheet = sanitize_text(getattr(preview, "referenced_sheet", "") or "") or "—"
        split = "—"
        if bool(getattr(preview, "split_configured", False)):
            split = sanitize_text(getattr(preview, "split_col", "") or "") or "—"

        w_count = len(list(getattr(validation, "warnings", []) or []))
        e_count = len(list(getattr(validation, "errors", []) or []))
        summary = f"sheet: {sheet} • split: {split} • w:{w_count} e:{e_count}"

        correctness = compute_correctness(validation, self._state.compile_result, self._state.run_result)

        # NOT_READY overlay is derived only from global input gating (not schema validity).
        has_valid_inputs = bool(self._state.input_path and self._state.selected_profile_path and self._state.user_date_valid)
        last_run_ok = bool(self._state.run_result is not None and getattr(self._state.run_result, "status", "") == "success")
        overlays = compute_overlays(
            self._state,
            is_running=bool(self._state.run_busy),
            has_valid_inputs=has_valid_inputs,
            last_run_ok=last_run_ok,
        )

        # Overlay badge display (Phase 1): show NOT_READY/RUNNING only.
        overlay_badge = ""
        if "NOT_READY" in overlays:
            overlay_badge = "NOT_READY"
        elif "RUNNING" in overlays:
            overlay_badge = "RUNNING"

        widget.set_highlighted(summary=summary, correctness=correctness, overlay=overlay_badge)

    def _status_text_from_state(self, *, overlays: set[str]) -> str:
        if "RUNNING" in overlays:
            return sv.STATUS_RUNNING
        if "NOT_READY" in overlays:
            return sv.STATUS_NOT_READY
        if self._state.batch_run_result is not None:
            status = sanitize_text(getattr(self._state.batch_run_result, "batch_status", "") or "")
            return sv.STATUS_FAILED if status == "FAILED" else sv.STATUS_DONE
        if self._state.run_result is not None:
            return sv.STATUS_FAILED if getattr(self._state.run_result, "status", "") == "failed" else sv.STATUS_DONE
        return sv.STATUS_READY

    def _refresh_outcome_header_ui(self) -> None:
        """Refresh outcome-first header and Problems box (UI-only)."""

        correctness = compute_correctness(
            self._state.selected_profile_validation,
            self._state.compile_result,
            self._state.run_result,
        )
        self._outcome_correctness_chip.setText(correctness)
        if correctness == "OK":
            self._outcome_correctness_chip.setStyleSheet(
                "padding: 3px 8px; border-radius: 10px; background: #e8f5e9; border: 1px solid #66bb6a; color: #1b5e20;"
            )
        elif correctness == "WARN":
            self._outcome_correctness_chip.setStyleSheet(
                "padding: 3px 8px; border-radius: 10px; background: #fff8e1; border: 1px solid #ffb300; color: #6d4c41;"
            )
        elif correctness == "FAIL":
            self._outcome_correctness_chip.setStyleSheet(
                "padding: 3px 8px; border-radius: 10px; background: #ffebee; border: 1px solid #e57373; color: #b71c1c;"
            )
        else:
            self._outcome_correctness_chip.setStyleSheet(
                "padding: 3px 8px; border-radius: 10px; background: #eee; color: #333;"
            )

        has_valid_inputs = bool(self._state.input_path and self._state.selected_profile_path and self._state.user_date_valid)
        last_run_ok = bool(self._state.run_result is not None and getattr(self._state.run_result, "status", "") == "success")
        overlays_set = compute_overlays(
            self._state,
            is_running=bool(self._state.run_busy),
            has_valid_inputs=has_valid_inputs,
            last_run_ok=last_run_ok,
        )
        ordered = [o for o in ("NOT_READY", "RUNNING", "DONE") if o in overlays_set]
        overlays_text = " ".join(ordered)
        self._outcome_overlays.setText(overlays_text)
        self._outcome_overlays.setVisible(bool(overlays_text))
        self._outcome_status_text.setText(self._status_text_from_state(overlays=set(ordered)))

        self._refresh_selected_profile_details_ui()
        self._refresh_problems_box_ui()

    def _compute_preview(self, profile_name: str, profile_dict: dict) -> ProfilePreview:
        # LOCK: static-only preview derived from profile JSON only.
        job_id = profile_dict.get("job_id") if isinstance(profile_dict, dict) else None
        export_label = profile_dict.get("export_label") if isinstance(profile_dict, dict) else None
        workbooks = profile_dict.get("workbooks") if isinstance(profile_dict, dict) else None
        if not isinstance(workbooks, list):
            workbooks = []

        referenced_sheet = ""
        split_col = ""
        split_configured = False
        tokens_count = 0
        summaries_count = 0
        templates: list[str] = []

        for wb in workbooks:
            if not isinstance(wb, dict):
                continue
            if not referenced_sheet and isinstance(wb.get("referenced_sheet"), str):
                referenced_sheet = wb.get("referenced_sheet") or ""
            if not split_col:
                split = wb.get("split")
                if isinstance(split, dict) and isinstance(split.get("split_col"), str):
                    split_col = split.get("split_col") or ""
            if isinstance(wb.get("tokens"), list):
                tokens_count += len(wb.get("tokens") or [])
            if isinstance(wb.get("summaries"), list):
                summaries_count += len(wb.get("summaries") or [])
            t = wb.get("workbook_name_template")
            if isinstance(t, str) and t:
                templates.append(t)

        split_configured = bool(split_col)

        supported = ["{YYYY_MM_DD}", "{client}"]
        used: list[str] = []
        joined_templates = "\n".join(templates)
        for ph in supported:
            if ph in joined_templates:
                used.append(ph)

        return ProfilePreview(
            profile_name=str(profile_name or ""),
            job_id=str(job_id) if isinstance(job_id, str) else "",
            export_label=str(export_label) if isinstance(export_label, str) else "",
            workbooks_count=len(workbooks),
            referenced_sheet=str(referenced_sheet or ""),
            split_configured=split_configured,
            split_col=str(split_col or ""),
            tokens_count=int(tokens_count),
            summaries_count=int(summaries_count),
            placeholders_supported=supported,
            placeholders_used=used,
        )


