"""Profile creator view (P9.5a skeleton).

LOCKS:
- UI-only. No workbook I/O.
- No backend calls during typing/preview.
- Schema-only validation on Save.
- Deterministic filenames + ordering.
"""

from __future__ import annotations

from pathlib import Path

from PySide6.QtWidgets import (
    QComboBox,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QPlainTextEdit,
    QPushButton,
)
from PySide6.QtWidgets import QVBoxLayout, QWidget

from ..services import backend_facade
from ..services.profile_creator import (
    build_profile_dict,
    choose_profile_filename,
    format_profile_json,
    make_group_aggregate_summary,
    save_profile_json,
    upsert_presence_token,
)
from ..services.safe_errors import sanitize_text
from ..services.workspace_root import find_workspace_root
from ..services.settings_store import load_settings
from ..services.xray_assisted import assisted_groups, columns_for_group
from ..services.xray_models import XRayGroup, XRayResult
from ..state import AppState


class ProfileCreatorView(QWidget):
    def __init__(self, state: AppState) -> None:
        super().__init__()
        self._state = state

        # Assisted draft state (UI-only).
        self._split_col: str = ""
        self._tokens: list[dict] = []
        self._summary_group_by: str = ""
        self._summary_sum_cols: list[str] = []

        self._workspace = find_workspace_root()
        self._default_profiles_dir = (self._workspace.root / "profiles").resolve()
        self._settings = load_settings(on_notice=self._state.add_notice)

        root = QVBoxLayout()
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(10)

        title = QLabel("Profile Creator")
        title.setStyleSheet("font-weight: 600;")
        subtitle = QLabel("Create a new profile JSON (schema-only validation; no workbook I/O).")
        subtitle.setWordWrap(True)

        form_box = QGroupBox("New profile")
        form = QFormLayout()
        self._job_id = QLineEdit()
        self._export_label = QLineEdit()
        self._referenced_sheet = QLineEdit()

        form.addRow("Job ID (required)", self._job_id)
        form.addRow("Export label (optional)", self._export_label)
        form.addRow("Referenced sheet (required)", self._referenced_sheet)
        form_box.setLayout(form)

        assisted_box = QGroupBox("Assisted (X-Ray)")
        av = QVBoxLayout()
        av.setContentsMargins(8, 8, 8, 8)
        av.setSpacing(6)

        self._assist_meta = QLabel("X-Ray: —")
        self._assist_meta.setWordWrap(True)
        self._assist_meta.setStyleSheet("color: #666;")

        sheet_row = QHBoxLayout()
        self._use_sheet_btn = QPushButton("Use X-Ray sheet as referenced sheet")
        sheet_row.addWidget(self._use_sheet_btn)
        sheet_row.addStretch(1)

        pick_row = QHBoxLayout()
        self._assist_filter = QLineEdit()
        self._assist_filter.setPlaceholderText("Filter columns…")
        self._assist_group = QComboBox()
        for g in assisted_groups():
            self._assist_group.addItem(g.label, g.key)
        pick_row.addWidget(QLabel("Group"))
        pick_row.addWidget(self._assist_group)
        pick_row.addStretch(1)
        pick_row.addWidget(QLabel("Filter"))
        pick_row.addWidget(self._assist_filter, 1)

        self._assist_columns = QListWidget()
        self._assist_columns.setUniformItemSizes(True)

        btn_row = QHBoxLayout()
        self._set_split_btn = QPushButton("Set split column")
        self._add_token_btn = QPushButton("Add token (presence)")
        self._set_group_by_btn = QPushButton("Set summary group_by")
        self._add_sum_metric_btn = QPushButton("Add summary sum metric")
        btn_row.addWidget(self._set_split_btn)
        btn_row.addWidget(self._add_token_btn)
        btn_row.addWidget(self._set_group_by_btn)
        btn_row.addWidget(self._add_sum_metric_btn)
        btn_row.addStretch(1)

        self._assist_selected = QLabel("Selected: —")
        self._assist_selected.setWordWrap(True)
        self._assist_selected.setStyleSheet("color: #666;")

        av.addWidget(self._assist_meta)
        av.addLayout(sheet_row)
        av.addLayout(pick_row)
        av.addWidget(self._assist_columns, 1)
        av.addLayout(btn_row)
        av.addWidget(self._assist_selected)
        assisted_box.setLayout(av)

        self._filename_label = QLabel("Filename: —")
        self._filename_label.setStyleSheet("color: #666;")
        self._save_dir_label = QLabel("Save folder: profiles")
        self._save_dir_label.setStyleSheet("color: #666;")

        preview_box = QGroupBox("JSON preview")
        pv = QVBoxLayout()
        pv.setContentsMargins(8, 8, 8, 8)
        pv.setSpacing(6)
        self._preview = QPlainTextEdit()
        self._preview.setReadOnly(True)
        self._preview.setPlainText("{}\n")
        self._preview.setMaximumBlockCount(2000)
        pv.addWidget(self._preview, 1)
        preview_box.setLayout(pv)

        self._status = QLabel("")
        self._status.setWordWrap(True)
        self._status.setStyleSheet("color: #666;")

        actions = QHBoxLayout()
        self._save_btn = QPushButton("Save profile")
        self._save_btn.setEnabled(False)
        actions.addStretch(1)
        actions.addWidget(self._save_btn)

        root.addWidget(title)
        root.addWidget(subtitle)
        root.addWidget(form_box, 1)
        root.addWidget(assisted_box, 1)
        root.addWidget(self._filename_label)
        root.addWidget(self._save_dir_label)
        root.addWidget(preview_box, 2)
        root.addWidget(self._status)
        root.addLayout(actions)
        self.setLayout(root)

        # Typing updates preview only (no backend calls).
        self._job_id.textChanged.connect(lambda _t: self._refresh_preview())
        self._export_label.textChanged.connect(lambda _t: self._refresh_preview())
        self._referenced_sheet.textChanged.connect(lambda _t: self._refresh_preview())
        self._save_btn.clicked.connect(self._on_save_clicked)

        # Assisted section is UI-only: reacts to X-ray and user browsing.
        self._state.xray_changed.connect(lambda _obj: self._refresh_assisted())
        self._assist_group.currentIndexChanged.connect(lambda _i: self._refresh_assisted())
        self._assist_filter.textChanged.connect(lambda _t: self._refresh_assisted())
        self._assist_columns.itemSelectionChanged.connect(lambda: self._refresh_assisted_buttons())
        self._use_sheet_btn.clicked.connect(self._on_use_xray_sheet)
        self._set_split_btn.clicked.connect(self._on_set_split_col)
        self._add_token_btn.clicked.connect(self._on_add_token)
        self._set_group_by_btn.clicked.connect(self._on_set_summary_group_by)
        self._add_sum_metric_btn.clicked.connect(self._on_add_summary_sum_metric)

        self._refresh_preview()
        self._refresh_assisted()

    def _current_xray(self) -> XRayResult | None:
        return self._state.xray

    def _current_group(self) -> XRayGroup:
        data = self._assist_group.currentData()
        if isinstance(data, XRayGroup):
            return data
        # Fallback: keep deterministic default.
        return XRayGroup.STRINGS

    def _selected_column_header(self) -> str:
        items = self._assist_columns.selectedItems()
        if not items:
            return ""
        return (items[0].text() or "").strip()

    def _refresh_assisted_buttons(self) -> None:
        has_xray = self._current_xray() is not None
        has_col = bool(self._selected_column_header())
        self._use_sheet_btn.setEnabled(has_xray)
        self._set_split_btn.setEnabled(has_xray and has_col)
        self._add_token_btn.setEnabled(has_xray and has_col)
        self._set_group_by_btn.setEnabled(has_xray and has_col)
        self._add_sum_metric_btn.setEnabled(has_xray and has_col and bool(self._summary_group_by))

    def _refresh_assisted(self) -> None:
        xray = self._current_xray()
        self._assist_columns.clear()

        if xray is None:
            self._assist_meta.setText("X-Ray: no input file loaded")
            self._assist_selected.setText(self._format_assisted_selected())
            self._refresh_assisted_buttons()
            return

        sheet = (str(getattr(xray, "sheet_name", "")) or "").strip() or "—"
        cols = int(getattr(xray, "total_columns", 0) or 0)
        conf = (str(getattr(xray, "confidence_display", "")) or "").strip() or "—"
        self._assist_meta.setText(f"X-Ray: loaded | Sheet: {sheet} | Columns: {cols} | Confidence: {conf}")

        group = self._current_group()
        filter_text = (self._assist_filter.text() or "").strip()
        for c in columns_for_group(xray=xray, group=group, filter_text=filter_text):
            self._assist_columns.addItem(c)

        self._assist_selected.setText(self._format_assisted_selected())
        self._refresh_assisted_buttons()

    def _format_assisted_selected(self) -> str:
        parts: list[str] = []
        if self._split_col:
            parts.append(f"split_col={self._split_col}")
        if self._tokens:
            parts.append(f"tokens={len(self._tokens)}")
        if self._summary_group_by:
            m = len(self._summary_sum_cols)
            parts.append(f"summary_group_by={self._summary_group_by} (sum_metrics={m})")
        return "Selected: " + (" | ".join(parts) if parts else "—")

    def _on_use_xray_sheet(self) -> None:
        xray = self._current_xray()
        if xray is None:
            return
        sheet = (str(getattr(xray, "sheet_name", "")) or "").strip()
        if sheet:
            self._referenced_sheet.setText(sheet)

    def _on_set_split_col(self) -> None:
        col = self._selected_column_header()
        if not col:
            return
        self._split_col = col
        self._refresh_preview()
        self._refresh_assisted()

    def _on_add_token(self) -> None:
        col = self._selected_column_header()
        if not col:
            return
        self._tokens = upsert_presence_token(tokens=self._tokens, source_col=col)
        self._refresh_preview()
        self._refresh_assisted()

    def _on_set_summary_group_by(self) -> None:
        col = self._selected_column_header()
        if not col:
            return
        self._summary_group_by = col
        self._refresh_preview()
        self._refresh_assisted()

    def _on_add_summary_sum_metric(self) -> None:
        col = self._selected_column_header()
        if not col:
            return
        if col not in self._summary_sum_cols:
            self._summary_sum_cols.append(col)
            # Keep deterministic ordering independent of click order.
            self._summary_sum_cols.sort(key=lambda s: (str(s).casefold(), str(s)))
        self._refresh_preview()
        self._refresh_assisted()

    def _effective_profiles_dir(self) -> Path:
        configured = (self._settings.profiles_dir or "").strip()
        if configured:
            p = Path(configured)
            if p.exists() and p.is_dir():
                return p.resolve()
        # Fall back to state (if runner already set it) or default ./profiles.
        try:
            p2 = Path((self._state.profiles_dir or "").strip())
            if str(p2).strip() and p2.exists() and p2.is_dir():
                return p2.resolve()
        except Exception:
            pass
        return self._default_profiles_dir

    def _refresh_preview(self) -> None:
        job_id = (self._job_id.text() or "").strip()
        export_label = (self._export_label.text() or "").strip()
        referenced_sheet = (self._referenced_sheet.text() or "").strip()

        summaries: list[dict] = []
        if self._summary_group_by:
            summaries = [make_group_aggregate_summary(group_by=self._summary_group_by, sum_cols=self._summary_sum_cols)]

        # Build deterministic profile dict from current inputs.
        profile = build_profile_dict(
            job_id=job_id,
            export_label=export_label,
            referenced_sheet=referenced_sheet,
            split_col=self._split_col,
            tokens=self._tokens,
            summaries=summaries,
        )
        self._preview.setPlainText(format_profile_json(profile))

        profiles_dir = self._effective_profiles_dir()
        folder_basename = profiles_dir.name or "profiles"
        self._save_dir_label.setText(f"Save folder: {folder_basename}")
        self._save_dir_label.setToolTip(str(profiles_dir))

        can_save = bool(job_id and referenced_sheet)
        if can_save:
            filename = choose_profile_filename(profiles_dir=profiles_dir, job_id=job_id)
            self._filename_label.setText(f"Filename: {filename}")
        else:
            self._filename_label.setText("Filename: —")

        self._save_btn.setEnabled(can_save)

    def _on_save_clicked(self) -> None:
        # Action-only backend usage: schema-only validation.
        job_id = (self._job_id.text() or "").strip()
        export_label = (self._export_label.text() or "").strip()
        referenced_sheet = (self._referenced_sheet.text() or "").strip()

        if not job_id or not referenced_sheet:
            self._status.setText("Job ID and referenced sheet are required.")
            self._status.setStyleSheet("color: #a1262f;")
            return

        profile = build_profile_dict(job_id=job_id, export_label=export_label, referenced_sheet=referenced_sheet)
        report = backend_facade.validate_profile_schema(profile)
        if not getattr(report, "is_valid", False):
            # Sanitized errors only.
            msgs = []
            for it in list(getattr(report, "errors", []) or []):
                code = sanitize_text(getattr(it, "code", "ERR"))
                msg = sanitize_text(getattr(it, "message", "Invalid profile"))
                msgs.append(f"{code}: {msg}")
            text = "Cannot save. Schema errors:\n" + ("\n".join(msgs) if msgs else "Invalid profile")
            self._status.setText(text)
            self._status.setStyleSheet("color: #a1262f;")
            return

        profiles_dir = self._effective_profiles_dir()
        filename = choose_profile_filename(profiles_dir=profiles_dir, job_id=job_id)
        result = save_profile_json(profiles_dir=profiles_dir, filename=filename, profile_dict=profile)
        if not result.ok:
            self._status.setText(sanitize_text(result.message))
            self._status.setStyleSheet("color: #a1262f;")
            return

        # Success: show basename only + refresh runner list.
        self._status.setText(sanitize_text(result.message))
        self._status.setStyleSheet("color: #1e7a34;")
        self._state.add_notice(f"Profile saved: {filename}")
        self._state.request_profiles_refresh(select_basename=filename)

        # Update preview/filename (will suffix if saving again).
        self._refresh_preview()
