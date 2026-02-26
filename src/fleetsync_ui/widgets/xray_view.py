"""XRAY view widget.

Renders grouped column headers only (never cell values).
"""

from __future__ import annotations

from typing import Optional

from PySide6.QtWidgets import QLabel, QTreeWidget, QTreeWidgetItem, QVBoxLayout, QWidget

from ..services.xray_models import XRayGroup, XRayResult, XRAY_GROUP_ORDER


def format_xray_collapsed_header(result: Optional[XRayResult]) -> tuple[str, str]:
    """Build the always-visible collapsed header line.

    LOCKS:
    - Must be informative even when groups are collapsed.
    - Must not persist expand/collapse state.
    - Must not trigger any backend calls.
    """

    if result is None:
        title = "X-Ray"
        meta = "State: NO_FILE | Preview sheet: — | Columns: 0 | Confidence: —"
        return title, meta

    sheet = (str(getattr(result, "sheet_name", "")) or "").strip() or "—"
    conf = (str(getattr(result, "confidence_display", "")) or "").strip() or "—"
    cols = int(getattr(result, "total_columns", 0) or 0)
    sampled = int(getattr(result, "sampled_rows", 0) or 0)
    title = f"X-Ray ({cols} columns)"
    meta = f"State: LOADED | Preview sheet: {sheet} | Columns: {cols} | Confidence: {conf} | Sampled: {sampled} rows"
    return title, meta


class XRayView(QWidget):
    def __init__(self) -> None:
        super().__init__()

        self._title = QLabel("X-Ray")
        self._meta = QLabel("")
        self._meta.setWordWrap(True)
        self._diag = QLabel("")
        self._diag.setWordWrap(True)
        self._diag.hide()
        self._tree = QTreeWidget()
        self._tree.setHeaderHidden(True)
        self._tree.setUniformRowHeights(True)

        layout = QVBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(6)
        layout.addWidget(self._title)
        layout.addWidget(self._meta)
        layout.addWidget(self._diag)
        layout.addWidget(self._tree, 1)
        self.setLayout(layout)

        self._last_input_path: str | None = None
        self._expanded_by_group: dict[str, bool] = {}

        self.set_result(None)

    def set_result(self, result: Optional[XRayResult]) -> None:
        # Capture current expansion state before rebuilding the tree.
        current_expanded: dict[str, bool] = {}
        for i in range(self._tree.topLevelItemCount()):
            item = self._tree.topLevelItem(i)
            if item is None:
                continue
            text = item.text(0)
            # "GROUP (N)" -> group name
            group_name = text.split(" ", 1)[0]
            current_expanded[group_name] = item.isExpanded()

        self._tree.clear()
        if result is None:
            title, meta = format_xray_collapsed_header(None)
            self._title.setText(title)
            self._meta.setText(meta)
            self._diag.setText("")
            self._diag.hide()
            self._last_input_path = None
            self._expanded_by_group = {}
            return

        title, meta = format_xray_collapsed_header(result)
        self._title.setText(title)
        self._meta.setText(meta)

        same_file = self._last_input_path == result.input_path
        if same_file:
            # Preserve user choices from the previous render.
            self._expanded_by_group = current_expanded
        else:
            # New file: start collapsed by default.
            self._expanded_by_group = {}
            self._last_input_path = result.input_path

        for group in XRAY_GROUP_ORDER:
            cols = result.columns_by_group.get(group, ())
            group_item = QTreeWidgetItem([f"{group.value} ({len(cols)})"])
            if same_file and group.value in self._expanded_by_group:
                group_item.setExpanded(self._expanded_by_group[group.value])
            else:
                group_item.setExpanded(False)
            self._tree.addTopLevelItem(group_item)
            for col in cols:
                QTreeWidgetItem(group_item, [col])

        # Diagnostics: show only when user has expanded something.
        any_expanded = False
        for i in range(self._tree.topLevelItemCount()):
            item = self._tree.topLevelItem(i)
            if item is not None and item.isExpanded():
                any_expanded = True
                break

        if any_expanded:
            hdr = "Not found" if result.detected_header_row is None else str(result.detected_header_row)
            self._diag.setText(
                "Diagnostics\n"
                f"Detected header row: {hdr}\n"
                f"Confidence: {result.confidence_display}\n"
                f"Effective width: {result.effective_width} cols\n"
                f"Scan bounds: {result.sample_rows}×{result.sample_cols}"
            )
            self._diag.show()
        else:
            self._diag.setText("")
            self._diag.hide()

        if not same_file:
            self._tree.collapseAll()
            self._diag.setText("")
            self._diag.hide()
