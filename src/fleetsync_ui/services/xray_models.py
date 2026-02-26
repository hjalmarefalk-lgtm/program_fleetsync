"""XRAY data models.

X-Ray v1 is a read-only structural preview:
- It must list column headers only (no cell values displayed).
- Column groups are deterministic.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Mapping, Sequence


class XRayGroup(str, Enum):
    """Locked group set for X-Ray v1.5.

    Locked order:
        STRINGS → TIME → INTEGERS → FLOATS → TEXT → OTHER
    """

    INTEGERS = "INTEGERS"
    FLOATS = "FLOATS"
    STRINGS = "STRINGS"
    TIME = "TIME"
    TEXT = "TEXT"
    OTHER = "OTHER"


XRAY_GROUP_ORDER: tuple[XRayGroup, ...] = (
    XRayGroup.STRINGS,
    XRayGroup.TIME,
    XRayGroup.INTEGERS,
    XRayGroup.FLOATS,
    XRayGroup.TEXT,
    XRayGroup.OTHER,
)


@dataclass(frozen=True, slots=True)
class XRayResult:
    """Result of probing a single input file.

    Determinism contract:
    - Groups appear in `XRAY_GROUP_ORDER`.
    - Within each group, columns preserve original header order.
    """

    input_path: str

    # P6.1: UI-safe diagnostics (no values).
    sheet_name: str
    sheet_index: int
    ws_max_row: int
    ws_max_col: int
    sample_rows: int
    sample_cols: int
    detected_header_row: int | None
    best_row_nonempty_count: int
    header_nonempty_count: int
    effective_width: int
    # P6.7: structural cap counter (header scan only; UI-safe)
    header_scan_cells_scanned: int
    confidence_raw: float
    confidence_display: str

    headers: tuple[str, ...]
    columns_by_group: Mapping[XRayGroup, tuple[str, ...]]
    sampled_rows: int

    def group_count(self, group: XRayGroup) -> int:
        return len(self.columns_by_group.get(group, ()))

    @property
    def total_columns(self) -> int:
        ew = int(self.effective_width) if self.effective_width is not None else 0
        return max(len(self.headers), max(0, ew))

    def ordered_groups(self) -> Sequence[XRayGroup]:
        return XRAY_GROUP_ORDER
