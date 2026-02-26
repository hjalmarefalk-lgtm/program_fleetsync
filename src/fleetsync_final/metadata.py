"""Phase 1 metadata contract (no Excel I/O)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass(frozen=True)
class InputMetadata:
    """Metadata about an input workbook (structure only)."""

    sheet_names: List[str]
    columns_by_sheet: Dict[str, List[str]]
    row_counts_by_sheet: Dict[str, int]
    source_path: Optional[str] = None

    def get_columns(self, sheet_name: str) -> List[str]:
        """Return columns for a sheet; empty list if sheet is missing."""
        return list(self.columns_by_sheet.get(sheet_name, []))
