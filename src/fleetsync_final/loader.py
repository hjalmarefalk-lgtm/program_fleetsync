"""Phase 4 loader: read Excel sheets into DataFrames and metadata (no UI)."""

from __future__ import annotations

from .metadata import InputMetadata
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd
else:
    import pandas as pd


_HEADER_ALIASES: dict[str, str] = {
    "egenavgift (ink moms)": "Egenavgift (inkl moms)",
}


def _normalize_header_name(name: object) -> object:
    if not isinstance(name, str):
        return name
    normalized = name.replace("\u00A0", " ").strip()
    alias = _HEADER_ALIASES.get(normalized.casefold())
    return alias if alias is not None else normalized


def _normalize_dataframe_columns(df: "pd.DataFrame") -> "pd.DataFrame":
    normalized_cols = [_normalize_header_name(col) for col in df.columns]
    if normalized_cols == list(df.columns):
        return df
    out_df = df.copy()
    out_df.columns = normalized_cols
    return out_df


def build_input_metadata(
    sheet_names: list[str],
    columns_by_sheet: dict[str, list[str]],
    row_counts_by_sheet: dict[str, int],
    source_path: str | None = None,
) -> InputMetadata:
    """Build InputMetadata from provided structures."""
    return InputMetadata(
        sheet_names=sheet_names,
        columns_by_sheet=columns_by_sheet,
        row_counts_by_sheet=row_counts_by_sheet,
        source_path=source_path,
    )


def load_input_xlsx(
    path: str, sheet_names: list[str] | None = None
) -> tuple[InputMetadata, dict[str, "pd.DataFrame"]]:
    """Load selected sheets into DataFrames and build InputMetadata.

    If sheet_names is provided, only those sheets are loaded. This is a
    performance optimization for large workbooks.
    """
    if sheet_names:
        loaded = pd.read_excel(path, sheet_name=sheet_names)
        if isinstance(loaded, dict):
            sheets = loaded
        else:
            sheets = {sheet_names[0]: loaded}
    else:
        sheets = pd.read_excel(path, sheet_name=None)
    sheets = {name: _normalize_dataframe_columns(df) for name, df in sheets.items()}
    sheet_names = list(sheets.keys())
    columns_by_sheet = {name: list(df.columns) for name, df in sheets.items()}
    row_counts_by_sheet = {name: int(len(df)) for name, df in sheets.items()}
    metadata = build_input_metadata(
        sheet_names=sheet_names,
        columns_by_sheet=columns_by_sheet,
        row_counts_by_sheet=row_counts_by_sheet,
        source_path=path,
    )
    return metadata, sheets
