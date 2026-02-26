"""Phase 4 export writer: write one output workbook (no UI, no validation)."""

from __future__ import annotations

from decimal import Decimal
import math
from pathlib import Path
from typing import Dict
import os
import tempfile

import pandas as pd
from pandas.api.types import is_float_dtype, is_integer_dtype

from .naming import RESERVED_SHEET_NAMES, make_unique_sheet_names


def _best_effort_fsync(path: Path) -> None:
    """Best-effort fsync; never raises (skip or ignore on Windows)."""
    try:
        if os.name == "nt":
            return
        fd = os.open(str(path), os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError:
        return


def _column_number_format(dtype: object) -> str | None:
    if is_float_dtype(dtype):
        return "#,##0.00"
    if is_integer_dtype(dtype):
        return "#,##0"
    return None


def _coerce_scalar(v: object) -> object:
    # pandas/openpyxl may produce numpy scalars; convert them to Python scalars.
    try:
        item = getattr(v, "item", None)
        if callable(item):
            return item()
    except Exception:
        return v
    return v


def _is_integer_like_number(v: object) -> bool:
    v = _coerce_scalar(v)
    if v is None or isinstance(v, bool):
        return False
    if isinstance(v, int):
        return True
    if isinstance(v, float):
        if not math.isfinite(v):
            return False
        nearest = round(v)
        eps = 1e-9 * max(1.0, abs(v))
        return abs(v - nearest) <= eps
    if isinstance(v, Decimal):
        try:
            return v == v.to_integral_value()
        except Exception:
            return False
    return False


def _apply_number_formats(ws, df: pd.DataFrame, start_row: int, start_col: int) -> None:
    if df.empty:
        return
    data_start_row = start_row + 2
    data_end_row = data_start_row + len(df) - 1
    col_start = start_col + 1

    for idx, column in enumerate(df.columns):
        fmt = _column_number_format(df[column].dtype)
        if not fmt:
            continue
        col_idx = col_start + idx
        if fmt == "#,##0.00":
            # Apply 2-decimal display only for values with a true fractional part.
            # Integer-like values (including 123.0) should not display trailing .00.
            for row in range(data_start_row, data_end_row + 1):
                cell = ws.cell(row=row, column=col_idx)
                v = cell.value
                if v is None or isinstance(v, bool):
                    continue
                if _is_integer_like_number(v):
                    cell.number_format = "#,##0"
                else:
                    cell.number_format = "#,##0.00"
        else:
            for row in range(data_start_row, data_end_row + 1):
                ws.cell(row=row, column=col_idx).number_format = fmt


def write_workbook_xlsx(
    path: str | Path,
    workbook_name: str,
    total_df: pd.DataFrame,
    split_dfs: Dict[str, pd.DataFrame],
    summary_tables: Dict[str, pd.DataFrame],
    main_sheet_name: str | None = None,
) -> Path:
    """Write one workbook with data sheets and summary sheet.

    Export contract (P9.9):
    - If more than one split sheet is produced (N>1): write "Total" + split sheets.
    - If one split sheet is produced (N=1): write exactly one data sheet (no "Total" sheet).
    - If summary tables are produced: write "Sammanfattning" as the first sheet.

    The single data sheet name is derived from `main_sheet_name` (typically the profile's
    referenced_sheet) using the deterministic sheet-name sanitization rules.
    """
    output_path = Path(path)
    if output_path.suffix.lower() != ".xlsx":
        output_path = output_path / f"{workbook_name}.xlsx"

    split_names_sorted = sorted(split_dfs.keys())
    safe_split_names = make_unique_sheet_names(split_names_sorted, reserved=RESERVED_SHEET_NAMES)
    sheet_name_map = dict(zip(split_names_sorted, safe_split_names))

    split_count = len(split_names_sorted)
    has_splits = split_count > 0
    has_total = split_count > 1
    has_summary = bool(summary_tables)

    # Data sheet name for no-split outputs. Keep deterministic and avoid reserved collisions.
    raw_main = str(main_sheet_name or "Total")
    safe_main = make_unique_sheet_names([raw_main], reserved=RESERVED_SHEET_NAMES)[0]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        suffix=".tmp.xlsx",
        dir=output_path.parent,
        delete=False,
    ) as tmp_file:
        tmp_path = Path(tmp_file.name)

    try:
        with pd.ExcelWriter(tmp_path, engine="openpyxl") as writer:
            if has_summary:
                summary_sheet = "Sammanfattning"
                start_col = 0
                for table_name in sorted(summary_tables.keys()):
                    table = summary_tables[table_name]
                    table.to_excel(
                        writer,
                        sheet_name=summary_sheet,
                        index=False,
                        startrow=1,
                        startcol=start_col,
                    )
                    ws = writer.book[summary_sheet]
                    ws.cell(row=1, column=start_col + 1, value=table_name)
                    _apply_number_formats(ws, table, start_row=1, start_col=start_col)

                    table_width = max(len(table.columns), 1)
                    start_col += table_width + 1

            if has_total:
                total_df.to_excel(writer, sheet_name="Total", index=False)
                total_ws = writer.book["Total"]
                _apply_number_formats(total_ws, total_df, start_row=0, start_col=0)
            elif not has_splits:
                # P9.9: single-sheet export when no split sheets are produced.
                total_df.to_excel(writer, sheet_name=safe_main, index=False)
                main_ws = writer.book[safe_main]
                _apply_number_formats(main_ws, total_df, start_row=0, start_col=0)

            for raw_name in split_names_sorted:
                split_dfs[raw_name].to_excel(writer, sheet_name=sheet_name_map[raw_name], index=False)
                split_ws = writer.book[sheet_name_map[raw_name]]
                _apply_number_formats(split_ws, split_dfs[raw_name], start_row=0, start_col=0)

        _best_effort_fsync(tmp_path)
    except Exception:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        raise

    os.replace(tmp_path, output_path)
    return output_path
