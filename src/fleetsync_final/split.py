"""Phase 2 split logic: pure deterministic transforms only (no I/O, no UI)."""

from __future__ import annotations

import pandas as pd

from .models import SplitSpec
from .naming import make_unique_sheet_names


def split_by_selected_values(df: pd.DataFrame, split: SplitSpec) -> dict[str, pd.DataFrame]:
    """Split rows by selected values using deterministic, safe sheet names."""
    if split.selected_values:
        selected: list[object] = sorted(set(split.selected_values))
    else:
        selected = sorted(df[split.split_col].dropna().unique().tolist(), key=lambda v: str(v))

    sheet_names = make_unique_sheet_names([str(v) for v in selected])

    result: dict[str, pd.DataFrame] = {}
    for value, sheet_name in zip(selected, sheet_names):
        subset = df.loc[df[split.split_col] == value].copy()
        result[sheet_name] = subset

    return result
