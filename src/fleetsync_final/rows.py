"""Phase 2 row ordering/filtering: pure deterministic transforms only (no I/O, no UI)."""

from __future__ import annotations

import pandas as pd

from .models import ContractFilterSpec, RowDropRule, RowOrderSpec


def apply_row_order(df: pd.DataFrame, row_order: RowOrderSpec) -> pd.DataFrame:
    """Return a stably sorted DataFrame based on row_order."""
    if not row_order.sort_keys:
        return df

    if isinstance(row_order.ascending, list):
        if len(row_order.ascending) != len(row_order.sort_keys):
            raise ValueError("row_order.ascending length must match row_order.sort_keys length")
        ascending = row_order.ascending
    else:
        ascending = row_order.ascending

    return df.sort_values(
        by=row_order.sort_keys,
        ascending=ascending,
        kind="mergesort",
    )


def apply_drop_rows(df: pd.DataFrame, drop_rules: list[RowDropRule]) -> pd.DataFrame:
    """Drop rows matching drop_rules without reordering or adding columns."""
    if not drop_rules:
        return df

    filtered = df
    for rule in drop_rules:
        if rule.col not in filtered.columns:
            continue
        if not rule.drop_values:
            continue
        mask = ~filtered[rule.col].isin(rule.drop_values)
        filtered = filtered.loc[mask]

    return filtered


def apply_contract_filter(df: pd.DataFrame, contract_filter: ContractFilterSpec | None) -> pd.DataFrame:
    """Keep only rows matching contract_filter values for the configured column."""
    if contract_filter is None:
        return df
    if not contract_filter.col:
        return df.iloc[0:0].copy()
    if contract_filter.col not in df.columns:
        return df.iloc[0:0].copy()
    if not contract_filter.values:
        return df.iloc[0:0].copy()

    filtered = df.loc[df[contract_filter.col].isin(contract_filter.values)]
    return filtered.copy()
