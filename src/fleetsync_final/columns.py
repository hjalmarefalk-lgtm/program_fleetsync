"""Phase 2 column policies: pure deterministic transforms only (no I/O, no UI)."""

from __future__ import annotations

import pandas as pd

from .models import ColumnPolicy


def apply_column_policy(
    df: pd.DataFrame, policy: ColumnPolicy, token_cols: list[str] | None = None
) -> pd.DataFrame:
    """Apply keep/drop/order policy; append token columns deterministically."""
    existing_cols = list(df.columns)
    drop_set = set(policy.drop_cols)
    token_cols = token_cols or []
    token_set = set(token_cols)
    normal_cols = [col for col in existing_cols if col not in token_set]

    if policy.keep_cols:
        keep_list = [col for col in policy.keep_cols if col in normal_cols and col not in drop_set]
    else:
        keep_list = [col for col in normal_cols if col not in drop_set]

    ordered: list[str] = []
    seen: set[str] = set()

    for col in policy.order_cols:
        if col in keep_list and col not in seen:
            ordered.append(col)
            seen.add(col)

    for col in normal_cols:
        if col in keep_list and col not in seen:
            ordered.append(col)
            seen.add(col)

    for col in token_cols:
        if col in existing_cols and col not in drop_set and col not in seen:
            ordered.append(col)
            seen.add(col)

    return df.loc[:, ordered]
