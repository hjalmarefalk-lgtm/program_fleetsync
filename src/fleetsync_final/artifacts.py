"""Phase 3 artifacts: deterministic outputs; do not mutate inputs in-place."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Literal, Union, TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd


@dataclass(frozen=True)
class TokenArtifact:
    """Token artifact output (may include output columns when configured)."""

    token_id: str
    scope: Literal["workbook", "sheet"]
    data: Union["pd.DataFrame", Dict[str, object]]
    meta: Dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class SummaryArtifact:
    """Summary artifact output (deterministic, workbook/sheet scoped)."""

    summary_id: str
    scope: Literal["workbook", "sheet"]
    tables: Dict[str, "pd.DataFrame"]


def assert_no_df_mutation(before_df: "pd.DataFrame", after_df: "pd.DataFrame", context: str) -> None:
    """Raise if a token/summary step mutates sheet columns or row counts."""
    if list(before_df.columns) != list(after_df.columns):
        raise AssertionError(f"{context}: sheet columns mutated")
    if len(before_df) != len(after_df):
        raise AssertionError(f"{context}: sheet row count mutated")
