from __future__ import annotations

import re
from typing import Iterable


_SEGMENT_NON_ALNUM_RE = re.compile(r"[^a-z0-9åäö]+")
_SEGMENT_UNDERSCORES_RE = re.compile(r"_+")


def normalize_col_segment(text: str) -> str:
    """Normalize a single name segment for Style C column headers.

    Rules (LOCK):
    - lowercase
    - preserve Swedish letters: åäö
    - all other chars (including whitespace/control) -> '_'
    - collapse '_' and trim
    - forbid whitespace/tabs/newlines (achieved by the replacement above)
    - empty -> 'x'
    """

    if text is None:
        text = ""

    # Short UTF-8 fork: preserve Swedish letters åäö (no ASCII folding).
    normalized = str(text).lower()

    normalized = _SEGMENT_NON_ALNUM_RE.sub("_", normalized)
    normalized = _SEGMENT_UNDERSCORES_RE.sub("_", normalized)
    normalized = normalized.strip("_")

    return normalized if normalized else "x"


def build_summary_metric_col(op: str, field: str | None) -> str:
    """Build a summary metric output header.

    LOCK:
    - if op == 'count_rows' => 'count_rows'
    - else '<op>_<field>' (both normalized segments)
    """

    if op == "count_rows":
        return "count_rows"

    op_part = normalize_col_segment(op)
    field_part = normalize_col_segment(field if field is not None else "")
    return f"{op_part}_{field_part}"


def make_unique_against(existing: set[str], proposed: list[str], max_len: int = 64) -> list[str]:
    """Return proposed names made unique against an existing set.

    Requirements (LOCK):
    - Deterministic
    - Never overwrites; on collision appends '__2', '__3', ...
    - Suffix must *survive* max_len: reserve suffix length before trimming base.

    Purity note:
    - Does not mutate the input 'existing' set; uniqueness is computed against a copy.
    """

    if max_len <= 0:
        raise ValueError("max_len must be > 0")

    used = set(existing)
    out: list[str] = []

    for raw_name in proposed:
        base0 = str(raw_name)
        if len(base0) > max_len:
            base0 = base0[:max_len]

        if base0 not in used:
            used.add(base0)
            out.append(base0)
            continue

        suffix_n = 2
        while True:
            suffix = f"__{suffix_n}"
            base_max = max_len - len(suffix)
            if base_max <= 0:
                raise ValueError(
                    f"max_len={max_len} is too small to fit suffix '{suffix}'"
                )
            base_part = base0
            if len(base_part) > base_max:
                base_part = base_part[:base_max]
            candidate = f"{base_part}{suffix}"
            if candidate not in used:
                used.add(candidate)
                out.append(candidate)
                break
            suffix_n += 1

    return out


def _normalize_many(segments: Iterable[str]) -> list[str]:
    """Internal helper for tests/debugging; not part of public API."""

    return [normalize_col_segment(s) for s in segments]
