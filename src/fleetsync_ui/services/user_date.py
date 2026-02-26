"""Deterministic user_date normalization and validation.

LOCK (P9.3):
- Accept YYYY-MM-DD and YYYY_MM_DD
- Normalize to canonical internal format: YYYY-MM-DD
- No locale/time defaults; no parsing beyond basic numeric range checks
"""

from __future__ import annotations

import re
from dataclasses import dataclass


_USER_DATE_RE = re.compile(r"^\d{4}[-_]\d{2}[-_]\d{2}$")


@dataclass(frozen=True)
class UserDateState:
    raw: str
    normalized: str | None
    valid: bool


def normalize_user_date(text: str) -> UserDateState:
    raw = (text or "").strip()
    if not raw:
        return UserDateState(raw="", normalized=None, valid=False)

    if not _USER_DATE_RE.fullmatch(raw):
        return UserDateState(raw=raw, normalized=None, valid=False)

    normalized = raw.replace("_", "-")

    # Basic range checks (month 01-12, day 01-31). No calendar semantics.
    try:
        month = int(normalized[5:7])
        day = int(normalized[8:10])
    except ValueError:
        return UserDateState(raw=raw, normalized=None, valid=False)

    if month < 1 or month > 12:
        return UserDateState(raw=raw, normalized=None, valid=False)
    if day < 1 or day > 31:
        return UserDateState(raw=raw, normalized=None, valid=False)

    return UserDateState(raw=raw, normalized=normalized, valid=True)
