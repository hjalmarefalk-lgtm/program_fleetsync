from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Protocol


class _BatchItemLike(Protocol):
    profile_name: str
    status: str
    warnings_count: int
    fatals_count: int
    reason: str


@dataclass(frozen=True)
class BatchCounts:
    ok: int
    warn: int
    fail: int


def compute_batch_counts(items: Iterable[_BatchItemLike]) -> BatchCounts:
    ok = warn = fail = 0
    for it in items:
        status = str(getattr(it, "status", ""))
        warnings_count = int(getattr(it, "warnings_count", 0) or 0)
        fatals_count = int(getattr(it, "fatals_count", 0) or 0)
        if status == "failed" or fatals_count > 0:
            fail += 1
        elif warnings_count > 0:
            warn += 1
        else:
            ok += 1
    return BatchCounts(ok=ok, warn=warn, fail=fail)


def deterministic_reason(it: _BatchItemLike) -> str:
    reason = (getattr(it, "reason", "") or "").strip()
    if reason:
        return reason

    status = str(getattr(it, "status", ""))
    warnings_count = int(getattr(it, "warnings_count", 0) or 0)
    fatals_count = int(getattr(it, "fatals_count", 0) or 0)

    if status == "failed" or fatals_count > 0:
        return "run fatals"
    if warnings_count > 0:
        return "run warnings"
    return ""
