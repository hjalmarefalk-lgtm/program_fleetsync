"""Assisted Profile Creator helpers powered by X-Ray (P9.5b).

LOCKS:
- UI-only consumption of existing XRayResult metadata.
- Never triggers probing or backend calls.
- Never returns cell values (headers only).
- Deterministic ordering: group order is fixed; columns sorted by normalized header.
"""

from __future__ import annotations

from dataclasses import dataclass

from .xray_models import XRayGroup, XRayResult, XRAY_GROUP_ORDER


ASSISTED_GROUP_ORDER: tuple[XRayGroup, ...] = XRAY_GROUP_ORDER


@dataclass(frozen=True)
class AssistedGroup:
    key: XRayGroup
    label: str


def assisted_groups() -> list[AssistedGroup]:
    return [AssistedGroup(key=g, label=g.value) for g in ASSISTED_GROUP_ORDER]


def _norm_header(h: str) -> str:
    return (h or "").strip().casefold()


def columns_for_group(*, xray: XRayResult, group: XRayGroup, filter_text: str = "") -> list[str]:
    """Return deterministic column header list for a group.

    - Uses XRayResult.columns_by_group (headers only).
    - Sorts by normalized header for quick scanning.
    - Filters within group by substring match on normalized header.
    """

    cols = list(xray.columns_by_group.get(group, ()) or ())
    cols = [str(c) for c in cols if str(c).strip()]

    ft = (filter_text or "").strip().casefold()
    if ft:
        cols = [c for c in cols if ft in _norm_header(c)]

    # Deterministic sort by normalized header, then by original as tiebreaker.
    cols.sort(key=lambda c: (_norm_header(c), c))
    return cols
