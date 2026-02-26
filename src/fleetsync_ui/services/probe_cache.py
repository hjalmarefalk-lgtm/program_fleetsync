"""In-memory cache for probe/X-Ray results.

LOCKS (P5.5 / sheet-picker V2):
- Never persisted to disk.
- Keyed strictly by (abs_path, mtime_ns, size, sheet_name).
- Deterministic eviction (LRU).
- Values are metadata-only DTO-ish objects (no workbook handles).
"""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path
from typing import Generic, Optional, TypeVar

from .xray_models import XRayResult


@dataclass(frozen=True, slots=True)
class ProbeKey:
    abs_path: str
    mtime_ns: int
    size: int
    sheet_name: str


T = TypeVar("T")


class ProbeCache(Generic[T]):
    def __init__(self, *, max_entries: int) -> None:
        self._max_entries = int(max_entries)
        self._data: "OrderedDict[ProbeKey, T]" = OrderedDict()

    @property
    def max_entries(self) -> int:
        return self._max_entries

    def clear(self) -> None:
        self._data.clear()

    def get(self, key: ProbeKey) -> Optional[T]:
        if key not in self._data:
            return None
        # LRU: move to end deterministically.
        self._data.move_to_end(key)
        return self._data[key]

    def set(self, key: ProbeKey, value: T) -> None:
        if key in self._data:
            self._data[key] = value
            self._data.move_to_end(key)
        else:
            self._data[key] = value
        while len(self._data) > self._max_entries:
            self._data.popitem(last=False)


FileKey = tuple[str, int, int]


def compute_file_key(input_path: str) -> FileKey | None:
    """Return a file identity key for the current on-disk file state.

    If the path is missing/unstatable, returns None.
    """

    if not input_path:
        return None
    try:
        p = Path(input_path)
        # resolve() may fail if the file doesn't exist; fall back to absolute.
        try:
            abs_path = str(p.resolve())
        except Exception:
            abs_path = str(p.absolute())
        st = p.stat()
        return (abs_path, int(st.st_mtime_ns), int(st.st_size))
    except Exception:
        return None


def compute_probe_key(input_path: str, *, sheet_name: str) -> ProbeKey | None:
    fk = compute_file_key(input_path)
    if fk is None:
        return None
    abs_path, mtime_ns, size = fk
    return ProbeKey(abs_path=abs_path, mtime_ns=mtime_ns, size=size, sheet_name=str(sheet_name))


# P5.5: metadata-only result cache (XRayResult contains headers/groups only).
MAX_CACHE_ENTRIES = 5
PROBE_CACHE: ProbeCache[XRayResult] = ProbeCache(max_entries=MAX_CACHE_ENTRIES)
