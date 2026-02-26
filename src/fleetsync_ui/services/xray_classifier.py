"""XRAY classifier service.

Rules:
- Uses up to K=50 non-null sample values per column.
- If < 3 non-null values => OTHER.
- Numeric precedence is locked: INTEGERS/FLOATS must not be stolen by TIME.
- TIME classification (v1.5):
    - Only considered if not numeric.
    - If ≥80% of samples match a supported time-like format => TIME.
    - If header contains time-hints, threshold is lowered to 60%.
- STRINGS vs TEXT:
    - TEXT if avg length >= 40 OR contains newline; else STRINGS.
"""

from __future__ import annotations

import re
from typing import Iterable, Mapping, Sequence

from .xray_models import XRayGroup, XRayResult, XRAY_GROUP_ORDER


_INT_RE = re.compile(r"^[+-]?\d+$")

# Supported time-like formats (minimum required):
# - ISO date: YYYY-MM-DD
# - ISO datetime: YYYY-MM-DD[ T]HH:MM(:SS)?(Z|±HH:MM optional)
# - Clock time: HH:MM(:SS)?
_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_ISO_DATETIME_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(:\d{2})?([Zz]|[+-]\d{2}:\d{2})?$"
)
_CLOCK_TIME_RE = re.compile(r"^\d{2}:\d{2}(:\d{2})?$")

_TIME_HINTS = (
    "date",
    "datum",
    "time",
    "tid",
    "timestamp",
    "datetime",
    "created",
    "updated",
)


def classify_columns(
    *,
    input_path: str,
    sheet_name: str = "",
    sheet_index: int = 0,
    ws_max_row: int = 0,
    ws_max_col: int = 0,
    sample_rows: int = 0,
    sample_cols: int = 0,
    detected_header_row: int | None = None,
    best_row_nonempty_count: int = 0,
    header_nonempty_count: int = 0,
    effective_width: int = 0,
    header_scan_cells_scanned: int = 0,
    confidence_raw: float = 0.0,
    confidence_display: str = "0.00",
    headers: Sequence[str],
    samples_by_header: Mapping[str, Sequence[object]],
    sampled_rows: int,
) -> XRayResult:
    """Classify columns into XRayGroup buckets.

    Deterministic ordering: preserves the original `headers` order within groups.
    """

    grouped: dict[XRayGroup, list[str]] = {g: [] for g in XRAY_GROUP_ORDER}

    for header in headers:
        raw_samples = samples_by_header.get(header, ())
        group = infer_group(header, raw_samples)
        grouped[group].append(header)

    columns_by_group = {g: tuple(cols) for g, cols in grouped.items()}
    return XRayResult(
        input_path=input_path,
        sheet_name=str(sheet_name),
        sheet_index=int(sheet_index),
        ws_max_row=int(ws_max_row),
        ws_max_col=int(ws_max_col),
        sample_rows=int(sample_rows),
        sample_cols=int(sample_cols),
        detected_header_row=detected_header_row,
        best_row_nonempty_count=int(best_row_nonempty_count),
        header_nonempty_count=int(header_nonempty_count),
        effective_width=int(effective_width),
        header_scan_cells_scanned=int(header_scan_cells_scanned),
        confidence_raw=float(confidence_raw),
        confidence_display=str(confidence_display),
        headers=tuple(headers),
        columns_by_group=columns_by_group,
        sampled_rows=int(sampled_rows),
    )


def infer_group(header: str, samples: Sequence[object]) -> XRayGroup:
    non_null = [v for v in samples if v is not None and str(v).strip() != ""]
    if len(non_null) < 3:
        return XRayGroup.OTHER

    if _all_int_like(non_null):
        return XRayGroup.INTEGERS

    if _all_float_like(non_null):
        return XRayGroup.FLOATS

    if _looks_time_like(header, non_null):
        return XRayGroup.TIME

    # Strings / text
    text_samples = [str(v) for v in non_null]
    if any("\n" in s or "\r" in s for s in text_samples):
        return XRayGroup.TEXT
    avg_len = sum(len(s) for s in text_samples) / max(1, len(text_samples))
    if avg_len >= 40:
        return XRayGroup.TEXT
    return XRayGroup.STRINGS


def _looks_time_like(header: str, values: Sequence[object]) -> bool:
    """Return True if the sample values are predominantly parseable as time/date.

    Threshold:
    - default 80%
    - 60% if header has time hints
    """

    header_hint = _has_time_hint(header)
    threshold = 0.60 if header_hint else 0.80

    svals = [_stringify(v) for v in values]
    svals = [s for s in svals if s]
    if len(svals) < 3:
        return False

    ok = 0
    for s in svals:
        if _ISO_DATE_RE.match(s) or _ISO_DATETIME_RE.match(s) or _CLOCK_TIME_RE.match(s):
            ok += 1
    ratio = ok / len(svals)
    return ratio >= threshold


def _has_time_hint(header: str) -> bool:
    h = header.casefold()
    return any(token in h for token in _TIME_HINTS)


def _all_int_like(values: Iterable[object]) -> bool:
    try:
        for v in values:
            s = _stringify(v)
            if not _INT_RE.match(s):
                return False
        return True
    except Exception:
        return False


def _all_float_like(values: Iterable[object]) -> bool:
    try:
        for v in values:
            s = _stringify(v)
            if _INT_RE.match(s):
                continue
            s2 = _normalize_decimal_string(s)
            float(s2)
        return True
    except Exception:
        return False


def _stringify(v: object) -> str:
    if isinstance(v, bool):
        return ""  # force OTHER via parsing failures
    if v is None:
        return ""
    return str(v).strip()


def _normalize_decimal_string(s: str) -> str:
    # Support common comma decimal, but avoid mangling thousands separators.
    # If it contains a comma and no dot, treat comma as decimal separator.
    if "," in s and "." not in s:
        return s.replace(",", ".")
    return s
