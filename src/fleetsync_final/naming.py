"""Phase 2 sheet naming: deterministic transforms only (no I/O, no UI)."""

from __future__ import annotations

import re

_INVALID_SHEET_CHARS = r"[:\\/\?\*\[\]]"
RESERVED_SHEET_NAMES = {"Total", "Sammanfattning"}


def _normalize_reserved(names: set[str]) -> set[str]:
    return {name.lower() for name in names}


def sanitize_sheet_name(raw: str) -> str:
    """Return a safe Excel sheet name from raw input."""
    name = re.sub(_INVALID_SHEET_CHARS, "", raw)
    name = name.strip()
    name = name.strip("'")
    name = re.sub(r"\s+", "_", name)
    if not name:
        return "Sheet"
    return name


def enforce_31_chars(name: str, max_len: int = 31) -> str:
    """Ensure the sheet name is at most max_len characters (tail-preserving)."""
    if len(name) <= max_len:
        return name
    return name[-max_len:]


def make_unique_sheet_names(
    raw_names: list[str], reserved: set[str] | None = None
) -> list[str]:
    """Return deterministic, unique sheet names (sanitized + tail-trim + suffix)."""
    used: set[str] = set()
    result: list[str] = []
    reserved = reserved or set()
    reserved_norm = _normalize_reserved(reserved | RESERVED_SHEET_NAMES)

    for raw in raw_names:
        sanitized = sanitize_sheet_name(raw)
        candidate = enforce_31_chars(sanitized)
        if candidate not in used and candidate.lower() not in reserved_norm:
            used.add(candidate)
            result.append(candidate)
            continue

        suffix_index = 1
        while True:
            suffix = f"~{suffix_index}"
            max_base_len = 31 - len(suffix)
            base = sanitized[-max_base_len:] if max_base_len > 0 else ""
            candidate = f"{base}{suffix}"
            if candidate not in used and candidate.lower() not in reserved_norm:
                used.add(candidate)
                result.append(candidate)
                break
            suffix_index += 1

    return result
