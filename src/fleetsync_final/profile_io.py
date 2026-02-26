"""Profile load/save helpers (Phase 1, no Excel I/O)."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from .json_canonical import dump_canonical_to_file, load_json_file


def load_profile(path: str | Path) -> Dict[str, Any]:
    """Load a profile JSON file into a dict."""
    source = Path(path)
    if not source.exists():
        raise ValueError(f"Profile file not found: {source}")
    data = load_json_file(source)
    if not isinstance(data, dict):
        raise ValueError("Profile JSON must be an object")
    return data


def save_profile(profile_dict: Dict[str, Any], path: str | Path, overwrite: bool = False) -> None:
    """Save a profile dict to JSON with deterministic formatting."""
    target = Path(path)
    if target.exists() and not overwrite:
        raise ValueError(f"Profile file exists and overwrite=False: {target}")
    dump_canonical_to_file(profile_dict, target)
