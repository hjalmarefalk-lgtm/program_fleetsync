"""Canonical JSON utilities for deterministic profile I/O (Phase 1)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict


def dumps_canonical(obj: Any) -> str:
    """Serialize to canonical JSON (deterministic formatting)."""
    text = json.dumps(
        obj,
        ensure_ascii=False,
        sort_keys=True,
        indent=2,
    )
    return text + "\n"


def dump_canonical_to_file(obj: Any, path: str | Path) -> None:
    """Write canonical JSON to a file (UTF-8)."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(dumps_canonical(obj), encoding="utf-8")


def load_json_file(path: str | Path) -> Dict[str, Any]:
    """Load JSON from a file (UTF-8)."""
    source = Path(path)
    return json.loads(source.read_text(encoding="utf-8"))
