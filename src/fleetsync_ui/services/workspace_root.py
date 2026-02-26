"""Workspace root resolution.

Goal (LOCK): determine the workspace/repo root without depending on the current
working directory.

Strategy:
- Walk upward from the fleetsync_ui package location until a marker is found.
- Marker: RUNBOOK.md (preferred) or .git directory.
- If not found, fall back to Path.cwd().

This module is pure Python (no Qt imports).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sys
from typing import Iterable, Optional


@dataclass(frozen=True)
class WorkspaceRootResult:
    root: Path
    used_fallback: bool
    marker: str


def _iter_parents(start_dir: Path) -> Iterable[Path]:
    p = start_dir
    while True:
        yield p
        if p.parent == p:
            break
        p = p.parent


def find_workspace_root(*, start_dir: Optional[Path] = None) -> WorkspaceRootResult:
    """Return workspace root and how it was determined."""

    if start_dir is None:
        # services/ -> fleetsync_ui/ -> src/ -> fleetsync_ui/ -> ...
        start_dir = Path(__file__).resolve().parent

    for candidate in _iter_parents(start_dir):
        if (candidate / "RUNBOOK.md").is_file():
            return WorkspaceRootResult(root=candidate, used_fallback=False, marker="RUNBOOK.md")
        if (candidate / ".git").exists():
            return WorkspaceRootResult(root=candidate, used_fallback=False, marker=".git")

    # Fallback policy:
    # - Dev / python -m: keep cwd fallback.
    # - Packaged exe: prefer the executable directory so a sibling ./profiles folder works.
    if bool(getattr(sys, "frozen", False)):
        try:
            exe_dir = Path(sys.executable).resolve().parent
            return WorkspaceRootResult(root=exe_dir, used_fallback=True, marker="sys.executable")
        except Exception:
            pass

    return WorkspaceRootResult(root=Path.cwd().resolve(), used_fallback=True, marker="cwd")
