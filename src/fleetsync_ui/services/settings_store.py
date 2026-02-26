"""Settings persistence (offline, local).

Decision (LOCK): per-user settings file.

Location:
- QStandardPaths.AppDataLocation / FleetSyncUI / config.json

Only non-sensitive values are stored (paths only).
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Callable
from typing import Any

from PySide6.QtCore import QStandardPaths


@dataclass(frozen=True)
class SettingsDTO:
    profiles_dir: str = ""
    output_base_dir: str = ""


NoticeFn = Callable[[str], None]


SETTINGS_RESET_NOTICE = "Settings file was invalid and was reset to defaults."
SETTINGS_SAVE_FAILED_NOTICE = "Could not save settings. Using current settings for this session."


def _settings_path() -> Path:
    base = QStandardPaths.writableLocation(QStandardPaths.AppDataLocation)
    if not base:
        # Per-user fallback that does not depend on workspace.
        return Path.home() / ".fleetsync_ui" / "config.json"

    return Path(base) / "FleetSyncUI" / "config.json"


def _load_settings_from_path(path: Path, on_notice: NoticeFn | None) -> SettingsDTO:
    try:
        if not path.exists():
            return SettingsDTO()
        if not path.is_file():
            if on_notice is not None:
                on_notice(SETTINGS_RESET_NOTICE)
            return SettingsDTO()
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            if on_notice is not None:
                on_notice(SETTINGS_RESET_NOTICE)
            return SettingsDTO()
        profiles_dir = data.get("profiles_dir")
        output_base_dir = data.get("output_base_dir")
        return SettingsDTO(
            profiles_dir=str(profiles_dir) if isinstance(profiles_dir, str) else "",
            output_base_dir=str(output_base_dir) if isinstance(output_base_dir, str) else "",
        )
    except Exception:
        if on_notice is not None:
            on_notice(SETTINGS_RESET_NOTICE)
        return SettingsDTO()


def load_settings(on_notice: NoticeFn | None = None) -> SettingsDTO:
    return _load_settings_from_path(_settings_path(), on_notice)


def _atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=str(path.parent),
            delete=False,
            newline="\n",
        ) as f:
            f.write(text)
            f.flush()
            try:
                os.fsync(f.fileno())
            except Exception:
                # Best-effort only; do not fail the write for fsync issues.
                pass
            tmp_path = Path(f.name)
        os.replace(str(tmp_path), str(path))
        tmp_path = None
    finally:
        if tmp_path is not None:
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass


def save_settings(settings: SettingsDTO, on_notice: NoticeFn | None = None) -> None:
    path = _settings_path()
    payload: dict[str, Any] = asdict(settings)
    text = json.dumps(payload, indent=2, sort_keys=True)
    try:
        _atomic_write_text(path, text)
    except Exception:
        if on_notice is not None:
            on_notice(SETTINGS_SAVE_FAILED_NOTICE)
        return
