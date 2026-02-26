"""Phase 4 path utilities: deterministic output folders (no UI, no I/O beyond mkdir)."""

from __future__ import annotations

from pathlib import Path
import os
import sys
from typing import Optional


def _get_windows_downloads_dir() -> Optional[Path]:
    try:
        import ctypes
        from ctypes import wintypes
        import uuid
    except Exception:
        return None

    class GUID(ctypes.Structure):
        _fields_ = [
            ("Data1", wintypes.DWORD),
            ("Data2", wintypes.WORD),
            ("Data3", wintypes.WORD),
            ("Data4", wintypes.BYTE * 8),
        ]

    folder_id = uuid.UUID("{374DE290-123F-4565-9164-39C4925E467B}")
    b = folder_id.bytes
    guid = GUID(
        int.from_bytes(b[0:4], "little"),
        int.from_bytes(b[4:6], "little"),
        int.from_bytes(b[6:8], "little"),
        (wintypes.BYTE * 8)(*b[8:16]),
    )
    path_ptr = ctypes.c_wchar_p()
    shell32 = ctypes.windll.shell32
    shell32.SHGetKnownFolderPath.argtypes = [
        ctypes.POINTER(GUID),
        wintypes.DWORD,
        wintypes.HANDLE,
        ctypes.POINTER(ctypes.c_wchar_p),
    ]
    res = shell32.SHGetKnownFolderPath(ctypes.byref(guid), 0, 0, ctypes.byref(path_ptr))
    if res != 0 or not path_ptr.value:
        return None
    path = Path(path_ptr.value)
    try:
        ctypes.windll.ole32.CoTaskMemFree(path_ptr)
    except Exception:
        pass
    return path


def get_default_downloads_dir() -> Path:
    """Return the default Downloads directory with safe fallbacks."""
    if sys.platform.startswith("win"):
        downloads = _get_windows_downloads_dir()
        if downloads and downloads.exists():
            return downloads
        user_profile = os.environ.get("USERPROFILE")
        if user_profile:
            fallback = Path(user_profile) / "Downloads"
            if fallback.exists():
                return fallback
    else:
        fallback = Path.home() / "Downloads"
        if fallback.exists():
            return fallback
    return Path.home()


def sanitize_component(text: str) -> str:
    """Sanitize a path component deterministically."""
    cleaned = text.replace("/", "_").replace("\\", "_").strip()
    return cleaned


def build_output_dir(base_dir: Path, user_date: str, label: str) -> Path:
    """Build and create the output directory `{YYYY_MM_DD}-{label}`."""
    safe_date = sanitize_component(user_date)
    safe_label = sanitize_component(label)
    if not safe_label:
        safe_label = "export"
    if not safe_date:
        safe_date = "unknown_date"
    folder_name = f"{safe_date}-{safe_label}"
    output_path = Path(base_dir) / folder_name
    output_path.mkdir(parents=True, exist_ok=True)
    return output_path


def resolve_profile_path(profile_arg: str) -> Path:
    """Resolve a profile path literally from the CLI argument."""
    p = Path(profile_arg).expanduser()
    if p.is_absolute():
        return p.resolve()
    return (Path.cwd() / p).resolve()
