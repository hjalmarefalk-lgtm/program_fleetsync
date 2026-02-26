"""Backend facade.

All backend imports MUST be confined to this module.

This module defines a minimal, pure-Python API surface for the UI to interact
with the headless backend package (`fleetsync_final`).

Hard rules:
- Do not add `sys.path` hacks.
- Do not expose pandas/openpyxl objects.
- Do not run jobs here (P4.1 scaffold only).

Deterministic ordering policy (LOCK):
- Preserve backend-reported message order as-is.
- Do not sort unless the source is inherently unordered.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass
from pathlib import Path
import re
from typing import Any, Dict, List, Optional

from .dto import CompileResult, MessageItem, ProfileRef, RunResult, ValidationReport
from .safe_errors import safe_user_error, sanitize_message_item, sanitize_text


@dataclass(frozen=True)
class ProfileGroupRef:
    """One-level profile group folder reference.

    DTO rule: pure-Python container (safe for UI use).
    """

    path: str
    name: str


class BackendFacadeError(RuntimeError):
    """Raised for backend-related failures with UI-safe messages."""


_HIDDEN_GROUP_NAMES = {
    "__pycache__",
    ".git",
    ".svn",
    "system volume information",
    "$recycle.bin",
}


def _is_visible_group_dir(p: Path) -> bool:
    """Return True if p should be shown as a profile group folder."""

    try:
        if not p.is_dir():
            return False

        name = (p.name or "").strip()
        if not name:
            return False

        lower = name.lower()
        if lower in _HIDDEN_GROUP_NAMES:
            return False

        # Cross-platform cheap filters for hidden/system-like folders.
        # (Windows hidden attribute is not reliably accessible via stdlib.)
        if name.startswith(".") or name.startswith("__") or name.startswith("$"):
            return False

        return True
    except Exception:
        return False


def list_profile_groups(profiles_root: Path) -> List[ProfileGroupRef]:
    """List immediate child directories under profiles_root as profile groups.

    LOCK (P10.1): one-level only, deterministic ordering, ignore hidden/system dirs.
    """

    root = Path(profiles_root)
    if not root.exists():
        return []
    if not root.is_dir():
        raise BackendFacadeError(sanitize_text(f"profiles_root is not a directory: {root.name}"))

    groups: list[ProfileGroupRef] = []
    try:
        for child in root.iterdir():
            if _is_visible_group_dir(child):
                groups.append(ProfileGroupRef(path=str(child.resolve()), name=child.name))
    except Exception:
        # Safe fallback: treat errors as empty listing.
        return []

    groups.sort(key=lambda g: (g.name or "").lower())
    return groups


def list_profiles_in_dir(group_dir: Path) -> List[ProfileRef]:
    """List profile JSON files in a single group directory (non-recursive).

    LOCK (P10.1): only *.json files, non-recursive, deterministic ordering.
    """

    directory = Path(group_dir)
    if not directory.exists():
        return []
    if not directory.is_dir():
        raise BackendFacadeError(sanitize_text(f"group_dir is not a directory: {directory.name}"))

    paths = sorted((p for p in directory.glob("*.json") if p.is_file()), key=lambda p: p.name.lower())
    return [ProfileRef(path=str(p.resolve()), name=p.name) for p in paths]


# Backend imports are intentionally confined to this module.
# We keep them optional-at-import-time so `import fleetsync_ui` remains possible,
# but calling facade functions will fail fast with a clean error if the backend
# is not installed.
_BACKEND_IMPORT_ERROR: Optional[BaseException]
try:
    import fleetsync_final as _fleetsync_final_pkg
    from fleetsync_final import profile_io as _profile_io
    from fleetsync_final import profile_compiler as _profile_compiler
    from fleetsync_final import profile_validate as _profile_validate
    from fleetsync_final import run_job as _run_job_mod
    from fleetsync_final import path_utils as _path_utils

    _BACKEND_IMPORT_ERROR = None
except BaseException as e:  # noqa: BLE001
    _fleetsync_final_pkg = None
    _profile_io = None
    _profile_compiler = None
    _profile_validate = None
    _run_job_mod = None
    _path_utils = None
    _BACKEND_IMPORT_ERROR = e


def _require_backend() -> None:
    if (
        _BACKEND_IMPORT_ERROR is not None
        or _profile_io is None
        or _profile_validate is None
        or _run_job_mod is None
        or _path_utils is None
    ):
        raise BackendFacadeError(
            "Backend package is not available. Install the backend in editable mode as described in RUNBOOK.md, "
            "then restart the UI."
        ) from None


def _require_compiler() -> None:
    _require_backend()
    if _profile_compiler is None:
        raise BackendFacadeError("Backend compiler is not available") from None


def get_backend_version() -> str:
    """Return the installed backend version string, or "unknown"."""

    # Prefer installed distribution metadata when available.
    try:
        from importlib.metadata import version

        return version("fleetsync-final")
    except Exception:  # noqa: BLE001
        pass

    # Fallback: check package attribute (if backend import succeeded).
    if _fleetsync_final_pkg is not None:
        v = getattr(_fleetsync_final_pkg, "__version__", None)
        if isinstance(v, str) and v:
            return v

    return "unknown"


def _level_from_backend_severity(severity: str) -> str:
    s = (severity or "").upper()
    if s == "WARNING":
        return "warning"
    if s == "FATAL":
        return "fatal"
    return "info"


def _message_from_backend_issue(issue_obj: object) -> MessageItem:
    # Backend issue is a dataclass with fields: code, severity, message, path(optional).
    code = str(getattr(issue_obj, "code", "UNKNOWN"))
    severity = str(getattr(issue_obj, "severity", "INFO"))
    message = str(getattr(issue_obj, "message", ""))
    path = getattr(issue_obj, "path", None)
    if path:
        # Preserve backend message order, but include path context deterministically.
        message = f"{message} (path={path})"
    return MessageItem(level=_level_from_backend_severity(severity), code=code, message=message)


def list_profiles(profiles_dir: Path) -> List[ProfileRef]:
    """List profile JSON files in a directory (deterministic order)."""

    directory = Path(profiles_dir)
    if not directory.exists():
        return []
    if not directory.is_dir():
        raise BackendFacadeError(f"profiles_dir is not a directory: {directory}")

    # Support nested groups under the profiles directory.
    # UI privacy policy: expose only a relative path (no absolute paths).
    def _rel_name(p: Path) -> str:
        try:
            return p.relative_to(directory).as_posix()
        except Exception:
            return p.name

    paths = [p for p in directory.rglob("*.json") if p.is_file()]
    paths.sort(key=lambda p: _rel_name(p).lower())
    return [ProfileRef(path=str(p.resolve()), name=_rel_name(p)) for p in paths]


def run_job(input_path: str, job_spec_handle: Any, output_base_dir: str | None = None) -> RunResult:
    """Run a compiled JobSpec against an input workbook and return a UI-safe RunResult.

    P4.9 rules (LOCK):
    - Must call backend only through this facade module.
    - Must execute the backend run in a worker thread (caller responsibility).
    - Treat backend warnings/fatals as untrusted: sanitize before returning.
    - Do not invent output folder conventions: output naming/structure is backend-owned.

    Notes:
    - `job_spec_handle` is expected to be an in-memory backend JobSpec dataclass.
    - `output_base_dir` is optional and, when provided, is passed to backend as its base dir.
    """

    _require_backend()

    input_path = (input_path or "").strip()
    if not input_path:
        msg = sanitize_message_item(safe_user_error("RUN_INPUT", "Input path is required"))
        return RunResult(status="failed", output_dir="", outputs=[], warnings=[], fatals=[msg])

    if job_spec_handle is None:
        msg = sanitize_message_item(safe_user_error("RUN_JOBSPEC", "Compile must succeed before run"))
        return RunResult(status="failed", output_dir="", outputs=[], warnings=[], fatals=[msg])

    base_dir: Path | None
    if isinstance(output_base_dir, str) and output_base_dir.strip():
        base_dir = Path(output_base_dir.strip())
    else:
        base_dir = None

    try:
        report = _run_job_mod.run_job(input_path=input_path, job_spec=job_spec_handle, output_dir=base_dir)  # type: ignore[union-attr]

        warnings: list[MessageItem] = []
        for w in list(getattr(report, "warnings", []) or []):
            warnings.append(sanitize_message_item(_message_from_backend_issue(w)))

        fatals: list[MessageItem] = []
        for f in list(getattr(report, "fatals", []) or []):
            fatals.append(sanitize_message_item(_message_from_backend_issue(f)))

        outputs: list[str] = []
        for p in list(getattr(report, "outputs", []) or []):
            if isinstance(p, str) and p:
                outputs.append(p)

        # Determine output directory without inventing conventions:
        # - Prefer parent of the first output file when present.
        # - Otherwise, compute the backend-owned output folder name via path_utils.
        output_dir = ""
        if outputs:
            output_dir = str(Path(outputs[0]).parent)
        else:
            # Mirror backend base-dir selection.
            actual_base = base_dir if base_dir is not None else _path_utils.get_default_downloads_dir()  # type: ignore[union-attr]
            label = getattr(job_spec_handle, "export_label", "") or getattr(job_spec_handle, "job_id", "") or "FleetSync"
            user_date = getattr(job_spec_handle, "user_date", "")
            try:
                output_dir = str(_path_utils.build_output_dir(Path(actual_base), str(user_date), str(label)))  # type: ignore[union-attr]
            except Exception:
                output_dir = ""

        status = "failed" if fatals else "success"
        return RunResult(status=status, output_dir=output_dir, outputs=outputs, warnings=warnings, fatals=fatals)
    except Exception:
        msg = sanitize_message_item(safe_user_error("RUN_EXCEPTION", "Backend run failed"))
        return RunResult(status="failed", output_dir="", outputs=[], warnings=[], fatals=[msg])


def load_profile(profile_path: Path) -> Dict[str, Any]:
    """Load a profile JSON file into a dict.

    Raises BackendFacadeError with a UI-safe message on failure.
    """

    _require_backend()

    source = Path(profile_path)
    try:
        data = _profile_io.load_profile(source)
        if not isinstance(data, dict):
            raise BackendFacadeError("Profile JSON must be an object")
        return data
    except BackendFacadeError:
        raise
    except FileNotFoundError:
        # Do not expose full paths by default; UI can show full path later via tooltip/copy.
        raise BackendFacadeError(f"Profile file not found: {source.name}") from None
    except ValueError:
        # Do not surface raw exception text.
        raise BackendFacadeError("Profile file is invalid") from None
    except Exception:
        raise BackendFacadeError("Profile load failed") from None


def validate_profile_schema(profile_dict: Dict[str, Any]) -> ValidationReport:
    """Validate profile schema only.

    Notes:
    - This is schema-only validation.
    - Messages are sanitized here (P4.3 GDPR gate).
    - Ordering rule (LOCK): preserve backend issue order.
    """

    _require_backend()

    try:
        report = _profile_validate.validate_profile_schema(profile_dict)
        warnings = [sanitize_message_item(_message_from_backend_issue(i)) for i in getattr(report, "warnings", [])]
        errors = [sanitize_message_item(_message_from_backend_issue(i)) for i in getattr(report, "fatals", [])]
        is_valid = bool(getattr(report, "valid", False))
        return ValidationReport(is_valid=is_valid, warnings=warnings, errors=errors)
    except Exception:
        # Do not leak raw exceptions here; surface as a single sanitized fatal message item.
        return ValidationReport(
            is_valid=False,
            warnings=[],
            errors=[safe_user_error("EXCEPTION", "Profile schema validation failed")],
        )


def _validate_user_date_for_compile(user_date: str) -> bool:
    # LOCK: UI date format is YYYY-MM-DD, no system time semantics.
    if not user_date:
        return False
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", user_date):
        return False
    try:
        month = int(user_date[5:7])
        day = int(user_date[8:10])
    except ValueError:
        return False
    if month < 1 or month > 12:
        return False
    if day < 1 or day > 31:
        return False
    return True


def _job_spec_summary(job_spec: object) -> Dict[str, Any]:
    # Safe, DTO-friendly summary (no workbook I/O; no paths).
    job_id = getattr(job_spec, "job_id", "")
    user_date = getattr(job_spec, "user_date", "")
    export_label = getattr(job_spec, "export_label", "")
    workbooks = list(getattr(job_spec, "workbooks", []) or [])

    wb_summaries: list[dict[str, Any]] = []
    for wb in workbooks:
        wb_summaries.append(
            {
                "workbook_id": getattr(wb, "workbook_id", ""),
                "client": getattr(wb, "client", None),
                "referenced_sheet": getattr(wb, "referenced_sheet", ""),
                "workbook_name_template": getattr(wb, "workbook_name_template", ""),
                "tokens_count": len(list(getattr(wb, "tokens", []) or [])),
                "summaries_count": len(list(getattr(wb, "summaries", []) or [])),
            }
        )

    return {
        "job_id": str(job_id or ""),
        "user_date": str(user_date or ""),
        "export_label": str(export_label or ""),
        "workbooks_count": len(workbooks),
        "workbooks": wb_summaries,
    }


def compile_job_spec(profile_dict: Dict[str, Any], user_date: str) -> CompileResult:
    """Compile backend JobSpec from a profile dict.

    Rules (LOCK):
    - Must not rewrite the on-disk profile.
    - Inject `user_date` in-memory only.
    - No run_job execution.
    """

    result, _ = compile_job_spec_with_handle(profile_dict, user_date)
    return result


def compile_job_spec_with_handle(profile_dict: Dict[str, Any], user_date: str) -> tuple[CompileResult, object | None]:
    """Compile JobSpec and return an opaque handle for UI-local storage.

    The handle must not cross Qt signals; store it in AppState only.
    """

    _require_compiler()

    if not isinstance(profile_dict, dict):
        return (
            CompileResult(
                attempted=True,
                success=False,
                job_spec_summary={},
                messages=[safe_user_error("COMPILE_INPUT", "Profile must be a JSON object")],
            ),
            None,
        )

    user_date = (user_date or "").strip()
    if not _validate_user_date_for_compile(user_date):
        return (
            CompileResult(
                attempted=True,
                success=False,
                job_spec_summary={},
                messages=[safe_user_error("USER_DATE", "user_date must be YYYY-MM-DD")],
            ),
            None,
        )

    injected = copy.deepcopy(profile_dict)
    injected["user_date"] = user_date

    try:
        job_spec = _profile_compiler.job_spec_from_profile_dict(injected)  # type: ignore[union-attr]
        summary = _job_spec_summary(job_spec)

        # Compilation gate: fail on required fields even if schema passed.
        missing: list[str] = []
        if not summary.get("job_id"):
            missing.append("job_id")
        if not summary.get("export_label"):
            missing.append("export_label")
        if not summary.get("user_date"):
            missing.append("user_date")
        if int(summary.get("workbooks_count") or 0) <= 0:
            missing.append("workbooks")

        if missing:
            msg = safe_user_error("COMPILE_MISSING", f"Missing required: {', '.join(missing)}")
            return (
                CompileResult(
                    attempted=True,
                    success=False,
                    job_spec_summary=summary,
                    messages=[sanitize_message_item(msg)],
                ),
                None,
            )

        return (
            CompileResult(attempted=True, success=True, job_spec_summary=summary, messages=[]),
            job_spec,
        )
    except Exception:
        return (
            CompileResult(
                attempted=True,
                success=False,
                job_spec_summary={},
                messages=[safe_user_error("EXCEPTION", "JobSpec compilation failed")],
            ),
            None,
        )
