"""Profile Creator helpers (P9.5a).

LOCKS:
- UI-only feature; backend is untouched.
- Deterministic filename normalization + suffixing.
- Atomic-ish JSON saves (temp file then replace).
- No workbook I/O.
"""

from __future__ import annotations

import json
import os
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict


_ALLOWED = set("abcdefghijklmnopqrstuvwxyz0123456789åäö_")


def _norm_id_part(text: str) -> str:
    raw = (text or "").strip().lower()
    if not raw:
        return ""

    out: list[str] = []
    for ch in raw:
        if ch in _ALLOWED:
            out.append(ch)
        elif ch.isspace():
            out.append("_")
        else:
            out.append("_")
    s = "".join(out)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def normalize_job_id(job_id: str) -> str:
    """Normalize job_id for deterministic filename base.

    Rules (LOCK):
    - lowercase
    - allowed: [a-z0-9åäö_]
    - everything else becomes underscore
    - collapse underscores, trim
    """

    return _norm_id_part(job_id)


def make_presence_token(*, source_col: str, token_id_prefix: str = "tok") -> Dict[str, Any]:
    """Create a minimal schema-valid token entry.

    Notes:
    - This is schema-only and intentionally does not attempt to infer cell values.
    - `inputs` is an object in schema; no required keys, but we include helpful defaults.
    """

    source_col = (source_col or "").strip()
    safe = _norm_id_part(source_col) or "col"
    token_id = f"{(token_id_prefix or 'tok').strip() or 'tok'}_{safe}"
    return {
        "token_id": token_id,
        "token_type": "presence_columns",
        "inputs": {"source_col": source_col, "values": [], "separator": ","},
        "scope": "workbook",
    }


def upsert_presence_token(*, tokens: list[Dict[str, Any]], source_col: str) -> list[Dict[str, Any]]:
    """Insert (or replace) a presence token for source_col, deterministically."""

    tok = make_presence_token(source_col=source_col)
    token_id = str(tok.get("token_id", ""))
    out = [t for t in list(tokens or []) if str(t.get("token_id", "")) != token_id]
    out.append(tok)
    out.sort(key=lambda t: (str(t.get("token_id", "")).casefold(), str(t.get("token_id", ""))))
    return out


def make_group_aggregate_summary(*, group_by: str, sum_cols: list[str] | None = None) -> Dict[str, Any]:
    """Create a minimal schema-valid group_aggregate summary entry."""

    group_by = (group_by or "").strip()
    cols = [str(c).strip() for c in list(sum_cols or []) if str(c).strip()]

    metrics: list[dict[str, Any]] = [{"op": "count_rows"}]
    for c in cols:
        metrics.append({"op": "sum", "col": c})

    return {
        "summary_type": "group_aggregate",
        "scope": "workbook",
        "group_by": group_by,
        "metrics": metrics,
    }


def choose_profile_filename(*, profiles_dir: Path, job_id: str, max_len: int = 64) -> str:
    """Return a deterministic available filename under profiles_dir.

    File name: <normalized job_id>.json
    If exists: <normalized>__2.json, __3.json, ...

    max_len applies to the filename only.
    """

    base = normalize_job_id(job_id)
    if not base:
        base = "profile"

    ext = ".json"

    def _clip(name_base: str, suffix: str) -> str:
        # Ensure deterministic clipping to respect max_len.
        max_base_len = max_len - len(ext) - len(suffix)
        if max_base_len < 1:
            # Degenerate case: keep something.
            name_base = "p"
            max_base_len = max_len - len(ext) - len(suffix)
        clipped = name_base[:max_base_len]
        clipped = re.sub(r"_+", "_", clipped).strip("_") or "profile"
        return f"{clipped}{suffix}{ext}"

    directory = Path(profiles_dir)
    first = _clip(base, "")
    if not (directory / first).exists():
        return first

    for n in range(2, 1000):
        suffix = f"__{n}"
        candidate = _clip(base, suffix)
        if not (directory / candidate).exists():
            return candidate

    # Extremely unlikely; deterministic fallback.
    return _clip(base, "__999")


def build_profile_dict(
    *,
    job_id: str,
    export_label: str,
    referenced_sheet: str,
    split_col: str = "",
    tokens: list[Dict[str, Any]] | None = None,
    summaries: list[Dict[str, Any]] | None = None,
) -> Dict[str, Any]:
    """Build a schema-compatible profile dict (no workbook I/O)."""

    job_id = (job_id or "").strip()
    export_label = (export_label or "").strip() or job_id
    referenced_sheet = (referenced_sheet or "").strip()
    split_col = (split_col or "").strip()
    tokens = list(tokens or [])
    summaries = list(summaries or [])

    return {
        "job_id": job_id,
        "export_label": export_label,
        "workbooks": [
            {
                "workbook_id": "wb_1",
                "referenced_sheet": referenced_sheet,
                # Keep deterministic and schema-valid. No locale formatting.
                "workbook_name_template": "{YYYY_MM_DD}",
                "split": {"split_col": split_col, "selected_values": []},
                "column_policy": {"keep_cols": [], "drop_cols": [], "order_cols": []},
                "drop_rows": [],
                "row_order": {"sort_keys": [], "ascending": True},
                "tokens": tokens,
                "summaries": summaries,
            }
        ],
    }


def format_profile_json(profile_dict: Dict[str, Any]) -> str:
    """Deterministic JSON for preview + save."""

    return json.dumps(profile_dict, ensure_ascii=False, sort_keys=True, indent=2) + "\n"


@dataclass(frozen=True)
class SaveResult:
    ok: bool
    filename: str
    message: str


def save_profile_json(*, profiles_dir: Path, filename: str, profile_dict: Dict[str, Any]) -> SaveResult:
    """Atomic-ish save of JSON to profiles_dir/filename.

    Writes a temp file in the same directory and then os.replace() to final.
    """

    directory = Path(profiles_dir)
    directory.mkdir(parents=True, exist_ok=True)

    safe_name = (filename or "").strip()
    if not safe_name.lower().endswith(".json"):
        return SaveResult(ok=False, filename="", message="Invalid filename")

    target = directory / safe_name
    if target.exists():
        return SaveResult(ok=False, filename=safe_name, message="File already exists")

    payload = format_profile_json(profile_dict)

    tmp_path: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="\n",
            delete=False,
            dir=str(directory),
            prefix=f".{safe_name}.tmp.",
        ) as tmp:
            tmp.write(payload)
            tmp.flush()
            os.fsync(tmp.fileno())
            tmp_path = tmp.name

        os.replace(tmp_path, target)
        tmp_path = None
        return SaveResult(ok=True, filename=safe_name, message=f"Saved: {safe_name}")
    except Exception:
        return SaveResult(ok=False, filename=safe_name, message="Save failed")
    finally:
        if tmp_path:
            try:
                os.unlink(tmp_path)
            except Exception:
                pass
