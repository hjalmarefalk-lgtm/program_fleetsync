"""Result presentation helpers (UI-only).

LOCKS:
- Pure Python (no Qt imports).
- Deterministic behavior (fixed constants, no timestamps).
- Sanitizer applied to all output lines.
- No paths in copied text (strip path separators defensively).
- Preserve message ordering.
"""

from __future__ import annotations

from typing import Iterable, Literal, Sequence

from .dto import MessageItem
from .safe_errors import sanitize_message_item, sanitize_text


MAX_VISIBLE_MESSAGES = 20


Level = Literal["warning", "fatal"]


def truncate_messages(
    messages: Sequence[MessageItem] | None,
    max_visible: int = MAX_VISIBLE_MESSAGES,
) -> tuple[list[MessageItem], int]:
    """Return (visible_messages, hidden_count) with deterministic truncation."""

    max_visible = int(max_visible)
    if max_visible < 0:
        max_visible = 0

    items = list(messages or [])
    visible = items[:max_visible]
    hidden_count = max(0, len(items) - len(visible))
    return visible, hidden_count


def format_message_lines(messages: Sequence[MessageItem] | None, level: Level) -> list[str]:
    """Format messages into clipboard-safe, sanitized lines.

    Output rules:
    - One line per message.
    - Prefix is level-specific: "WARNING" or "FATAL".
    - Do not include file paths; strip path separators after sanitization.
    - Preserve ordering.
    """

    prefix = "WARNING" if level == "warning" else "FATAL"

    lines: list[str] = []
    for m in list(messages or []):
        sm = sanitize_message_item(m)
        code = sanitize_text(sm.code)
        msg = sanitize_text(sm.message)

        # Belt + suspenders: prevent path-ish fragments in clipboard.
        msg = _strip_path_separators(msg)
        code = _strip_path_separators(code)

        line = f"{prefix} {code}: {msg}".strip()
        lines.append(line)

    return lines


def format_output_basenames(outputs: Iterable[object] | None) -> list[str]:
    """Return output file basenames in deterministic order.

    Ordering rule (LOCK):
    - If inputs are in a sequence/list order, preserve that order.
    - If inputs are not sequence-like, caller should provide ordered inputs.
    """

    out: list[str] = []
    for p in list(outputs or []):
        if not isinstance(p, str) or not p:
            continue
        out.append(_basename(p))
    return out


def _basename(path: str) -> str:
    s = str(path)
    s = s.replace("\\", "/")
    if "/" in s:
        return s.rsplit("/", 1)[-1]
    return s


def _strip_path_separators(text: str) -> str:
    # Remove both separators; keep deterministic whitespace.
    s = str(text or "")
    s = s.replace("\\", " ")
    s = s.replace("/", " ")
    s = " ".join(s.split())
    return s
