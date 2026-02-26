"""Swedish UI strings (P9.4).

LOCK:
- UI-only (no backend strings)
- Flat + minimal (not a full i18n system)
- Deterministic: no locale-based formatting
"""

from __future__ import annotations

# Profile Runner (scoped)
PROFILE_RUNNER_TITLE = "Profilkörning"
PROFILE_RUNNER_SUBTITLE = "Kör markerade profiler från början till slut (backend körs i bakgrunden; resultat saneras)."

PROFILES_GROUP_TITLE = "Profiler"
PROFILES_DIR_UNSET = "Profilkatalog: (inte vald)"
CHOOSE_PROFILES_FOLDER = "Välj profilkatalog…"
RESET_PROFILES_DEFAULT = "Återställ till standard (./profiles)"

# Main action buttons (scoped)
RUN_SELECTED_PROFILES = "Kör markerade profiler"
RUN_ALL_PROFILES = "Kör alla profiler"

# Outcome-first status area (scoped)
STATUS_READY = "Redo"
STATUS_RUNNING = "Kör…"
STATUS_DONE = "Klar"
STATUS_FAILED = "Misslyckades"
STATUS_NOT_READY = "Inte redo"

# Problems box (scoped)
PROBLEMS_TITLE = "Problem"
PROBLEMS_EMPTY = "(inga problem)"
PROBLEMS_COUNTS_FMT = "Fel/Fatala ({fatals})   Varningar ({warnings})"
PROBLEMS_MORE_FMT = "+{count} till…"

# Selected profile details (scoped)
DETAILS_SELECTED_TITLE = "Valda profiler"
DETAILS_EMPTY = "(inga valda profiler)"
DETAILS_MORE_FMT = "+{count} profiler till…"
DETAILS_SPLIT_FULL_LABEL = "full"
DETAILS_MISSING_PREFIX = "saknar kolumner:"
DETAILS_RUNFOLLOW_TITLE = "Körning (live)"
DETAILS_RUNFOLLOW_OK = "OK"

# Left banner headings (scoped)
LEFT_INPUT_DATE_LABEL = "Inmatningsdatum:"
LEFT_XRAY_PREVIEW_SHEET_LABEL = "Röntgen: förhandsgranska blad (endast UI):"
