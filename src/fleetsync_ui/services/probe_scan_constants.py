"""Central scan window constants.

LOCKS:
- Header detection + effective-width inference must not scan beyond:
  N=50 rows, M=200 cols.
- Previewability checks use a smaller, fixed window (50x50).

These are structural caps (not performance timings).
"""

from __future__ import annotations

# Sheet-preview scan window (used only to decide if a sheet is previewable).
PREVIEW_SCAN_N = 50
PREVIEW_SCAN_M = 50

# Header detection scan window (densest-row + effective width inference).
HEADER_SCAN_N = 50
HEADER_SCAN_M = 200

# Derived caps.
HEADER_SCAN_CELLS_MAX = HEADER_SCAN_N * HEADER_SCAN_M
