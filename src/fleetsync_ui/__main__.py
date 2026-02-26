"""Package entrypoint.

Allows running the UI as:
- `python -m fleetsync_ui`

This is a thin wrapper over `fleetsync_ui.app`.
"""

from __future__ import annotations

from .app import main


if __name__ == "__main__":
    raise SystemExit(main())
