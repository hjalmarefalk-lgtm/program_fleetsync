# program_fleetsync

Program-only FleetSync runtime repository (UI + backend code only).

## Run
1. Create venv
   - `py -3.13 -m venv .venv`
2. Install package
   - `./.venv/Scripts/python -m pip install -U pip`
   - `./.venv/Scripts/python -m pip install -e .`
3. Launch UI
   - `./.venv/Scripts/python -m fleetsync_ui.app`
   - or `./.venv/Scripts/fleetsync-ui`

## Notes
- This repo intentionally excludes profiles and tests.
- Select a profiles folder in the UI when needed.
