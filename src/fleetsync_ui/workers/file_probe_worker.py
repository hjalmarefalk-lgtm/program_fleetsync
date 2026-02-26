"""Worker for file probing.

Runs file probing + X-ray classification off the UI thread.
"""

from __future__ import annotations

from PySide6.QtCore import QObject, Signal, Slot

from ..services.dto import ProbeXRayPayload
from ..services.xray_service import probe_xray
from ..services.safe_errors import sanitize_text


class FileProbeWorker(QObject):
    result_ready = Signal(object)  # ProbeXRayPayload
    error = Signal(str)

    def __init__(
        self,
        *,
        input_path: str,
        sheet_name: str | None = None,
        nrows: int = 300,
        k_values: int = 50,
    ) -> None:
        super().__init__()
        self._input_path = input_path
        self._sheet_name = sheet_name
        self._nrows = nrows
        self._k_values = k_values

    @Slot()
    def run(self) -> None:
        try:
            payload = probe_xray(input_path=self._input_path, sheet_name=self._sheet_name)
            self.result_ready.emit(payload)
        except Exception as e:
            msg = sanitize_text(str(e)) or "Probe failed"
            self.error.emit(msg)
