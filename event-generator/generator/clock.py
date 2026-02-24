from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone


class SimulationClock:
    """Maps wall-clock elapsed time to simulation time with compression."""

    def __init__(self, sim_start: datetime, compression: float) -> None:
        if sim_start.tzinfo is None:
            sim_start = sim_start.replace(tzinfo=timezone.utc)
        self._sim_start = sim_start.astimezone(timezone.utc)
        self._compression = compression
        self._real_start_monotonic = time.monotonic()

    def now(self) -> datetime:
        elapsed_real = time.monotonic() - self._real_start_monotonic
        elapsed_sim = elapsed_real * self._compression
        return self._sim_start + timedelta(seconds=elapsed_sim)
