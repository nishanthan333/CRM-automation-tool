"""The background sync timer used by `watchdog serve`."""
import logging
import threading
from typing import Callable

log = logging.getLogger("watchdog")


class SyncTimer:
    """Run `job` every `interval_s` seconds, for as long as the process lives.

    The timer re-arms after every run, success or failure: a sync that raises
    (HubSpot down, rate limit, locked database) is logged and the next one is
    still scheduled. Runs never overlap, since the next is armed only after the
    current one ends. Threads are daemons, so they never hold the process open.
    """

    def __init__(self, interval_s: float, job: Callable[[], None], *,
                 timer_factory: Callable[..., threading.Timer] = threading.Timer):
        self.interval_s = interval_s
        self._job = job
        self._factory = timer_factory
        self._lock = threading.Lock()
        self._timer = None
        self._stopped = False

    def start(self, immediate: bool = False) -> None:
        """Arm the first run; `immediate` makes it fire now rather than after
        a full interval, so a freshly started server is not stale."""
        self._arm(0 if immediate else self.interval_s)

    def stop(self) -> None:
        with self._lock:
            self._stopped = True
            if self._timer is not None:
                self._timer.cancel()

    def _arm(self, delay: float | None = None) -> None:
        with self._lock:
            if self._stopped:
                return
            self._timer = self._factory(self.interval_s if delay is None else delay, self._fire)
            self._timer.daemon = True
            self._timer.start()

    def _fire(self) -> None:
        if self._stopped:
            return
        try:
            self._job()
        except Exception:  # noqa: BLE001 - one bad sync must not end all syncs
            log.exception("scheduled sync failed; will retry in %ss", self.interval_s)
        finally:
            self._arm()
