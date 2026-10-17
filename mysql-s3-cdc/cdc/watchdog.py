"""Restart failed managed Kafka Connect tasks without disturbing healthy ones."""

from __future__ import annotations

import logging
import signal
import time
from collections import defaultdict

from .connectors import MANAGED_PREFIX, fatal_failure, request, restart_failed_tasks, short_failure


LOG = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    stopping = False

    def stop(_number, _frame):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    LOG.info("Connector watchdog started")
    attempts: dict[str, int] = defaultdict(int)
    next_retry: dict[str, float] = defaultdict(float)
    stable_since: dict[str, float] = {}
    while not stopping:
        try:
            names = request("GET", "/connectors") or []
            for name in names:
                name = str(name)
                if not name.startswith(MANAGED_PREFIX):
                    continue
                status = request("GET", f"/connectors/{name}/status")
                states = [str(task.get("state", "UNKNOWN")) for task in status.get("tasks", [])]
                if states and all(state == "RUNNING" for state in states):
                    stable_since.setdefault(name, time.monotonic())
                    if time.monotonic() - stable_since[name] >= 300:
                        attempts.pop(name, None)
                        next_retry.pop(name, None)
                    continue
                stable_since.pop(name, None)
                if "FAILED" in states:
                    trace = " ".join(str(task.get("trace", "")) for task in status.get("tasks", []))
                    if fatal_failure(trace):
                        LOG.error(
                            "Not restarting non-retryable failure for %s: %s",
                            name, short_failure(trace),
                        )
                        continue
                    if attempts[name] >= 3:
                        LOG.error("Retry limit reached for %s; manual investigation required", name)
                        continue
                    now = time.monotonic()
                    if now < next_retry[name]:
                        continue
                    attempts[name] += 1
                    delay = (30, 120, 300)[attempts[name] - 1]
                    next_retry[name] = now + delay
                    LOG.warning(
                        "Restarting failed task(s) for %s (attempt %d/3; next retry in %ds)",
                        name, attempts[name], delay,
                    )
                    restart_failed_tasks(name)
        except (OSError, RuntimeError) as exc:
            LOG.warning("Connector health check failed: %s", exc)

        for _ in range(30):
            if stopping:
                break
            time.sleep(1)
    LOG.info("Connector watchdog stopped")


if __name__ == "__main__":
    main()
