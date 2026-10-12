"""Restart failed managed Kafka Connect tasks without disturbing healthy ones."""

from __future__ import annotations

import logging
import signal
import time

from .connectors import MANAGED_PREFIX, request, restart_failed_tasks


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
    while not stopping:
        try:
            names = request("GET", "/connectors") or []
            for name in names:
                name = str(name)
                if not name.startswith(MANAGED_PREFIX):
                    continue
                status = request("GET", f"/connectors/{name}/status")
                states = [str(task.get("state", "UNKNOWN")) for task in status.get("tasks", [])]
                if "FAILED" in states:
                    LOG.warning("Restarting failed task(s) for %s", name)
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
