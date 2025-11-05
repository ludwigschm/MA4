"""Minimal async dispatch queue for bridge calls.

MA2 Bridge-Calls asynchronisiert (MA3-Pattern), Payload vollständig erhalten.
"""

from __future__ import annotations

import logging
import queue
import threading
from typing import Callable

_log = logging.getLogger(__name__)

_q: "queue.Queue[Callable[[], None]]" = queue.Queue(maxsize=10000)


def _worker() -> None:
    while True:
        try:
            fn = _q.get()
            fn()
        except Exception:  # pragma: no cover - defensive fallback
            _log.exception("async task failed")
        finally:
            _q.task_done()


_thread = threading.Thread(target=_worker, name="AsyncBridge", daemon=True)
_thread.start()


def enqueue(fn: Callable[[], None]) -> None:
    """Schedule *fn* for background execution without blocking the UI."""

    if fn is None:
        return
    try:
        _q.put_nowait(fn)
    except queue.Full:
        _log.warning("async queue full; dropping event")
