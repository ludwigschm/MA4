"""Route UI events to the appropriate device."""

from __future__ import annotations

import threading
from collections import deque
from dataclasses import dataclass
from typing import Callable, Deque, Dict, Literal, Sequence

__all__ = ["UIEvent", "EventRouter"]


Priority = Literal["high", "normal"]


@dataclass(slots=True)
class UIEvent:
    """Event emitted from the UI that should be forwarded to a device."""

    name: str
    payload: dict[str, object] | None = None
    target: str | None = None
    broadcast: bool = False
    priority: Priority = "normal"


class EventRouter:
    """Route events to devices with optional batching to limit traffic."""

    def __init__(
        self,
        deliver: Callable[[str, UIEvent], None],
        *,
        batch_interval_s: float = 0.016,
        max_batch: int = 32,
        multi_route: bool = False,
    ) -> None:
        self._deliver = deliver
        self._batch_interval_s = batch_interval_s
        self._max_batch = max(1, int(max_batch))
        self._multi_route = multi_route
        self._active_player: str | None = None
        self._known_players: set[str] = set()
        self._queues: Dict[str, Deque[UIEvent]] = {}
        self._timers: Dict[str, threading.Timer] = {}
        self._lock = threading.Lock()

    def register_player(self, player: str) -> None:
        self._known_players.add(player)

    def unregister_player(self, player: str) -> None:
        self._known_players.discard(player)
        with self._lock:
            self._queues.pop(player, None)
            timer = self._timers.pop(player, None)
        if timer:
            timer.cancel()

    def set_active_player(self, player: str | None) -> None:
        if player is None:
            self._active_player = None
            return
        self.register_player(player)
        self._active_player = player

    def route(self, event: UIEvent) -> None:
        targets = self._select_targets(event)
        if not targets:
            return
        flush_jobs: list[tuple[str, Sequence[UIEvent]]] = []
        with self._lock:
            for target in targets:
                queue = self._queues.setdefault(target, deque())
                queue.append(event)
                if event.priority == "high":
                    batch = list(queue)
                    queue.clear()
                    timer = self._timers.pop(target, None)
                    if timer:
                        timer.cancel()
                    flush_jobs.append((target, batch))
                    continue
                if len(queue) >= self._max_batch:
                    batch = list(queue)
                    queue.clear()
                    timer = self._timers.pop(target, None)
                    if timer:
                        timer.cancel()
                    flush_jobs.append((target, batch))
                    continue
                timer = self._timers.get(target)
                if timer is None:
                    delay = max(0.0, self._batch_interval_s)
                    timer = threading.Timer(delay, self._flush_timer, args=(target,))
                    timer.daemon = True
                    self._timers[target] = timer
                    timer.start()
        for target, batch in flush_jobs:
            self._flush_batch(target, batch)

    def flush_all(self) -> None:
        with self._lock:
            items = list(self._queues.items())
            self._queues.clear()
            timers = list(self._timers.values())
            self._timers.clear()
        for timer in timers:
            timer.cancel()
        for target, queue in items:
            if queue:
                self._flush_batch(target, list(queue))

    # ------------------------------------------------------------------
    def _select_targets(self, event: UIEvent) -> Sequence[str]:
        if event.target:
            self.register_player(event.target)
            return (event.target,)
        if event.broadcast:
            if self._multi_route:
                return tuple(sorted(self._known_players))
            if self._active_player:
                return (self._active_player,)
            return ()
        if self._active_player:
            return (self._active_player,)
        return ()

    def _flush_timer(self, player: str) -> None:
        with self._lock:
            queue = self._queues.get(player)
            if not queue:
                self._timers.pop(player, None)
                return
            batch = list(queue)
            queue.clear()
            self._timers.pop(player, None)
        self._flush_batch(player, batch)

    def _flush_batch(self, player: str, batch: Sequence[UIEvent]) -> None:
        for event in batch:
            self._deliver(player, event)
