import time

from core.event_router import EventRouter, UIEvent


def test_routes_only_to_active_device():
    deliveries: list[tuple[str, str]] = []

    def deliver(player: str, event: UIEvent) -> None:
        deliveries.append((player, event.name))

    router = EventRouter(deliver, batch_interval_s=0.01, max_batch=8)
    router.register_player("VP1")
    router.register_player("VP2")
    router.set_active_player("VP1")

    router.route(UIEvent(name="ping"))
    router.route(UIEvent(name="target", target="VP2"))
    router.flush_all()

    assert deliveries == [("VP1", "ping"), ("VP2", "target")]


def test_batching_flushes_periodically():
    deliveries: list[tuple[str, str]] = []

    def deliver(player: str, event: UIEvent) -> None:
        deliveries.append((player, event.name))

    router = EventRouter(deliver, batch_interval_s=0.01, max_batch=4)
    router.set_active_player("VP1")
    router.route(UIEvent(name="a"))
    router.route(UIEvent(name="b"))
    time.sleep(0.05)
    assert ("VP1", "a") in deliveries
    assert ("VP1", "b") in deliveries
    router.flush_all()
