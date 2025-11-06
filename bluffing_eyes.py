"""Minimal starter to launch the tabletop Kivy application."""

from __future__ import annotations

import argparse
import logging
import os
import threading
import time
from typing import Optional, Sequence

from kivy.clock import Clock

from tabletop.app import main as app_main
from tabletop.pupil_bridge import NeonDeviceConfig, PupilBridge

log = logging.getLogger(__name__)


def _resolve_players(
    mode: str, configs: dict[str, NeonDeviceConfig]
) -> list[str]:
    order = ["VP1", "VP2"]
    lowered = (mode or "").strip().lower()
    if lowered == "vp1":
        return ["VP1"]
    if lowered == "vp2":
        return ["VP2"]
    if lowered == "both":
        return [player for player in order if configs[player].is_configured]
    # auto -> all configured in canonical order
    return [player for player in order if configs[player].is_configured]


def start_eye_trackers_sequence(
    *, session: Optional[int] = None, block: Optional[int] = None, player: str = "auto"
) -> PupilBridge:
    """Run the deterministic start sequence for the configured eye trackers."""

    os.environ["EYE_START_EXTERNALLY"] = "1"

    configs = {role: NeonDeviceConfig.load(role) for role in ("VP1", "VP2")}
    for role in ("VP1", "VP2"):
        summary = configs[role].summary()
        log.info("Konfiguration %s: %s", role, summary)

    target_players = _resolve_players(player, configs)
    if not target_players:
        raise RuntimeError("Keine aktiven Eye-Tracker in der Konfiguration gefunden")

    bridge = PupilBridge()
    log.info("Verbinde Eye-Tracker…")
    bridge.connect()
    for current in target_players:
        if not bridge.is_connected(current):
            raise RuntimeError(f"{current} ist nicht verbunden")

    label_time = time.strftime("%Y%m%d-%H%M%S")

    def _compose_label(pid: str) -> str:
        base = f"session_{label_time}_{pid}"
        if session is not None:
            base += f"_s{session}"
        if block is not None:
            base += f"_b{block}"
        return base

    labels = {pid: _compose_label(pid) for pid in target_players}

    state: dict[str, object] = {}
    done = threading.Event()
    scheduled: list["ClockEvent"] = []

    def _fail(exc: BaseException) -> None:
        if not state.get("error"):
            state["error"] = exc
        done.set()

    def _schedule(callback, delay: float) -> None:
        event = Clock.schedule_once(callback, delay)
        scheduled.append(event)

    def _ensure_rec(bridge_obj: PupilBridge, player_id: str, label: str) -> None:
        try:
            try:
                if bridge_obj.is_recording_active(player_id) or bridge_obj.recording_was_confirmed(
                    player_id, within_s=5.0
                ):
                    log.info("Recording already active/confirmed on %s", player_id)
                    return
            except Exception:
                pass
            bridge_obj.ensure_recording(player_id, label=label, tries=3, wait_s=1.0)
        except Exception as exc:
            is_streaming = False
            try:
                is_streaming = bridge_obj.is_streaming(player_id)
            except Exception:
                pass
            if is_streaming:
                log.warning(
                    "Recording start raised on %s but stream OK – continue: %s",
                    player_id,
                    exc,
                )
                return
            raise

    def _schedule_recording(player_id: str, label: str, delay: float) -> None:
        def _runner(_dt: float) -> None:
            if done.is_set():
                return
            try:
                _ensure_rec(bridge, player_id, label)
            except Exception as exc:
                log.warning("Recording start failed for %s: %s", player_id, exc)
                _fail(exc)

        _schedule(_runner, delay)

    def _start_stream(player_id: str, delay: float) -> None:
        def _runner(_dt: float) -> None:
            if done.is_set():
                return
            cfg = configs[player_id]
            endpoint = cfg.address or cfg.ip or "-"
            log.info("Start stream %s (%s)", player_id, endpoint)
            try:
                bridge.start_streaming(player_id)
            except Exception as exc:
                streaming_ok = False
                try:
                    streaming_ok = bridge.is_streaming(player_id)
                except Exception:
                    streaming_ok = False
                if streaming_ok:
                    log.warning(
                        "Stream start raised on %s but status OK – continue: %s",
                        player_id,
                        exc,
                    )
                else:
                    log.error("Streamstart für %s fehlgeschlagen: %s", player_id, exc)
                    _fail(exc)
                    return
            _schedule_recording(player_id, labels[player_id], 0.3)

        _schedule(_runner, delay)

    vp1_delay = 0.0
    vp2_delay = 1.2 if "VP1" in target_players else 0.0
    if "VP1" in target_players:
        _start_stream("VP1", vp1_delay)
    if "VP2" in target_players:
        _start_stream("VP2", vp2_delay)

    def _retry_broadcast(prefix: str, _dt: float) -> None:
        if done.is_set():
            return
        for player_id in target_players:
            try:
                _ensure_rec(bridge, player_id, f"{prefix}_{labels[player_id]}")
            except Exception as exc:
                log.warning("retry broadcast failed for %s: %s", player_id, exc)

    _schedule(lambda dt: _retry_broadcast("game", dt), 1.2)
    _schedule(lambda dt: _retry_broadcast("game_retry", dt), 3.0)

    def _gate_and_launch(_dt: float) -> None:
        if done.is_set():
            return
        try:
            statuses = {pid: bridge.is_streaming(pid) for pid in target_players}
            missing = [pid for pid, ok in statuses.items() if not ok]
            if not missing:
                log.info("Streams OK für %s", ", ".join(target_players))
                state["ready"] = True
                done.set()
                return
            raise SystemExit(
                "Streams not OK after 5s; aborting to avoid half-start. Missing: "
                + ", ".join(missing)
            )
        except Exception as exc:
            _fail(exc)

    _schedule(_gate_and_launch, 5.0)

    try:
        start = time.monotonic()
        while True:
            Clock.tick()
            if done.wait(timeout=0.05):
                break
            if time.monotonic() - start > 15.0 and not state.get("ready"):
                _fail(TimeoutError("Eye-Tracker start sequence timeout"))
                break
    finally:
        for event in scheduled:
            try:
                event.cancel()
            except Exception:
                pass

    error = state.get("error")
    if error:
        try:
            bridge.close()
        except Exception:
            log.debug("Fehler beim Aufräumen des Eye-Tracker-Bridges", exc_info=True)
        if isinstance(error, SystemExit):
            raise error
        raise RuntimeError("Eye-Tracker-Startsequenz fehlgeschlagen") from error

    log.info("Eye-Tracker-Startsequenz abgeschlossen")
    return bridge


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse command line arguments for the experiment launcher."""

    parser = argparse.ArgumentParser(description="Start the Bluffing Eyes tabletop app")
    parser.add_argument(
        "--session",
        type=int,
        required=False,
        default=None,
        help="Optional: Session-ID. Wenn nicht gesetzt, fragt die UI.",
    )
    parser.add_argument(
        "--block",
        type=int,
        required=False,
        default=None,
        help="Optional: einzelner Block. Wenn nicht gesetzt, steuert der Code die Blöcke.",
    )
    parser.add_argument(
        "--player",
        type=str,
        default="auto",
        choices=("auto", "both", "VP1", "VP2"),
        required=False,
        help=(
            "Optional: Player selector. 'auto' (default) tracks all connected players, "
            "'both' forces VP1 and VP2, otherwise restrict to the chosen player."
        ),
    )
    parser.add_argument(
        "--perf",
        action="store_true",
        help="Aktiviere zusätzliche Performance-Logs (für Debugging).",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    """Entry point that wires CLI arguments into the Kivy application."""

    args = parse_args(argv)
    if args.perf:
        os.environ["TABLETOP_PERF"] = "1"
    try:
        bridge = start_eye_trackers_sequence(
            session=args.session, block=args.block, player=args.player
        )
    except Exception:
        log.exception("Eye-Tracker start sequence failed. Aborting.")
        raise SystemExit(1)
    app_main(session=args.session, block=args.block, player=args.player, bridge=bridge)


if __name__ == "__main__":  # pragma: no cover - convenience wrapper
    main()
