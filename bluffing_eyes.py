"""Minimal starter to launch the tabletop Kivy application."""

from __future__ import annotations

import argparse
import os
import logging
from time import monotonic, sleep
from typing import Optional, Sequence

from tabletop.app import main as app_main
from tabletop.pupil_bridge import NeonDeviceConfig, PupilBridge

log = logging.getLogger(__name__)


def _wait_ok(check_fn, timeout: float, interval: float = 0.2, what: str = "status") -> None:
    """Poll ``check_fn`` until it returns truthy or timeout expires."""

    deadline = monotonic() + max(0.0, timeout)
    last_error: Optional[BaseException] = None
    while monotonic() < deadline:
        try:
            if check_fn():
                return
        except BaseException as exc:  # pragma: no cover - defensive
            last_error = exc
            log.debug("Warte auf %s: %s", what, exc)
        sleep(max(0.05, interval))
    if last_error is not None:
        raise TimeoutError(f"{what} did not become OK in {timeout:.1f}s: {last_error}")
    raise TimeoutError(f"{what} did not become OK in {timeout:.1f}s")


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

    configs = {
        role: NeonDeviceConfig.load(role)
        for role in ("VP1", "VP2")
    }
    for role in ("VP1", "VP2"):
        summary = configs[role].summary()
        log.info("Konfiguration %s: %s", role, summary)

    target_players = _resolve_players(player, configs)
    if not target_players:
        raise RuntimeError("Keine aktiven Eye-Tracker in der Konfiguration gefunden")

    bridge: Optional[PupilBridge] = None
    try:
        bridge = PupilBridge()
        log.info("Verbinde Eye-Tracker…")
        bridge.connect()
        for current in target_players:
            if not bridge.is_connected(current):
                raise RuntimeError(f"{current} ist nicht verbunden")

        def _start_stream(player_id: str, *, timeout: float = 8.0) -> None:
            cfg = configs[player_id]
            endpoint = cfg.address or cfg.ip or "-"
            log.info("Starte Video/Stream auf %s (%s)", player_id, endpoint)
            try:
                bridge.start_streaming(player_id, timeout=timeout)
            except TimeoutError as exc:
                try:
                    if bridge.is_streaming(player_id):
                        log.warning(
                            "Streaming-Timeout, aber Status OK – fahre fort (%s).",
                            player_id,
                        )
                        return
                except Exception:
                    pass
                raise RuntimeError(
                    f"Streamstart für {player_id} fehlgeschlagen: {exc}"
                ) from exc
            except Exception as exc:
                if bridge.is_streaming(player_id):
                    log.warning(
                        "Streamstart meldete Fehler, Status ist aber OK – fahre fort: %s",
                        player_id,
                    )
                    return
                raise RuntimeError(
                    f"Streamstart für {player_id} fehlgeschlagen: {exc}"
                ) from exc

        # Video/Stream sequenziell starten
        first, *rest = target_players
        _start_stream(first)
        for player_id in rest:
            sleep(1.0)
            _start_stream(player_id)

        sleep(5.0)

        for player_id in target_players:
            if not bridge.is_streaming(player_id):
                raise RuntimeError(f"Stream für {player_id} läuft nicht")

        log.info("Starte Aufnahme für %s", ", ".join(target_players))
        for player_id in target_players:
            if session is not None and block is not None:
                bridge.start_recording(session, block, player_id)
            else:
                ok = bridge.start_external_recording(player_id)
                if not ok:
                    raise RuntimeError(f"Recording-Start für {player_id} fehlgeschlagen")
            _wait_ok(
                lambda: bridge.is_recording(player_id),
                timeout=5.0,
                what=f"{player_id} recording",
            )

        log.info("Eye-Tracker-Startsequenz abgeschlossen")
        return bridge
    except Exception:
        log.exception("Eye-Tracker-Startsequenz fehlgeschlagen")
        if bridge is not None:
            try:
                bridge.close()
            except Exception:
                log.debug("Fehler beim Aufräumen des Eye-Tracker-Bridges", exc_info=True)
        raise


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
