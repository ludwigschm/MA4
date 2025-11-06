"""Minimal starter to launch the tabletop Kivy application."""

from __future__ import annotations

import argparse
import logging
import os
import time
from typing import Optional, Sequence

from tabletop.app import main as app_main
from tabletop.pupil_bridge import (
    NeonDeviceConfig,
    PupilBridge,
    RecordingStartResult,
)

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

def log_recording_result(player_id: str, result: RecordingStartResult) -> None:
    prefix = f"[{player_id.lower()}]"
    log.info(
        "%s start_recording ok=%s | active=%s | recording_id=%s | video_guid=%s | msg=%s",
        prefix,
        result.ok,
        result.is_recording_active,
        result.recording_id or "-",
        result.video_guid or "-",
        result.message or "",
    )


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

    timestamp = time.strftime("%Y%m%d-%H%M%S")

    def _compose_label(pid: str) -> str:
        parts = [
            "game",
            str(session) if session is not None else "-",
            str(block) if block is not None else "-",
            pid,
            timestamp,
        ]
        return "|".join(parts)

    order = [pid for pid in ("VP1", "VP2") if pid in target_players]

    try:
        for player_id in order:
            label = _compose_label(player_id)
            log.info("[%s] start_recording label=%s", player_id.lower(), label)
            result = bridge.start_recording(player=player_id, label=label)
            log_recording_result(player_id, result)
            if not result.ok or not result.is_recording_active:
                raise RuntimeError(
                    f"Recording start failed for {player_id}: {result.message}"
                )
    except Exception:
        try:
            bridge.close()
        except Exception:
            log.debug("Fehler beim Aufräumen des Eye-Tracker-Bridges", exc_info=True)
        raise

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
