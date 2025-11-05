#!/usr/bin/env python3
"""Emit synthetic double markers and report the estimated offset/drift."""

from __future__ import annotations

import argparse
import json
import random
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tabletop.sync.reconciler import TimeReconciler


@dataclass
class _StubBridge:
    """Minimal bridge implementation for the reconciler smoke test."""

    player: str
    offset_ns: int = 0

    def connected_players(self) -> list[str]:
        return [self.player]

    def estimate_time_offset(self, player: str) -> Optional[float]:
        if player != self.player:
            return None
        return float(self.offset_ns) / 1_000_000_000.0

    def refine_event(
        self,
        player: str,
        event_id: str,
        t_ref_ns: int,
        *,
        confidence: float,
        mapping_version: int,
        extra: Optional[dict[str, object]] = None,
    ) -> None:
        # Smoke-tests do not need refinement propagation; accept the call silently.
        return None

    def event_queue_load(self) -> tuple[int, int]:
        return (0, 0)


class _StubLogger:
    """Collect refinement requests in-memory for optional inspection."""

    def __init__(self) -> None:
        self.records: list[dict[str, object]] = []

    def upsert_refinement(
        self,
        event_id: str,
        player: str,
        t_ref_ns: int,
        mapping_version: int,
        confidence: float,
        reason: str,
    ) -> None:
        self.records.append(
            {
                "event_id": event_id,
                "player": player,
                "t_ref_ns": t_ref_ns,
                "mapping_version": mapping_version,
                "confidence": confidence,
                "reason": reason,
            }
        )


def _generate_event_id(index: int) -> str:
    return f"latency-marker-{index:03d}"


def _emit_double_marker(
    reconciler: TimeReconciler,
    *,
    player: str,
    host_ns: int,
    device_ns: int,
    event_id: str,
    mirror_offset_ns: int,
    sink,
) -> None:
    primary_payload = {"event_id": event_id}
    mirror_payload = {"event_id": event_id, "t_host_ns": host_ns}
    reconciler._process_device_event(
        player,
        "sync.test_marker",
        device_ns,
        primary_payload,
    )
    reconciler._process_device_event(
        player,
        "sync.host_ns",
        device_ns + mirror_offset_ns,
        mirror_payload,
    )
    if sink is not None:
        sink.write(
            json.dumps(
                {
                    "player": player,
                    "name": "sync.test_marker",
                    "t_device_ns": device_ns,
                    "payload": primary_payload,
                }
            )
            + "\n"
        )
        sink.write(
            json.dumps(
                {
                    "player": player,
                    "name": "sync.host_ns",
                    "t_device_ns": device_ns + mirror_offset_ns,
                    "payload": mirror_payload,
                }
            )
            + "\n"
        )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Emit synthetic double markers and report drift estimates.",
    )
    parser.add_argument(
        "--player",
        default="VP1",
        help="Name of the simulated player/device (default: %(default)s)",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=10,
        help="Number of double markers to generate (default: %(default)s)",
    )
    parser.add_argument(
        "--offset-ns",
        type=float,
        default=12_500_000.0,
        help="Reference offset in nanoseconds between host and device clocks.",
    )
    parser.add_argument(
        "--drift-ppm",
        type=float,
        default=5.0,
        help="Injected clock drift in parts-per-million (default: %(default)s)",
    )
    parser.add_argument(
        "--interval-ms",
        type=float,
        default=80.0,
        help="Marker spacing on the host clock in milliseconds (default: %(default)s)",
    )
    parser.add_argument(
        "--jitter-ns",
        type=float,
        default=0.0,
        help="Random jitter amplitude applied to device timestamps (default: %(default)s)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Seed for the jitter RNG (optional).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional JSONL output file capturing the generated marker events.",
    )
    return parser


def _create_reconciler(player: str, window_size: int) -> TimeReconciler:
    bridge = _StubBridge(player)
    logger = _StubLogger()
    return TimeReconciler(bridge, logger, window_size=window_size)


def _print_summary(
    *,
    expected_offset_ns: float,
    expected_slope: float,
    estimate,
    sample_count: int,
) -> None:
    if estimate is None:
        print("Keine Schätzung verfügbar – zu wenige Marker?", file=sys.stderr)
        return
    offset_ms = estimate.intercept_ns / 1_000_000.0
    drift_ppm = (estimate.slope - 1.0) * 1_000_000.0
    expected_offset_ms = expected_offset_ns / 1_000_000.0
    expected_drift_ppm = (expected_slope - 1.0) * 1_000_000.0

    print("Offset-Schätzung: %.3f ms (erwartet: %.3f ms)" % (offset_ms, expected_offset_ms))
    print("Drift-Schätzung:  %.3f ppm (erwartet: %.3f ppm)" % (drift_ppm, expected_drift_ppm))
    print("Verwendete Samples: %d" % sample_count)

    if sample_count < 2:
        print("Warnung: Zu wenige Samples für eine robuste Drift-Schätzung.", file=sys.stderr)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _build_parser().parse_args(argv)

    if args.count <= 0:
        print("Es müssen mindestens ein Doppel-Marker erzeugt werden.", file=sys.stderr)
        return 1

    if args.seed is not None:
        random.seed(args.seed)

    slope = 1.0 + args.drift_ppm / 1_000_000.0
    interval_ns = int(args.interval_ms * 1_000_000.0)
    jitter_amplitude = float(args.jitter_ns)
    base_host_ns = 1_000_000_000
    mirror_delta_ns = 2_000

    reconciler = _create_reconciler(args.player, window_size=max(10, args.count * 2))

    sink = None
    try:
        if args.output is not None:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            sink = args.output.open("w", encoding="utf-8")

        for index in range(args.count):
            host_ns = base_host_ns + index * interval_ns
            ideal_device = args.offset_ns + slope * host_ns
            if jitter_amplitude > 0.0:
                noise = random.uniform(-jitter_amplitude, jitter_amplitude)
            else:
                noise = 0.0
            device_ns = int(round(ideal_device + noise))
            event_id = _generate_event_id(index)
            _emit_double_marker(
                reconciler,
                player=args.player,
                host_ns=int(host_ns),
                device_ns=device_ns,
                event_id=event_id,
                mirror_offset_ns=mirror_delta_ns,
                sink=sink,
            )
    finally:
        if sink is not None:
            sink.close()

    estimate = reconciler.estimate_linear_mapping(args.player)
    state = reconciler._player_states.get(args.player)
    sample_count = state.sample_count if state is not None else 0

    _print_summary(
        expected_offset_ns=float(args.offset_ns),
        expected_slope=slope,
        estimate=estimate,
        sample_count=sample_count,
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())
