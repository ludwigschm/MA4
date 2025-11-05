"""Unit tests for the time reconciler and related infrastructure."""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Optional

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytest

from tabletop.engine import EventLogger
from tabletop.sync.reconciler import TimeReconciler


class _FakeBridge:
    def __init__(self, players: Iterable[str]) -> None:
        self._players = list(players)
        self.offsets_ns: Dict[str, int] = {player: 0 for player in self._players}
        self.refinements: List[Dict[str, object]] = []

    def connected_players(self) -> List[str]:
        return list(self._players)

    def estimate_time_offset(self, player: str) -> Optional[float]:
        return self.offsets_ns.get(player, 0) / 1_000_000_000.0

    def refine_event(
        self,
        player: str,
        event_id: str,
        t_ref_ns: int,
        *,
        confidence: float,
        mapping_version: int,
        extra: Optional[Dict[str, object]] = None,
    ) -> None:
        payload = {
            "player": player,
            "event_id": event_id,
            "t_ref_ns": t_ref_ns,
            "confidence": confidence,
            "mapping_version": mapping_version,
            "extra": extra or {},
        }
        self.refinements.append(payload)

    def event_queue_load(self) -> tuple[int, int]:
        return (0, 0)


class _FakeLogger:
    def __init__(self) -> None:
        self.records: List[Dict[str, object]] = []

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


def _inject_marker(
    reconciler: TimeReconciler,
    bridge: _FakeBridge,
    players: Iterable[str],
    models: Dict[str, tuple[float, float]],
    t_local_ns: int,
) -> None:
    for player in players:
        intercept_ns, slope = models[player]
        device_ns = intercept_ns + slope * t_local_ns
        offset_ns = int(device_ns - t_local_ns)
        bridge.offsets_ns[player] = offset_ns
    reconciler._process_marker("hb", t_local_ns)


def test_reconciler_builds_per_player_models_and_refines(caplog: pytest.LogCaptureFixture) -> None:
    players = ["VP1", "VP2"]
    models = {"VP1": (20_000_000.0, 1.00002), "VP2": (-5_000_000.0, 0.99998)}
    bridge = _FakeBridge(players)
    logger = _FakeLogger()
    reconciler = TimeReconciler(bridge, logger, window_size=25)

    start_ns = 1_000_000_000
    with caplog.at_level(logging.INFO):
        for index in range(60):
            t_local_ns = start_ns + index * 50_000_000
            _inject_marker(reconciler, bridge, players, models, t_local_ns)

    event_times: Dict[str, int] = {}
    for idx in range(5):
        event_id = f"evt{idx}"
        t_local_ns = start_ns + 25_000_000 + idx * 70_000_000
        event_times[event_id] = t_local_ns
        reconciler._process_event(event_id, t_local_ns)

    assert len(bridge.refinements) == len(players) * 5
    assert len(logger.records) == len(players) * 5

    state_vp1 = reconciler._player_states["VP1"]
    state_vp2 = reconciler._player_states["VP2"]

    assert state_vp1.rms_ns < 2_000_000
    assert state_vp2.rms_ns < 2_000_000

    assert state_vp1.mapping_version > 0
    assert state_vp2.mapping_version > 0

    vp1_refs = [item for item in bridge.refinements if item["player"] == "VP1"]
    vp2_refs = [item for item in bridge.refinements if item["player"] == "VP2"]

    assert vp1_refs and vp2_refs
    assert any(
        abs(ref["t_ref_ns"] - vp2_refs[idx]["t_ref_ns"]) > 1_000
        for idx, ref in enumerate(vp1_refs)
    )

    refined_map = {
        (item["player"], item["event_id"]): item["t_ref_ns"] for item in bridge.refinements
    }

    for record in logger.records:
        key = (record["player"], record["event_id"])
        assert key in refined_map
        intercept_ns, slope = models[record["player"]]
        expected_ns = intercept_ns + slope * event_times[record["event_id"]]
        assert abs(refined_map[key] - expected_ns) < 5_000_000

    info_messages = " ".join(record.getMessage() for record in caplog.records)
    assert "Mapping update VP1" in info_messages
    assert "Mapping update VP2" in info_messages


def test_reconciler_inverts_wrong_offset_sign(caplog: pytest.LogCaptureFixture) -> None:
    players = ["VP1"]
    models = {"VP1": (12_000_000.0, 1.00001)}
    bridge = _FakeBridge(players)
    logger = _FakeLogger()
    reconciler = TimeReconciler(bridge, logger, window_size=20)

    start_ns = 2_000_000_000
    with caplog.at_level(logging.WARNING):
        for index in range(40):
            t_local_ns = start_ns + index * 40_000_000
            for player in players:
                intercept_ns, slope = models[player]
                device_ns = intercept_ns + slope * t_local_ns
                if index < 10:
                    measured_offset = int(device_ns - t_local_ns)
                else:
                    measured_offset = int(t_local_ns - device_ns)
                bridge.offsets_ns[player] = measured_offset
            reconciler._process_marker("hb", t_local_ns)

    state = reconciler._player_states["VP1"]
    assert state.offset_sign == -1
    assert any("Offset semantics inverted" in record.getMessage() for record in caplog.records)


def test_reconciler_pairs_host_mirror_samples() -> None:
    players = ["VP1"]
    model = {"VP1": (15_000_000.0, 1.00002)}
    bridge = _FakeBridge(players)
    logger = _FakeLogger()
    reconciler = TimeReconciler(
        bridge,
        logger,
        window_size=25,
        marker_pair_weight=3.0,
    )

    start_ns = 5_000_000_000
    for index in range(30):
        t_local_ns = start_ns + index * 40_000_000
        _inject_marker(reconciler, bridge, players, model, t_local_ns)

    for index in range(5):
        event_id = f"sync{index}"
        t_host_ns = start_ns + 500_000_000 + index * 10_000_000
        intercept_ns, slope = model["VP1"]
        device_ns = int(intercept_ns + slope * t_host_ns)
        reconciler._process_device_event(
            "VP1",
            "sync.flash_start",
            device_ns,
            {"event_id": event_id},
        )
        reconciler._process_device_event(
            "VP1",
            "sync.host_ns",
            device_ns + 1_000,
            {"event_id": event_id, "t_host_ns": t_host_ns},
        )

    state = reconciler._player_states["VP1"]
    assert state.sample_count >= 2
    assert state.raw_offsets, "expected raw offsets from host mirror pairs"
    last_weight = state.raw_offsets[-1][3]
    assert pytest.approx(last_weight, rel=0.0, abs=1e-6) == reconciler._marker_pair_weight
    expected_intercept = model["VP1"][0]
    assert abs(state.intercept_ns - expected_intercept) < 5_000_000
    assert state.confidence >= TimeReconciler.CONF_MIN


def test_event_logger_supports_per_player_refinements(tmp_path: Path) -> None:
    db_path = tmp_path / "events.sqlite3"
    logger = EventLogger(str(db_path))
    logger.upsert_refinement("event-1", "VP1", 100, 1, 0.9, "test")
    logger.upsert_refinement("event-1", "VP2", 120, 2, 0.85, "test")

    cur = logger.conn.cursor()
    cur.execute(
        "SELECT player, t_ref_ns, mapping_version, confidence, reason FROM event_refinements WHERE event_id=?",
        ("event-1",),
    )
    rows = sorted(cur.fetchall())
    assert rows == [
        ("VP1", 100, 1, 0.9, "test"),
        ("VP2", 120, 2, 0.85, "test"),
    ]
    logger.close()
