"""Definitions for UX phases and their mapping to engine phases."""
from __future__ import annotations

from enum import Enum
from typing import Dict, Union

from tabletop.engine import Phase as EnginePhase


class UXPhase(Enum):
    """High level phases used by the tabletop UX layer."""

    WAIT_BOTH_START = "WAIT_BOTH_START"
    P1_INNER = "P1_INNER"
    P2_INNER = "P2_INNER"
    P1_OUTER = "P1_OUTER"
    P2_OUTER = "P2_OUTER"
    SIGNALER = "SIGNALER"
    JUDGE = "JUDGE"
    SHOWDOWN = "SHOWDOWN"


_UX_TO_ENGINE: Dict[UXPhase, EnginePhase] = {
    UXPhase.WAIT_BOTH_START: EnginePhase.WAITING_START,
    UXPhase.P1_INNER: EnginePhase.DEALING,
    UXPhase.P2_INNER: EnginePhase.DEALING,
    UXPhase.P1_OUTER: EnginePhase.DEALING,
    UXPhase.P2_OUTER: EnginePhase.DEALING,
    UXPhase.SIGNALER: EnginePhase.SIGNAL_WAIT,
    UXPhase.JUDGE: EnginePhase.CALL_WAIT,
    UXPhase.SHOWDOWN: EnginePhase.REVEAL_SCORE,
}


def to_engine_phase(ux_phase: Union[UXPhase, str]) -> EnginePhase:
    """Return the engine phase corresponding to the given UX phase."""

    if not isinstance(ux_phase, UXPhase):
        try:
            ux_phase = UXPhase(str(ux_phase))
        except ValueError:
            return EnginePhase.DEALING
    return _UX_TO_ENGINE.get(ux_phase, EnginePhase.DEALING)


__all__ = ["UXPhase", "to_engine_phase"]
