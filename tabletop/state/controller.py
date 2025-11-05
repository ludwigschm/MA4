"""Game state controller for the tabletop preparation app.

The controller encapsulates the non-UI logic that used to live directly in the
Kivy widgets. It operates purely on plain Python data so the behaviour can be
unit-tested without the graphical environment.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

from tabletop.state.phases import UXPhase


@dataclass
class TabletopState:
    """Container for all mutable game state that is independent of the UI."""

    round: int = 1
    phase: UXPhase = UXPhase.WAIT_BOTH_START
    signaler: int = 1
    judge: int = 2
    first_player: Optional[int] = None
    second_player: Optional[int] = None
    player_roles: Dict[int, int] = field(default_factory=dict)
    role_by_physical: Dict[int, int] = field(default_factory=lambda: {1: 1, 2: 2})
    physical_by_role: Dict[int, int] = field(default_factory=lambda: {1: 1, 2: 2})
    session_configured: bool = False
    session_finished: bool = False
    intro_active: bool = True
    blocks: List[Dict[str, Any]] = field(default_factory=list)
    current_block_idx: int = 0
    current_round_idx: int = 0
    current_block_info: Optional[Dict[str, Any]] = None
    round_in_block: int = 0
    current_block_total_rounds: int = 0
    current_round_has_stake: bool = False
    score_state: Optional[Dict[int, int]] = None
    score_state_block: Optional[int] = None
    score_state_round_start: Optional[Dict[int, int]] = None
    outcome_score_applied: bool = False
    pending_round_start_log: bool = False
    fixation_required: bool = False
    next_block_preview: Optional[Dict[str, Any]] = None
    player_signals: Dict[int, Optional[str]] = field(
        default_factory=lambda: {1: None, 2: None}
    )
    player_decisions: Dict[int, Optional[str]] = field(
        default_factory=lambda: {1: None, 2: None}
    )
    last_outcome: Dict[str, Any] = field(default_factory=dict)
    p1_pressed: bool = False
    p2_pressed: bool = False
    in_block_pause: bool = False
    pause_message: str = ""
    post_fixation_start_required: bool = False


@dataclass
class RoundSetupResult:
    """Information produced while preparing a round."""

    plan: Optional[Dict[str, Any]]


@dataclass
class PhaseApplication:
    """Description of the interactive elements for the current phase."""

    phase: UXPhase
    ready: bool
    start_active: bool
    active_cards: Dict[int, Tuple[str, ...]]
    active_signal_buttons: Dict[int, Tuple[str, ...]]
    active_decision_buttons: Dict[int, Tuple[str, ...]]
    show_showdown: bool


@dataclass
class ContinueResult:
    """Result of continuing after both start buttons were pressed."""

    blocked: bool
    intro_deactivated: bool = False
    requires_fixation: bool = False
    phase: Optional[UXPhase] = None
    await_second_start: bool = False


@dataclass
class PrepareNextRoundResult:
    """Result of preparing the subsequent round."""

    setup: RoundSetupResult
    in_block_pause: bool
    requires_fixation: bool
    session_finished: bool
    start_phase: Optional[UXPhase]
    await_second_start: bool = False


@dataclass
class CardTapResult:
    """Outcome of a card tap interaction."""

    allowed: bool
    record_text: Optional[str] = None
    log_action: Optional[str] = None
    log_payload: Optional[Dict[str, Any]] = None
    next_phase: Optional[UXPhase] = None


@dataclass
class SignalResult:
    """Outcome of a signal selection."""

    accepted: bool
    record_text: Optional[str] = None
    log_payload: Optional[Dict[str, Any]] = None
    next_phase: Optional[UXPhase] = None


@dataclass
class DecisionResult:
    """Outcome of a judge decision."""

    accepted: bool
    record_text: Optional[str] = None
    log_payload: Optional[Dict[str, Any]] = None
    next_phase: Optional[UXPhase] = None


class TabletopController:
    """Pure controller that mutates :class:`TabletopState`.

    The controller never touches UI elements directly. Instead, it updates the
    state and returns data objects that describe what should happen so that the
    caller can update the view layer accordingly.
    """

    def __init__(self, state: TabletopState):
        self.state = state
        self.update_turn_order()

    # ------------------------------------------------------------------ helpers
    def update_turn_order(self) -> None:
        state = self.state
        first = state.signaler if state.signaler in (1, 2) else 1
        if state.judge in (1, 2) and state.judge != first:
            second = state.judge
        else:
            second = 2 if first == 1 else 1
        state.first_player = first
        state.second_player = second
        state.player_roles = {first: 1, second: 2}

    def phase_for_player(self, player: int, which: str) -> Optional[UXPhase]:
        if player not in (1, 2):
            return None
        if which == "inner":
            return UXPhase.P1_INNER if player == 1 else UXPhase.P2_INNER
        if which == "outer":
            return UXPhase.P1_OUTER if player == 1 else UXPhase.P2_OUTER
        return None

    def get_current_plan(self) -> Optional[Tuple[Dict[str, Any], Dict[str, Any]]]:
        state = self.state
        if not state.blocks or state.session_finished or state.in_block_pause:
            return None
        if state.current_block_idx >= len(state.blocks):
            return None
        block = state.blocks[state.current_block_idx]
        rounds = block.get("rounds") or []
        if not rounds:
            return None
        if state.current_round_idx >= len(rounds):
            return None
        return block, rounds[state.current_round_idx]

    def compute_global_round(self) -> int:
        state = self.state
        if not state.blocks:
            return state.round
        total = 0
        for idx, block in enumerate(state.blocks):
            if idx < state.current_block_idx:
                total += len(block.get("rounds") or [])
        if state.current_block_idx >= len(state.blocks):
            return max(1, total)
        return total + state.current_round_idx + 1

    def peek_next_round_info(self) -> Optional[Dict[str, Any]]:
        state = self.state
        if not state.blocks:
            return None
        if state.current_block_idx >= len(state.blocks):
            return None
        block_idx = state.current_block_idx
        round_idx = state.current_round_idx + 1
        while block_idx < len(state.blocks):
            block = state.blocks[block_idx]
            rounds = block.get("rounds") or []
            if round_idx < len(rounds):
                return {
                    "block": block,
                    "round_index": round_idx,
                    "round_in_block": round_idx + 1,
                }
            block_idx += 1
            round_idx = 0
        return None

    def advance_round_pointer(self) -> None:
        state = self.state
        if not state.blocks or state.session_finished:
            state.round += 1
            return
        if state.current_block_idx >= len(state.blocks):
            state.session_finished = True
            return
        block = state.blocks[state.current_block_idx]
        state.current_round_idx += 1
        if state.current_round_idx >= len(block.get("rounds") or []):
            completed_block = block
            state.current_block_idx += 1
            state.current_round_idx = 0
            if state.current_block_idx >= len(state.blocks):
                state.session_finished = True
                state.in_block_pause = False
                state.pause_message = (
                    "Vielen Dank die Teilnahme! Das Experiment ist nun beendet!"
                )
                state.next_block_preview = None
            else:
                state.in_block_pause = True
                next_block = state.blocks[state.current_block_idx]
                condition = "Stake" if next_block.get("payout") else "ohne Stake"
                state.pause_message = (
                    "Dieser Block ist vorbei. Nehmen Sie sich einen Moment zum durchatmen.\n"
                    "Wenn Sie bereit sind klicken Sie auf weiter.\n"
                    f"Als nächstes folgt Block {next_block.get('index')} ({condition})."
                )
                state.next_block_preview = {
                    "block": next_block,
                    "round_index": 0,
                    "round_in_block": 1,
                }
        state.round = self.compute_global_round()

    # ----------------------------------------------------------------- routines
    def setup_round(self) -> RoundSetupResult:
        state = self.state
        state.outcome_score_applied = False
        if state.blocks and not state.session_finished and not state.in_block_pause:
            while (
                state.current_block_idx < len(state.blocks)
                and not state.blocks[state.current_block_idx].get("rounds")
            ):
                state.current_block_idx += 1
        plan_info = self.get_current_plan()
        if plan_info:
            block, plan = plan_info
            state.current_block_info = block
            state.next_block_preview = None
            state.round_in_block = state.current_round_idx + 1
            state.current_round_has_stake = bool(block.get("payout"))
            state.current_block_total_rounds = len(block.get("rounds") or [])
            if block.get("payout") and state.score_state_block != block.get("index"):
                state.score_state = {1: 0, 2: 0}
                state.score_state_block = block.get("index")
            if not block.get("payout"):
                state.score_state = None
                state.score_state_block = None
            state.score_state_round_start = (
                state.score_state.copy() if state.score_state else None
            )
            state.round = self.compute_global_round()
        else:
            if state.current_block_idx >= len(state.blocks):
                state.session_finished = True
            state.current_block_info = None
            state.round_in_block = 0
            state.current_round_has_stake = False
            state.current_block_total_rounds = 0
            state.round = self.compute_global_round()
            state.score_state_round_start = (
                state.score_state.copy() if state.score_state else None
            )
        state.player_signals = {1: None, 2: None}
        state.player_decisions = {1: None, 2: None}
        state.last_outcome = {
            "winner": None,
            "truthful": None,
            "actual_level": None,
            "actual_value": None,
            "judge_value": None,
            "signal_choice": None,
            "judge_choice": None,
            "payout": state.current_round_has_stake,
        }
        state.post_fixation_start_required = False
        if plan_info and state.round_in_block == 1:
            state.fixation_required = True
        elif plan_info:
            state.fixation_required = False
        else:
            state.fixation_required = False
        state.pending_round_start_log = bool(plan_info)
        plan = plan_info[1] if plan_info else None
        return RoundSetupResult(plan=plan)

    def apply_phase(self) -> PhaseApplication:
        state = self.state
        ready = state.session_configured and not state.session_finished
        start_active = state.phase in (UXPhase.WAIT_BOTH_START, UXPhase.SHOWDOWN)
        active_cards: Dict[int, Tuple[str, ...]] = {}
        active_signal_buttons: Dict[int, Tuple[str, ...]] = {}
        active_decision_buttons: Dict[int, Tuple[str, ...]] = {}
        if ready:
            if state.phase == UXPhase.P1_INNER:
                active_cards[1] = ("inner",)
            elif state.phase == UXPhase.P2_INNER:
                active_cards[2] = ("inner",)
            elif state.phase == UXPhase.P1_OUTER:
                active_cards[1] = ("outer",)
            elif state.phase == UXPhase.P2_OUTER:
                active_cards[2] = ("outer",)
            elif state.phase == UXPhase.SIGNALER:
                active_signal_buttons[state.signaler] = ("low", "mid", "high")
            elif state.phase == UXPhase.JUDGE:
                active_decision_buttons[state.judge] = ("bluff", "wahr")
        show_showdown = state.phase == UXPhase.SHOWDOWN
        return PhaseApplication(
            phase=state.phase,
            ready=ready,
            start_active=start_active,
            active_cards=active_cards,
            active_signal_buttons=active_signal_buttons,
            active_decision_buttons=active_decision_buttons,
            show_showdown=show_showdown,
        )

    def continue_after_start_press(self) -> ContinueResult:
        state = self.state
        if state.session_finished:
            return ContinueResult(blocked=True)
        intro_deactivated = False
        if state.intro_active:
            state.intro_active = False
            intro_deactivated = True
        start_phase = self.phase_for_player(state.first_player or 1, "inner")
        if start_phase is None:
            start_phase = UXPhase.P1_INNER
        requires_fixation = bool(state.fixation_required)
        await_second_start = False
        if state.post_fixation_start_required:
            state.post_fixation_start_required = False
            state.phase = start_phase
        elif state.fixation_required:
            state.fixation_required = False
            state.post_fixation_start_required = True
            await_second_start = True
            state.phase = UXPhase.WAIT_BOTH_START
        else:
            state.phase = start_phase
        return ContinueResult(
            blocked=False,
            intro_deactivated=intro_deactivated,
            requires_fixation=requires_fixation,
            phase=state.phase,
            await_second_start=await_second_start,
        )

    def prepare_next_round(
        self, *, start_immediately: bool = False
    ) -> PrepareNextRoundResult:
        state = self.state
        state.signaler, state.judge = state.judge, state.signaler
        self.update_turn_order()
        self.advance_round_pointer()
        state.phase = UXPhase.WAIT_BOTH_START
        setup = self.setup_round()
        start_phase = None
        if not state.session_finished:
            start_phase = self.phase_for_player(state.first_player or 1, "inner")
            if start_phase is None:
                start_phase = UXPhase.P1_INNER
        requires_fixation = bool(state.fixation_required)
        await_second_start = False
        if start_immediately and requires_fixation:
            state.fixation_required = False
            state.post_fixation_start_required = True
            await_second_start = True
        return PrepareNextRoundResult(
            setup=setup,
            in_block_pause=state.in_block_pause,
            requires_fixation=requires_fixation,
            session_finished=state.session_finished,
            start_phase=start_phase,
            await_second_start=await_second_start,
        )

    def tap_card(self, player: int, which: str) -> CardTapResult:
        if which not in {"inner", "outer"}:
            return CardTapResult(allowed=False)
        state = self.state
        expected_phase = self.phase_for_player(player, which)
        if expected_phase is None or state.phase != expected_phase:
            return CardTapResult(allowed=False)
        record_text = "Karte innen aufgedeckt" if which == "inner" else "Karte außen aufgedeckt"
        log_action = "reveal_inner" if which == "inner" else "reveal_outer"
        log_payload = {"card": 1 if which == "inner" else 2}
        first = state.first_player
        second = state.second_player
        next_phase = None
        if which == "inner":
            if player == first:
                next_phase = self.phase_for_player(second or player, "inner")
            else:
                next_phase = self.phase_for_player(first or player, "outer")
        else:
            if player == first:
                next_phase = self.phase_for_player(second or player, "outer")
            else:
                next_phase = UXPhase.SIGNALER
        return CardTapResult(
            allowed=True,
            record_text=record_text,
            log_action=log_action,
            log_payload=log_payload,
            next_phase=next_phase,
        )

    def pick_signal(self, player: int, level: str) -> SignalResult:
        state = self.state
        if state.phase != UXPhase.SIGNALER or player != state.signaler:
            return SignalResult(accepted=False)
        state.player_signals[player] = level
        return SignalResult(
            accepted=True,
            record_text=f"Signal gewählt: {level}",
            log_payload={"level": level},
            next_phase=UXPhase.JUDGE,
        )

    def pick_decision(self, player: int, decision: str) -> DecisionResult:
        state = self.state
        if state.phase != UXPhase.JUDGE or player != state.judge:
            return DecisionResult(accepted=False)
        state.player_decisions[player] = decision
        return DecisionResult(
            accepted=True,
            record_text=f"Entscheidung: {decision.upper()}",
            log_payload={"decision": decision},
            next_phase=UXPhase.SHOWDOWN,
        )

    def compute_outcome(
        self,
        *,
        signaler_total: Optional[int],
        judge_total: Optional[int],
        signaler_value: Optional[int],
        judge_value: Optional[int],
        level_from_value: Callable[[Optional[int]], Optional[str]],
    ) -> Dict[str, Any]:
        state = self.state
        signaler = state.signaler
        judge = state.judge
        signal_choice = state.player_signals.get(signaler)
        judge_choice = state.player_decisions.get(judge)
        actual_total = signaler_total
        judge_total_val = judge_total
        actual_value = signaler_value
        judge_value_val = judge_value
        actual_level = level_from_value(actual_value)
        truthful: Optional[bool] = None
        if signal_choice:
            if actual_level:
                truthful = signal_choice == actual_level
            elif actual_total in (20, 21, 22):
                truthful = False
        winner: Optional[int] = None
        if judge_choice and truthful is not None:
            if judge_choice == "wahr":
                if truthful:
                    if actual_value is not None and judge_value_val is not None:
                        if actual_value > judge_value_val:
                            winner = signaler
                        elif judge_value_val > actual_value:
                            winner = judge
                    else:
                        winner = judge
                else:
                    winner = signaler
            elif judge_choice == "bluff":
                winner = judge if not truthful else signaler
        draw = False
        if (
            judge_choice == "wahr"
            and truthful is True
            and winner is None
            and actual_value is not None
            and judge_value_val is not None
            and actual_value == judge_value_val
        ):
            draw = True
        outcome = {
            "winner": winner,
            "truthful": truthful,
            "actual_level": actual_level,
            "actual_value": actual_value,
            "actual_total": actual_total,
            "judge_total": judge_total_val,
            "judge_value": judge_value_val,
            "signal_choice": signal_choice,
            "judge_choice": judge_choice,
            "payout": state.current_round_has_stake,
            "draw": draw,
        }
        state.last_outcome = outcome
        return outcome


__all__ = [
    "TabletopController",
    "TabletopState",
    "RoundSetupResult",
    "PhaseApplication",
    "ContinueResult",
    "PrepareNextRoundResult",
    "CardTapResult",
    "SignalResult",
    "DecisionResult",
]
