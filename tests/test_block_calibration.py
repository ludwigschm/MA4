import os
import sys

os.environ.setdefault("KIVY_WINDOW", "mock")
os.environ.setdefault("KIVY_GRAPHICS", "mock")
os.environ.setdefault("KIVY_AUDIO", "mock")
os.environ.setdefault("KIVY_TEXT", "mock")

import pytest

pytest.importorskip("kivy")

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from tabletop.data.blocks import load_blocks
from tabletop.state.controller import TabletopController, TabletopState
import tabletop.tabletop_view as tabletop_view
from tabletop.tabletop_view import TabletopRoot


def test_block_mapping_fallback(monkeypatch):
    monkeypatch.setattr(tabletop_view, "resolve_background_texture", lambda: None)

    blocks = load_blocks()
    state = TabletopState(blocks=blocks)
    controller = TabletopController(state)

    view = TabletopRoot(controller=controller, state=state)
    view._time_reconciler = None
    view.current_block_info = {"index": 1}
    view._block_time_mapping_block = 1
    mapping = tabletop_view._BlockTimeMapping(
        intercept_ns=10_000.0,
        slope=1.5,
        rms_ns=1.0,
        median_abs_ns=1.0,
        sample_count=3,
    )
    view._block_time_mapping = {"VP1": mapping}

    device_ns, ready = view.map_host_to_device_ns("VP1", 100)
    assert ready is True
    assert device_ns == int(mapping.intercept_ns + mapping.slope * 100)


def test_block_mapping_not_used_for_other_blocks(monkeypatch):
    monkeypatch.setattr(tabletop_view, "resolve_background_texture", lambda: None)

    blocks = load_blocks()
    state = TabletopState(blocks=blocks)
    controller = TabletopController(state)

    view = TabletopRoot(controller=controller, state=state)
    view._time_reconciler = None
    view.current_block_info = {"index": 2}
    view._block_time_mapping_block = 1
    view._block_time_mapping = {
        "VP1": tabletop_view._BlockTimeMapping(0.0, 1.0, 0.0, 0.0, 3)
    }

    device_ns, ready = view.map_host_to_device_ns("VP1", 100)
    assert device_ns is None
    assert ready is False
