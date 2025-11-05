"""Centralised configuration values for the tabletop application.

This module exposes path helpers that are required across multiple
subsystems.  Keeping them in a dedicated place avoids having to recompute
directory information in different modules and allows the UI layer to share
the same assumptions as the engine layer without duplicating constants.
"""

from __future__ import annotations

from pathlib import Path


# --- Root paths -----------------------------------------------------------

#: Absolute path to the repository root (directory containing ``app_vorbereitung.py``).
ROOT: Path = Path(__file__).resolve().parents[2]

#: Base directory holding UX assets such as button icons and backgrounds.
UX_DIR: Path = ROOT / "UX"

#: Directory containing all card face textures.
CARD_DIR: Path = ROOT / "Karten"

#: Directory holding CSV round definitions for card combinations.
CARD_COMBINATIONS_DIR: Path = ROOT / "Kartenkombinationen"

#: Location of the optional ArUco overlay helper script.
ARUCO_OVERLAY_PATH: Path = ROOT / "tabletop" / "aruco_overlay.py"


__all__ = [
    "ROOT",
    "UX_DIR",
    "CARD_DIR",
    "CARD_COMBINATIONS_DIR",
    "ARUCO_OVERLAY_PATH",
]

