"""Helpers for loading tabletop block and card configuration data."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Dict, List, Tuple

from tabletop.data.config import CARD_COMBINATIONS_DIR, CARD_DIR


def load_blocks() -> List[Dict[str, Any]]:
    """Load all experiment blocks and their associated round plans.

    Each returned block dictionary exposes a ``csv_path`` entry that
    references the CSV inside the relocated ``Kartenkombinationen`` directory.
    Callers that need the on-disk file can therefore rely on the stored path
    without rebuilding it manually.
    """

    blocks: List[Dict[str, Any]] = []

    practice_path = CARD_COMBINATIONS_DIR / "Paaretest.csv"
    practice_rounds = load_csv_rounds(practice_path)
    if practice_rounds:
        blocks.append(
            {
                "index": 0,
                "label": "Übung",
                "csv": practice_path.name,
                "csv_path": practice_path,
                "path": practice_path,
                "rounds": practice_rounds,
                "payout": False,
                "practice": True,
            }
        )

    order: List[Tuple[int, str, bool]] = [
        (1, "Paare1.csv", False),
        (2, "Paare2.csv", True),
        (3, "Paare3.csv", False),
        (4, "Paare4.csv", True),
    ]

    for index, filename, payout in order:
        path = CARD_COMBINATIONS_DIR / filename
        rounds = load_csv_rounds(path)
        blocks.append(
            {
                "index": index,
                "label": f"Block {index}",
                "csv": filename,
                "csv_path": path,
                "path": path,
                "rounds": rounds,
                "payout": payout,
                "practice": False,
            }
        )

    return blocks


def load_csv_rounds(path: Path) -> List[Dict[str, Any]]:
    """Parse a CSV file describing the VP card assignments for each round."""

    rounds: List[Dict[str, Any]] = []

    try:
        with open(path, newline="", encoding="utf-8") as fp:
            rows = list(csv.reader(fp))
    except (FileNotFoundError, Exception):
        return rounds

    def parse_cards(row: List[str], start: int, end: int) -> Tuple[int, int]:
        values: List[int] = []
        for idx in range(start, min(end, len(row))):
            cell = (row[idx] or "").strip()
            if not cell:
                continue
            try:
                values.append(int(float(cell)))
            except ValueError:
                continue
            if len(values) == 2:
                break
        if len(values) < 2:
            raise ValueError("Zu wenige Karten")
        return tuple(values[:2])  # type: ignore[return-value]

    def parse_numeric(cell: Any) -> int | None:
        if cell is None:
            return None
        if isinstance(cell, (int, float)):
            return int(cell)
        text = str(cell).strip().replace(",", ".")
        if not text:
            return None
        try:
            return int(float(text))
        except ValueError:
            return None

    def parse_category(cell: Any) -> str | None:
        text = (cell or "").strip().strip('"').lower()
        return text or None

    start_idx = 0
    if rows:
        try:
            parse_cards(rows[0], 2, 6)
            parse_cards(rows[0], 7, 11)
        except Exception:
            start_idx = 1

    for row in rows[start_idx:]:
        if not row or all((cell or "").strip() == "" for cell in row):
            continue
        try:
            vp1_cards = parse_cards(row, 2, 6)
            vp2_cards = parse_cards(row, 7, 11)
        except Exception:
            continue

        vp1_value = parse_numeric(row[5]) if len(row) > 5 else None
        vp2_value = parse_numeric(row[10]) if len(row) > 10 else None
        vp1_category = parse_category(row[1]) if len(row) > 1 else None
        vp2_category = parse_category(row[6]) if len(row) > 6 else None

        if vp1_value is None:
            total = sum(vp1_cards)
            vp1_value = 0 if total in (20, 21, 22) else total
        if vp2_value is None:
            total = sum(vp2_cards)
            vp2_value = 0 if total in (20, 21, 22) else total

        rounds.append(
            {
                "vp1": vp1_cards,
                "vp2": vp2_cards,
                "vp1_value": vp1_value,
                "vp2_value": vp2_value,
                "vp1_category": vp1_category,
                "vp2_category": vp2_category,
            }
        )

    return rounds


def value_to_card_path(value: Any) -> str:
    """Map a numeric card value to the corresponding card texture path."""

    fallback = CARD_DIR / "back.png"

    try:
        number = int(value)
    except (TypeError, ValueError):
        return str(fallback)

    filename = f"{number}.png"
    path = CARD_DIR / filename
    return str(path) if path.exists() else str(fallback)


__all__ = [
    "load_blocks",
    "load_csv_rounds",
    "value_to_card_path",
]
