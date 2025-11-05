#!/usr/bin/env python3
"""Replay events and adjust timestamps by an offset/slope mapping."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Iterable, Optional

_TIMESTAMP_SUFFIX = "_ns"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Replay JSONL events and shift *_ns timestamps using an affine mapping."
        ),
    )
    parser.add_argument(
        "input",
        type=Path,
        help="Path to the source JSONL file containing events.",
    )
    parser.add_argument(
        "output",
        type=Path,
        help="Destination JSONL file that will receive the transformed events.",
    )
    parser.add_argument(
        "--offset-ns",
        type=float,
        default=0.0,
        help="Offset in nanoseconds applied after scaling (default: %(default)s).",
    )
    parser.add_argument(
        "--slope",
        type=float,
        default=1.0,
        help="Slope factor used to scale timestamps before adding the offset.",
    )
    parser.add_argument(
        "--suffix",
        default=_TIMESTAMP_SUFFIX,
        help="Key suffix identifying timestamp fields (default: %(default)s).",
    )
    return parser


def _transform_value(
    value: Any,
    *,
    key: Optional[str],
    slope: float,
    offset: float,
    suffix: str,
):
    if isinstance(value, dict):
        return {
            sub_key: _transform_value(
                sub_value,
                key=sub_key,
                slope=slope,
                offset=offset,
                suffix=suffix,
            )
            for sub_key, sub_value in value.items()
        }
    if isinstance(value, list):
        return [
            _transform_value(item, key=None, slope=slope, offset=offset, suffix=suffix)
            for item in value
        ]
    if key and key.endswith(suffix) and isinstance(value, (int, float)):
        transformed = offset + slope * float(value)
        return int(round(transformed))
    return value


def _transform_line(
    line: str,
    *,
    slope: float,
    offset: float,
    suffix: str,
) -> str:
    record = json.loads(line)
    if not isinstance(record, dict):
        return line
    transformed = {
        key: _transform_value(value, key=key, slope=slope, offset=offset, suffix=suffix)
        for key, value in record.items()
    }
    return json.dumps(transformed, ensure_ascii=False)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _build_parser().parse_args(argv)

    if args.slope <= 0:
        raise SystemExit("Slope must be greater than zero.")

    source = args.input
    destination = args.output
    if source.resolve() == destination.resolve():
        raise SystemExit("Input and output path must differ.")

    destination.parent.mkdir(parents=True, exist_ok=True)

    slope = float(args.slope)
    offset = float(args.offset_ns)
    suffix = str(args.suffix)

    with source.open("r", encoding="utf-8") as inp, destination.open(
        "w", encoding="utf-8"
    ) as out:
        transformed = 0
        for line in inp:
            stripped = line.strip()
            if not stripped:
                continue
            out.write(
                _transform_line(
                    stripped,
                    slope=slope,
                    offset=offset,
                    suffix=suffix,
                )
                + "\n"
            )
            transformed += 1

    print(
        "%d events written to %s (offset=%.3fms slope=%.6f)"
        % (
            transformed,
            destination,
            offset / 1_000_000.0,
            slope,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
