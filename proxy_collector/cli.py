from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional

from .coordinator import Coordinator


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="SOCKS5 proxy collection and validation pipeline")
    parser.add_argument(
        "--config",
        type=Path,
        help="Path to a JSON configuration file overriding defaults",
    )
    return parser.parse_args()


def load_overrides(path: Optional[Path]) -> Optional[Dict[str, Any]]:
    if not path:
        return None
    if not path.exists():
        logging.error("Configuration file %s does not exist", path)
        return None
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except json.JSONDecodeError as exc:
        logging.error("Unable to parse configuration file %s: %s", path, exc)
    return None


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    args = parse_args()
    overrides = load_overrides(args.config)

    coordinator = Coordinator(config_overrides=overrides)
    coordinator.run()
    return 0


__all__ = [
    "main",
    "parse_args",
    "load_overrides",
]
