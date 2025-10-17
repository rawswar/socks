from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, Optional

PROJECT_ROOT = Path(__file__).resolve().parent
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from socks5_proxy_system import Coordinator


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
    args = parse_args()
    overrides = load_overrides(args.config)
    coordinator = Coordinator(config_overrides=overrides)
    coordinator.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
