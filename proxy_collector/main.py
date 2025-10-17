from __future__ import annotations

import logging
import sys

from .cli import main as _run_main


def main() -> int:
    return _run_main()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        logging.error("Fatal error: %s", exc, exc_info=True)
        sys.exit(1)
