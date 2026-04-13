#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.analyze_minrepo import run_analysis
from scripts.collect_minrepo import run_collection


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the collection + analysis pipeline.")
    parser.add_argument("--config", default="config.toml", help="Path to TOML config")
    args = parser.parse_args()

    collect_code = run_collection(args.config)
    if collect_code != 0:
        return collect_code
    return run_analysis(args.config)


if __name__ == "__main__":
    raise SystemExit(main())
