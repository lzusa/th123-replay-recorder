#!/usr/bin/env python3
"""
Touhou 12.3 Replay Recording Service - Entry Point
PyInstaller should package this file as the executable entry.
"""

import argparse
import io
import logging
import os
import sys

from constants import DEFAULT_OUTPUT_DIR
from service import run_main


def configure_console_encoding():
    if sys.platform == "win32":
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")


def configure_debug_logging():
    if os.environ.get("REPLAY_DEBUG"):
        logging.getLogger().setLevel(logging.DEBUG)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Touhou 12.3 Replay Recording Service (Parallel)")
    parser.add_argument("--output", "-o", default=DEFAULT_OUTPUT_DIR, help="Output directory for replays")
    parser.add_argument("--poll", "-p", type=float, default=30.0, help="Poll interval in seconds")
    parser.add_argument(
        "--duration",
        "-d",
        type=float,
        default=0.0,
        help="Capture duration in seconds per game. Use 0 to wait until match end (default)",
    )
    parser.add_argument(
        "--workers",
        "-w",
        type=int,
        default=30,
        help="Max concurrent connections (default: 30 = 2 cores x 15 workers)",
    )
    parser.add_argument("--once", "-1", action="store_true", help="Run once instead of continuously")
    return parser


def main():
    configure_console_encoding()
    configure_debug_logging()
    parser = build_parser()
    args = parser.parse_args()
    run_main(args)


if __name__ == "__main__":
    main()
