import sys
from typing import List

from models import stats


# Track how many lines the last status display used, for clearing.
_last_status_lines = 0


def print_status_display():
    """Print a multi-line live status display showing all active captures.
    Uses cursor movement to update in place, compatible with Windows Terminal.
    """
    global _last_status_lines
    lines: List[str] = []
    lines.append(stats.get_status_line())
    active = stats.get_active_games_snapshot()
    if active:
        lines.append(f"{'IP':<24} {'Host':<20} {'Client':<20} {'Match':>5} {'Frames':>7} {'Saved':>5} {'Status'}")
        lines.append("-" * 96)
        for g in active:
            h_display = f"{g.host_name}({g.host_char})" if g.host_char else g.host_name
            c_display = f"{g.client_name}({g.client_char})" if g.client_char else g.client_name
            h_display = (h_display[:17] + "...") if len(h_display) > 20 else h_display
            c_display = (c_display[:17] + "...") if len(c_display) > 20 else c_display
            lines.append(
                f"{g.ip:<24} {h_display:<20} {c_display:<20} {g.match_id:>5} {g.frame_count:>7} {g.saved_count:>5} {g.status}"
            )
    else:
        lines.append("No active captures")

    if _last_status_lines > 0:
        sys.stdout.write(f"\033[{_last_status_lines}A")

    for i, line in enumerate(lines):
        if i > 0:
            sys.stdout.write("\n")
        sys.stdout.write(f"\r\033[K{line}")

    if _last_status_lines > len(lines):
        for _ in range(_last_status_lines - len(lines)):
            sys.stdout.write("\n\r\033[K")

    sys.stdout.flush()
    _last_status_lines = len(lines)


def print_status_line():
    """Print a single status line that updates in place (legacy)"""
    print_status_display()
