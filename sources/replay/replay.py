#!/usr/bin/env python3
"""
LogPilot Replay Service
=======================
Reads historical log files (HDFS, OpenSSH, Apache, or generic ISO 8601),
rewrites timestamps to the current time (UTC), and writes lines at a configurable
playback speed (1x, 10x, max throughput).

Usage:
    # 1x real-time speed to an output file:
    python replay.py data/loghub/HDFS_2k.log -o logs/hdfs.log --speed 1x

    # 10x accelerated speed:
    python replay.py data/loghub/OpenSSH_2k.log -o logs/openssh.log --speed 10x

    # Maximum throughput (benchmarking mode, no artificial sleep):
    python replay.py data/loghub/HDFS.log -o logs/hdfs.log --speed max

    # Stream directly to stdout:
    python replay.py data/loghub/Apache_2k.log --speed 10x
"""

import argparse
import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import Callable, Dict, Optional, Tuple


# ------------------------------------------------------------------------------
# Timestamp Patterns and Parsers
# ------------------------------------------------------------------------------
PATTERNS = [
    # HDFS: "081109 203518" (YYMMDD HHMMSS)
    {
        "name": "hdfs",
        "regex": re.compile(r"^(\d{6}\s+\d{6})"),
        "parse": lambda s: datetime.strptime(s, "%y%m%d %H%M%S").replace(tzinfo=timezone.utc),
        "format": lambda dt: dt.strftime("%y%m%d %H%M%S"),
    },
    # Apache error log: "[Sun Dec 04 04:51:08 2005]"
    {
        "name": "apache",
        "regex": re.compile(r"^\[([A-Za-z]{3}\s+[A-Za-z]{3}\s+\d+\s+\d{2}:\d{2}:\d{2}(?:\.\d+)?\s+\d{4})\]"),
        "parse": lambda s: datetime.strptime(re.sub(r"\.\d+", "", s), "%a %b %d %H:%M:%S %Y").replace(tzinfo=timezone.utc),
        "format": lambda dt: dt.strftime("%a %b %d %H:%M:%S %Y"),
    },
    # OpenSSH / Syslog: "Dec 10 06:55:46" or "Dec  5 06:55:46"
    {
        "name": "openssh",
        "regex": re.compile(r"^([A-Z][a-z]{2}\s+\d+\s+\d{2}:\d{2}:\d{2})"),
        "parse": lambda s: datetime.strptime(s, "%b %d %H:%M:%S").replace(
            year=datetime.now(timezone.utc).year, tzinfo=timezone.utc
        ),
        "format": lambda dt: f"{dt.strftime('%b')} {dt.day:2d} {dt.strftime('%H:%M:%S')}",
    },
    # ISO 8601 / Shop logs: "2026-11-20 14:02:11" or "2026-11-20T14:02:11.123"
    {
        "name": "iso8601",
        "regex": re.compile(r"^(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?)"),
        "parse": lambda s: datetime.fromisoformat(s.replace(" ", "T")).replace(tzinfo=timezone.utc),
        "format": lambda dt: dt.strftime("%Y-%m-%d %H:%M:%S"),
    },
]


def detect_pattern(line: str) -> Optional[Dict]:
    """Auto-detect timestamp format from the first matching line."""
    for p in PATTERNS:
        if p["regex"].search(line):
            return p
    return None


def parse_speed(speed_arg: str) -> Tuple[Optional[float], bool]:
    """
    Parse speed argument.
    Returns (speed_multiplier, is_max_mode).
    """
    cleaned = speed_arg.strip().lower()
    if cleaned in ("max", "maximum", "0", "0x", "inf", "fast"):
        return (None, True)
    if cleaned.endswith("x"):
        cleaned = cleaned[:-1]
    try:
        val = float(cleaned)
        if val <= 0:
            return (None, True)
        return (val, False)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid speed value: '{speed_arg}'. Expected '1x', '10x', 'max', or a positive float."
        )


# ------------------------------------------------------------------------------
# Core Replay Logic
# ------------------------------------------------------------------------------
def replay_file(
    input_path: str,
    output_path: Optional[str] = None,
    speed_arg: str = "1x",
    format_hint: str = "auto",
    max_delay: float = 5.0,
    limit: Optional[int] = None,
    loop: bool = False,
    flush_always: bool = True,
):
    speed_val, is_max = parse_speed(speed_arg)
    speed_display = "MAX" if is_max else f"{speed_val}x"

    if not os.path.isfile(input_path):
        sys.stderr.write(f"Error: Input file '{input_path}' not found.\n")
        sys.exit(1)

    # Prepare output stream
    out_file = None
    if output_path and output_path != "-":
        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
        out_file = open(output_path, "a", encoding="utf-8", buffering=1 if flush_always else -1)
        target_stream = out_file
        dest_display = output_path
    else:
        target_stream = sys.stdout
        dest_display = "<stdout>"

    sys.stderr.write(
        f"==> LogPilot Replay started\n"
        f"    Source:  {input_path}\n"
        f"    Target:  {dest_display}\n"
        f"    Speed:   {speed_display}\n"
        f"    Looping: {loop}\n\n"
    )

    pattern = None
    if format_hint != "auto":
        for p in PATTERNS:
            if p["name"] == format_hint.lower():
                pattern = p
                break
        if not pattern:
            sys.stderr.write(f"Warning: Unknown format hint '{format_hint}', falling back to auto-detect.\n")

    total_lines = 0
    start_wall_time = time.time()
    last_stat_time = start_wall_time
    last_stat_lines = 0

    try:
        while True:
            orig_t0: Optional[datetime] = None
            wall_t0: Optional[float] = None

            with open(input_path, "r", encoding="utf-8", errors="replace") as in_file:
                for raw_line in in_file:
                    if limit and total_lines >= limit:
                        return

                    now_wall = time.time()
                    rewritten_line = raw_line

                    # Determine pattern for this line
                    active_p = pattern
                    if active_p is None or not active_p["regex"].search(raw_line):
                        if format_hint == "auto":
                            detected = detect_pattern(raw_line)
                            if detected:
                                active_p = detected
                                if pattern is None:
                                    pattern = detected
                                    sys.stderr.write(f"    Detected format: '{pattern['name']}'\n")

                    if active_p:
                        m = active_p["regex"].search(raw_line)
                        if m:
                            matched_text = m.group(1) if m.groups() else m.group(0)
                            try:
                                log_dt = active_p["parse"](matched_text)

                                if orig_t0 is None:
                                    orig_t0 = log_dt
                                    wall_t0 = now_wall

                                if not is_max and speed_val:
                                    # Calculate simulated wall time
                                    delta_log_seconds = (log_dt - orig_t0).total_seconds()
                                    if max_delay > 0 and delta_log_seconds > max_delay * speed_val:
                                        # Cap large gaps in historical data
                                        delta_log_seconds = max_delay * speed_val
                                        orig_t0 = log_dt

                                    target_wall = wall_t0 + (delta_log_seconds / speed_val)
                                    sleep_secs = target_wall - now_wall

                                    if sleep_secs > 0:
                                        time.sleep(sleep_secs)

                                # Current timestamp in UTC
                                current_utc = datetime.now(timezone.utc)
                                new_ts_str = active_p["format"](current_utc)

                                # Replace timestamp in line
                                span = m.span(1) if m.groups() else m.span(0)
                                rewritten_line = raw_line[:span[0]] + new_ts_str + raw_line[span[1]:]

                            except (ValueError, OSError):
                                pass

                    target_stream.write(rewritten_line)
                    if flush_always:
                        target_stream.flush()

                    total_lines += 1

                    # Periodic statistics every 5 seconds
                    now = time.time()
                    if now - last_stat_time >= 5.0:
                        interval = now - last_stat_time
                        lines_in_interval = total_lines - last_stat_lines
                        rate = lines_in_interval / interval
                        sys.stderr.write(
                            f"[{datetime.now().strftime('%H:%M:%S')}] Replayed {total_lines:,} lines "
                            f"({rate:,.1f} lines/sec)\n"
                        )
                        last_stat_time = now
                        last_stat_lines = total_lines

            if not loop:
                break

    except KeyboardInterrupt:
        sys.stderr.write("\nReplay interrupted by user (SIGINT).\n")
    finally:
        if out_file and not out_file.closed:
            out_file.close()

        elapsed = max(0.001, time.time() - start_wall_time)
        overall_rate = total_lines / elapsed
        sys.stderr.write(
            f"\n==> Replay finished: {total_lines:,} total lines in {elapsed:.2f}s "
            f"({overall_rate:,.1f} lines/sec)\n"
        )


# ------------------------------------------------------------------------------
# Entry Point
# ------------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="LogPilot Replay Service — Streams historical logs with real-time timestamps at configurable speed."
    )
    parser.add_argument(
        "file",
        help="Path to historical log file (e.g. data/loghub/HDFS_2k.log)",
    )
    parser.add_argument(
        "-o",
        "--output",
        default=None,
        help="Destination log file to write/tail (default: stdout)",
    )
    parser.add_argument(
        "-s",
        "--speed",
        default="1x",
        help="Playback speed multiplier: 1x, 10x, max, or float (default: 1x)",
    )
    parser.add_argument(
        "-f",
        "--format",
        default="auto",
        choices=["auto", "hdfs", "openssh", "apache", "iso8601"],
        help="Log timestamp format (default: auto)",
    )
    parser.add_argument(
        "--max-delay",
        type=float,
        default=5.0,
        help="Cap consecutive inter-log delays in historical datasets to this many seconds (default: 5.0)",
    )
    parser.add_argument(
        "-n",
        "--limit",
        type=int,
        default=None,
        help="Maximum number of lines to replay",
    )
    parser.add_argument(
        "-l",
        "--loop",
        action="store_true",
        help="Loop the log file continuously",
    )
    parser.add_argument(
        "--no-flush",
        action="store_true",
        help="Disable per-line flush (useful for maximizing raw I/O throughput)",
    )

    args = parser.parse_args()

    replay_file(
        input_path=args.file,
        output_path=args.output,
        speed_arg=args.speed,
        format_hint=args.format,
        max_delay=args.max_delay,
        limit=args.limit,
        loop=args.loop,
        flush_always=not args.no_flush,
    )


if __name__ == "__main__":
    main()
