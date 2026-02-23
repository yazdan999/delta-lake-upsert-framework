"""
Basic logging utility for structured messages.
"""

from datetime import datetime, timezone


def log_info(message: str) -> None:
    ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    print(f"[INFO] {ts} - {message}")


def log_warning(message: str) -> None:
    ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    print(f"[WARN] {ts} - {message}")


def log_error(message: str) -> None:
    ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    print(f"[ERROR] {ts} - {message}")
