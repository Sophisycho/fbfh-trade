# logger.py
# -*- coding: utf-8 -*-
"""
A lightweight, importable logging utility with level toggles and optional colors/file output.

- Toggle by setting 1/0 variables directly in this module:
    PRINT_DEBUG, PRINT_INFO, PRINT_WARN, PRINT_ERROR, PRINT_SUCCESS, USE_COLOR, LOG_TO_FILE
- Customize LOG_FILE_PATH to enable file logging.
- Usage:
    from fbfh_trade import logger as log
    log.PRINT_DEBUG = 1
    log.info("Hello")
"""

from __future__ import annotations
import sys
import datetime
from typing import Optional

# ===== Toggle switches (1 = on, 0 = off) =====
PRINT_DEBUG = 0
PRINT_INFO = 1
PRINT_WARN = 1
PRINT_ERROR = 1
PRINT_SUCCESS = 1

USE_COLOR = 1
LOG_TO_FILE = 0
LOG_FILE_PATH = "app.log"


# ===== Internal color helpers =====
def _supports_color() -> bool:
    if not USE_COLOR:
        return False
    if sys.platform == "win32":
        try:
            import colorama  # type: ignore
            colorama.just_fix_windows_console()
            return True
        except Exception:
            return False
    return sys.stdout.isatty()


_COLOR_ENABLED = _supports_color()

# ANSI codes (only applied if _COLOR_ENABLED=True)
_COLORS = {
    "DEBUG": "\033[90m",   # bright black / gray
    "INFO": "\033[36m",    # cyan
    "WARN": "\033[33m",    # yellow
    "ERROR": "\033[31m",   # red
    "SUCCESS": "\033[32m", # green
    "RESET": "\033[0m",
}


def _now_str() -> str:
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _format(level: str, message: str) -> str:
    timestamp = _now_str()
    base = f"[{timestamp}] [{level}] {message}"
    if _COLOR_ENABLED:
        color = _COLORS.get(level, "")
        reset = _COLORS["RESET"]
        return f"{color}{base}{reset}"
    return base


def _write_line(line: str) -> None:
    print(line)
    if LOG_TO_FILE:
        try:
            with open(LOG_FILE_PATH, "a", encoding="utf-8") as f:
                # 寫入純文字（無 ANSI 顏色）
                f.write(line.replace(_COLORS.get("RESET", ""), "") + "\n")
        except Exception:
            # 檔案寫入不得影響主流程；若失敗，靜默略過。
            pass


def debug(message: str) -> None:
    """Print a DEBUG-level log if enabled."""
    if PRINT_DEBUG:
        _write_line(_format("DEBUG", message))


def info(message: str) -> None:
    """Print an INFO-level log if enabled."""
    if PRINT_INFO:
        _write_line(_format("INFO", message))


def warn(message: str) -> None:
    """Print a WARN-level log if enabled."""
    if PRINT_WARN:
        _write_line(_format("WARN", message))


def error(message: str) -> None:
    """Print an ERROR-level log if enabled."""
    if PRINT_ERROR:
        _write_line(_format("ERROR", message))


def success(message: str) -> None:
    """Print a SUCCESS-level log if enabled."""
    if PRINT_SUCCESS:
        _write_line(_format("SUCCESS", message))

