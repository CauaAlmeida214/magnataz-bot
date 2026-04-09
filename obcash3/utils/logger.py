from __future__ import absolute_import
"""
Structured logging for OB CASH 3.0.

Works in both source mode and PyInstaller executables without writing logs
into the temporary _MEI extraction directory.
"""

import json
import logging
import os
import sys
import tempfile
import threading
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, Optional

# Thread-local storage for contextual information
_thread_context = threading.local()


class JsonFormatter(logging.Formatter):
    """Format log records as JSON."""

    def format(self, record: logging.LogRecord) -> str:
        log_obj = {
            "timestamp": datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        if record.exc_info:
            log_obj["exception"] = self.formatException(record.exc_info)

        for key, value in record.__dict__.items():
            if key not in [
                "name",
                "msg",
                "args",
                "levelname",
                "levelno",
                "pathname",
                "filename",
                "module",
                "exc_info",
                "exc_text",
                "stack_info",
                "lineno",
                "funcName",
                "created",
                "msecs",
                "relativeCreated",
                "thread",
                "threadName",
                "processName",
                "process",
            ]:
                log_obj[key] = value

        context = getattr(_thread_context, "context", {})
        if context:
            log_obj["context"] = context

        return json.dumps(log_obj, ensure_ascii=False)


class SimpleFormatter(logging.Formatter):
    """Human-readable formatter for console output."""

    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    RESET = "\033[0m"
    BOLD = "\033[1m"

    COLORS = {
        "DEBUG": RESET,
        "INFO": GREEN,
        "WARNING": YELLOW,
        "ERROR": RED,
        "CRITICAL": RED + BOLD,
    }

    def format(self, record: logging.LogRecord) -> str:
        levelname = record.levelname
        color = self.COLORS.get(levelname, self.RESET)
        reset = self.RESET if color != self.RESET else ""

        time_str = datetime.fromtimestamp(record.created).strftime("%H:%M:%S")
        message = f"[{time_str}] {color}{levelname:8}{reset} {record.getMessage()}"

        context = getattr(_thread_context, "context", {})
        if context:
            ctx_str = " ".join(f"{key}={value}" for key, value in context.items())
            message = f"{message} {self.RESET}[{ctx_str}]"

        if record.exc_info:
            message += "\n" + self.formatException(record.exc_info)

        return message


class FileFormatter(logging.Formatter):
    """Plain formatter for file output without ANSI color codes."""

    def format(self, record: logging.LogRecord) -> str:
        time_str = datetime.fromtimestamp(record.created).strftime("%Y-%m-%d %H:%M:%S")
        message = f"[{time_str}] {record.levelname:8} {record.getMessage()}"

        context = getattr(_thread_context, "context", {})
        if context:
            ctx_str = " ".join(f"{key}={value}" for key, value in context.items())
            message = f"{message} [{ctx_str}]"

        if record.exc_info:
            message += "\n" + self.formatException(record.exc_info)

        return message


def _runtime_base_dir() -> Path:
    """Return a stable writable directory for runtime artifacts."""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(os.getcwd()).resolve()


def _is_pyinstaller_temp_path(path: Path) -> bool:
    """Detect PyInstaller temporary extraction paths."""
    path_str = str(path)
    if "_MEI" in path_str:
        return True

    meipass = getattr(sys, "_MEIPASS", None)
    if meipass:
        try:
            meipass_path = Path(meipass).resolve()
            if meipass_path == path or meipass_path in path.parents:
                return True
        except Exception:
            return True

    temp_dir = Path(tempfile.gettempdir()).resolve()
    if getattr(sys, "frozen", False):
        try:
            if temp_dir == path or temp_dir in path.parents:
                return True
        except Exception:
            return True

    return False


def _resolve_log_dir(log_dir: Optional[str] = None) -> Path:
    """
    Resolve a safe log directory for both source and PyInstaller execution.

    If the provided path points into the PyInstaller temp extraction directory,
    redirect logs to the current runtime folder.
    """
    runtime_base = _runtime_base_dir()
    if log_dir:
        candidate = Path(log_dir)
    else:
        try:
            from obcash3.config.settings import LOGS_DIR

            candidate = Path(LOGS_DIR)
        except Exception:
            candidate = Path("logs")

    if not candidate.is_absolute():
        candidate = runtime_base / candidate

    candidate = candidate.resolve(strict=False)

    if _is_pyinstaller_temp_path(candidate):
        return runtime_base / "logs"

    return candidate


def setup_logging(
    log_dir: Optional[str] = None,
    console_level: str = "INFO",
    file_level: str = "DEBUG",
    json_format: bool = False,
    max_bytes: int = 10 * 1024 * 1024,
    backup_count: int = 3,
) -> logging.Logger:
    """
    Configure the logging system.

    Logs are always written to a stable runtime directory, never to the
    temporary _MEI folder used by PyInstaller one-file executables.
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
        try:
            handler.close()
        except Exception:
            pass

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, console_level.upper(), logging.INFO))
    console_handler.setFormatter(SimpleFormatter())
    root_logger.addHandler(console_handler)

    try:
        log_path = _resolve_log_dir(log_dir)
        log_path.mkdir(parents=True, exist_ok=True)

        log_file = log_path / "obcash3.log"
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )
        file_handler.setLevel(getattr(logging, file_level.upper(), logging.DEBUG))
        file_handler.setFormatter(JsonFormatter() if json_format else FileFormatter())
        root_logger.addHandler(file_handler)

        error_file = log_path / "errors.log"
        error_handler = RotatingFileHandler(
            error_file,
            maxBytes=max_bytes // 2,
            backupCount=backup_count,
            encoding="utf-8",
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(JsonFormatter())
        root_logger.addHandler(error_handler)

        root_logger.info(
            "Logging initialized",
            extra={
                "log_dir": str(log_path),
                "console_level": console_level,
                "file_level": file_level,
                "frozen": bool(getattr(sys, "frozen", False)),
            },
        )
    except Exception as exc:
        print(f"Erro ao configurar logs: {exc}")
        root_logger.exception("Failed to configure file logging")

    return root_logger


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Get a logger instance."""
    return logging.getLogger(name) if name else logging.getLogger()


def set_context(**kwargs: Any) -> None:
    """Set thread-local context for logging."""
    if not hasattr(_thread_context, "context"):
        _thread_context.context = {}
    _thread_context.context.update(kwargs)


def clear_context() -> None:
    """Clear thread-local logging context."""
    if hasattr(_thread_context, "context"):
        del _thread_context.context
