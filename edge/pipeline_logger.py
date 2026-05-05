from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any, Mapping

LOGGER_NAME = "edge"


def configure_pipeline_logging(
    config: Mapping[str, Any] | None = None,
    *,
    terminal_output: bool | None = None,
) -> logging.Logger:
    """Configure the shared edge pipeline logger."""
    logging_config = dict(config or {})
    level = _coerce_level(str(logging_config.get("level", "INFO")))
    file_enabled = bool(logging_config.get("file_enabled", True))
    file_path = str(logging_config.get("file_path", "logs/edge_pipeline.log"))
    terminal_enabled = (
        bool(logging_config.get("terminal_output", True))
        if terminal_output is None
        else terminal_output
    )

    logger = logging.getLogger(LOGGER_NAME)
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
        handler.close()
    logger.setLevel(level)
    logger.propagate = False

    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if file_enabled:
        log_path = Path(file_path)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setFormatter(formatter)
        file_handler.setLevel(level)
        logger.addHandler(file_handler)

    if terminal_enabled:
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)
        stream_handler.setLevel(level)
        logger.addHandler(stream_handler)

    if not logger.handlers:
        logger.addHandler(logging.NullHandler())

    return logger


def get_pipeline_logger(name: str | None = None) -> logging.Logger:
    """Return a child logger for an edge pipeline module."""
    if not name:
        return logging.getLogger(LOGGER_NAME)
    return logging.getLogger(f"{LOGGER_NAME}.{name}")


def log_event(
    logger: logging.Logger,
    event: str,
    message: str = "",
    **fields: object,
) -> None:
    """Log a compact event line with optional key/value fields."""
    field_text = " ".join(f"{key}={value}" for key, value in fields.items())
    if message and field_text:
        logger.info("event=%s %s %s", event, message, field_text)
    elif message:
        logger.info("event=%s %s", event, message)
    elif field_text:
        logger.info("event=%s %s", event, field_text)
    else:
        logger.info("event=%s", event)


def _coerce_level(value: str) -> int:
    level_name = value.upper()
    level = logging.getLevelName(level_name)
    if isinstance(level, int):
        return level
    return logging.INFO
