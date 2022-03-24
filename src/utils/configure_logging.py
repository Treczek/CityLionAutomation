import logging
import logging.config
import sys

import structlog

LOGGING_CONFIG = dict(
    version=1,
    disable_existing_loggers=True,
    loggers={
        "": {"level": "INFO", "handlers": ["console"]},
    },
    handlers={
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "standard",
            "stream": sys.stdout,
        },
        "error_console": {
            "class": "logging.StreamHandler",
            "formatter": "error",
            "stream": sys.stderr,
        },
    },
    formatters={
        "standard": {
            "format": "[%(asctime)s] [%(levelname)s] %(name)15s - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
        "error": {
            "format": "[%(asctime)s] [%(levelname)s] %(name)15s:%(module)s:%(lineno)s - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
        "structlog": {
            "format": "[%(asctime)s] [STRUCTLOG: %(levelname)s] - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
    },
)


def setup_logging(verbosity):
    level = max(logging.ERROR - verbosity * 10, logging.DEBUG)
    LOGGING_CONFIG["loggers"][""]["level"] = level
    logging.config.dictConfig(LOGGING_CONFIG)
    structlog.configure(
        processors=[
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.processors.KeyValueRenderer(
                key_order=["event", "logger", "level"]
            ),
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
    )
