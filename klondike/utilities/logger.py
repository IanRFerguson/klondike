import logging
import os
import sys

from colorlogger import ColoredFormatter

logger = logging.getLogger(__name__)
_handler = logging.StreamHandler(sys.stdout)
_formatter = ColoredFormatter(
    "%(log_color)s%(levelname)s%(reset)s %(message)s",
    reset=True,
    log_colors={
        "DEBUG": "cyan",
        "INFO": "green",
        "WARNING": "yellow",
        "ERROR": "red",
        "CRITICAL": "red,bg_white",
    },
    style="%",
)

_handler.setFormatter(_formatter)
logger.addHandler(_handler)
logger.setLevel("INFO")

if os.environ.get("DEBUG") == "true":
    logger.setLevel("DEBUG")
    logger.debug("** Debugger Active **")
