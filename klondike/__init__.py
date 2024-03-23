import importlib
import logging
import os

# Define project logger
logger = logging.getLogger(__name__)

_handler = logging.StreamHandler()
_formatter = logging.Formatter("%(module)s %(levelname)s %(message)s")
_handler.setFormatter(_formatter)

logger.addHandler(_handler)

# Define logging behavior
if any([os.environ.get("TESTING"), os.environ.get("TEST")]):
    logger.setLevel("WARNING")
elif os.environ.get("DEBUG"):
    logger.setLevel("DEBUG")
    logger.debug("Debugger active...")
else:
    logger.setLevel("INFO")

#####

__all__ = []
POLAR_OBJECTS = [
    ("klondike.bigquery.bigquery", "BigQueryConnector"),
]

for module_, object_ in POLAR_OBJECTS:
    try:
        globals()[object_] = getattr(importlib.import_module(module_), object_)
    except ImportError:
        logger.debug(f"Could not import {module_}.{object_} ... skipping")
