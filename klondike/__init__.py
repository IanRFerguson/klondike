import importlib

from klondike.utilities.logger import logger

__all__ = []
POLAR_OBJECTS = [
    ("klondike.gcp.bigquery", "BigQueryConnector"),
    ("klondike.gcp.cloud_storage", "CloudStorageConnector"),
    ("klondike.snowflake.snowflake", "SnowflakeConnector"),
]

for module_, object_ in POLAR_OBJECTS:
    try:
        globals()[object_] = getattr(importlib.import_module(module_), object_)
    except ImportError:
        logger.warning(
            f"Could not import {object_} from {module_}. It will not be available."
        )
