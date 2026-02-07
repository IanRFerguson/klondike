"""
General utilities to use across all database connections
"""

import os
from typing import Optional

IF_EXISTS_BEHAVIOR = ["fail", "append", "truncate", "drop"]


def validate_if_exists_behavior(
    user_input: str, acceptable_values: list = IF_EXISTS_BEHAVIOR
) -> bool:
    """
    Ensures that user input to `write_dataframe` function is valid

    Args:
        user_input: User-provided if_exists behavior
        acceptable_values: List of acceptable values

    Returns:
        True if valid, False otherwise
    """
    return user_input in acceptable_values


def get_env_or_value(value: Optional[str], env_key: str) -> Optional[str]:
    """
    Return provided value or fall back to environment variable.

    Args:
        value: Explicitly provided value (takes precedence)
        env_key: Environment variable name to check if value is None

    Returns:
        The provided value if not None, otherwise the environment variable value
    """
    return value if value is not None else os.getenv(env_key)
