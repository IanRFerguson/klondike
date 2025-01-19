"""
General utilites to use across all database connections
"""

IF_EXISTS_BEHAVIOR = ["fail", "append", "truncate", "drop"]


def validate_if_exists_behavior(user_input: str, acceptable_values: list = IF_EXISTS_BEHAVIOR):
    """
    Ensures that user input to `write_dataframe` function is valid
    """

    return user_input in acceptable_values
