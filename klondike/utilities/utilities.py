"""
General utilites to use across all database connections
"""


def validate_if_exists_behavior(
    user_input: str, acceptable_values: list = ["fail", "append", "truncate", "drop"]
):
    """
    Ensures that user input to `write_dataframe` function is valid
    """

    return user_input in acceptable_values
