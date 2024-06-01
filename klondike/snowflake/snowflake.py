import os
from contextlib import contextmanager

import polars as pl

import snowflake.connector as snow

##########


class SnowflakeConnector:
    """
    TODO
    """

    def __init__(
        self,
        snowflake_account: str = os.getenv("SNOWFLAKE_ACCOUNT"),
        snowflake_user: str = os.getenv("SNOWFLAKE_USER"),
        snowflake_password: str = os.getenv("SNOWFLAKE_PASSWORD"),
        row_chunk_size: int = 100_000,
    ):
        self.snowflake_account = snowflake_account
        self.snowflake_user = snowflake_user
        self.snowflake_password = snowflake_password
        self.row_chunk_size = row_chunk_size

    @contextmanager
    def connection(self):
        """
        TODO
        """

        conn = snow.connect(
            account=self.snowflake_account,
            user=self.snowflake_user,
            password=self.snowflake_password,
        )

        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    @contextmanager
    def cursor(self, connection):
        """
        TODO
        """

        _cur_ = connection.cursor()

        try:
            yield _cur_
        finally:
            _cur_.close()

    def __query(self, sql: str) -> pl.DataFrame:
        """
        TODO
        """

        with self.connection() as _conn:
            with self.cursor(_conn) as _cursor:
                _cursor.execute(sql)
                stack = []

                while True:
                    data = _cursor.fetchmany(self.row_chunk_size)
                    if not data:
                        break

                    stack.append(data)

                return pl.concat(stack)

    def read_dataframe_from_bigquery(self, sql: str) -> pl.DataFrame:
        """
        TODO
        """

        pass

    def write_dataframe_to_bigquery(self) -> None:
        """
        TODO
        """

        pass
