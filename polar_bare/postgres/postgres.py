import os
from contextlib import contextmanager
from typing import Union

import psycopg

from polar_bare.pbear.generic_db import PolarBareDB

##########


class PolarPostgres(PolarBareDB):
    """
    Establish and authenticate a connection to a Postgres instance.

    Args:
        host: Required if `PG_HOST` absent in runtime environment
        db: Required if `PG_DBNAME` absent in runtime environment
        port: Required if `PG_PORT` absent in runtime environment
        username: Required if `PG_USERNAME` absent in runtime environment
        password: Required if `PG_PASSWORD` absent in runtime environment
        timeout: Defines connection timeout tolerance (default=60s)
    """

    def __init__(
        self,
        host: str,
        db: str,
        port: Union[str, int],
        username: str,
        password: str,
        timeout: int = 60,
    ):
        super().__init__()

        self.host = host or os.environ["PG_HOST"]
        self.db = db or os.environ["PG_DBNAME"]
        self.port = port or os.environ["PG_PORT"]
        self.timeout = timeout

        self.__username = username or os.environ["PG_USERNAME"]
        self.__password = password or os.environ["PG_PASSWORD"]

        self.dialect = "postgres"

    @contextmanager
    def connection(self):
        """
        Establishes a Postgres connection

        Example usage...
        ```
        with self.connection() as conn_:
            # Hit the db in scope
        ```
        """

        connection = psycopg.connect(
            user=self.__username,
            password=self.__password,
            host=self.host,
            port=self.port,
            dbname=self.db,
            connect_timeout=self.timeout,
        )

        try:
            yield connection
        except psycopg.Error:
            connection.rollback()
            raise
        else:
            connection.commit()
        finally:
            connection.close()
