from klondike import logger
from typing import Optional, Union
import os
from contextlib import contextmanager
import polars as pl

##########


class PostgresConnector:
    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        default_env_prefix: str = "POSTGRES",
        dialect: str = "postgresql",
    ):
        self.__default_env_prefix = default_env_prefix
        self.dialect = dialect

        self.host = self.__safe_get_env_variable(value=host, variable="host")
        self.port = self.__safe_get_env_variable(value=port, variable="port")
        self.database = self.__safe_get_env_variable(
            value=database, variable="database"
        )
        self.username = self.__safe_get_env_variable(
            value=username, variable="username"
        )
        self.password = self.__safe_get_env_variable(
            value=password, variable="password"
        )

    @property
    def uri(self):
        _uri = "{}://{}:{}@{}:{}/{}".format(
            self.dialect,
            self.username,
            self.password,
            self.host,
            self.port,
            self.database,
        )

        return _uri

    def __safe_get_env_variable(self, value: str, variable: str) -> Union[str | None]:
        if value:
            return value

        ENV_TARGET = f"{self.__default_env_prefix}_{variable.upper()}"
        if os.environ.get(ENV_TARGET):
            logger.debug(f"Setting variable from environment [key={ENV_TARGET}]")
            return os.environ[ENV_TARGET]

        raise OSError(
            f"Missing {variable} variable in constructor and no `{ENV_TARGET}` value set"
        )

    def __query(self, sql: str):
        """
        Execute SQL command against Postgres database
        """

        with self.connection() as _conn:
            with self.cursor(_conn) as _cursor:
                _cursor.execute(sql)

    def read_dataframe(self, sql: str) -> pl.DataFrame:
        logger.debug("Running SQL...", sql)
        df = pl.read_database_uri(query=sql, uri=self.uri)

        logger.info(f"Successfully read {len(df)} rows from Postgres")

        return df

    def write_dataframe(
        self,
        df: pl.DataFrame,
        schema_name: str,
        table_name: str,
        if_exists: str = "append",
    ):
        """
        Writes a Polars DataFrame to Postgres

        Args:
            df: `pl.DataFrame`
                Polars DataFrame to be written to Postgres
            schema_name:  `str`
                Destination schema name
            table_name: `str`
                Destination table name
            if_exists:  `str`
                One of `drop` `append` or `truncate`
        """

        table = f"{schema_name}.{table_name}"
        logger.info()
        df.write_database(
            table_name=table, connection=self.uri, if_table_exists=if_exists
        )

    def table_exists(self):
        pass

    def list_tables(self):
        pass
