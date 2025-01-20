from contextlib import contextmanager
from typing import Optional, Union

import polars as pl
import psycopg2

from klondike import logger
from klondike.base.abc_klondike import KlondikeBaseDBConnector

##########


class PostgresConnector(KlondikeBaseDBConnector):
    def __init__(
        self,
        host: str,
        database: str,
        username: str,
        password: str,
        port: Optional[int] = 5432,
    ):
        self.host = host
        self.database = database
        self.port = port

        self.username = username
        self.password = password

        self.__dialect = "postgres"

    @property
    def dialect(self):
        return self.__dialect

    @contextmanager
    def connection(self):
        connection_ = psycopg2.connect(
            host=self.host,
            database=self.database,
            port=self.port,
            username=self.username,
            password=self.password,
        )

        try:
            yield connection_
            connection_.commit()
        finally:
            connection_.close()

    @contextmanager
    def cursor(self, connection: psycopg2.connect):
        cursor_ = connection.cursor()

        try:
            yield cursor_
        finally:
            cursor_.close()

    def __split_schema_and_table(self, table_name: str) -> tuple:
        schema, table = table_name.split(".")

        return schema, table

    def __combine_schema_and_table(self, schema_name: str, table_name: str) -> str:
        return f"{schema_name}.{table_name}"

    def query(
        self,
        sql: str,
        timeout: int = 90,
        return_results: bool = True,
        parameters: Optional[list] = None,
    ) -> Union[pl.DataFrame, None]:
        """
        Execute a query against the Postgres database
        """

        with self.connection() as conn_:
            with self.cursor(connection=conn_) as cursor_:
                cursor_.execute(sql, parameters)

                if not cursor_.description and return_results:
                    logger.warning("Query returned no results")
                    return pl.DataFrame()

                elif not return_results:
                    return

                headers = [x[0] for x in cursor_.description]
                records = [headers] + cursor_.fetchall()

                return pl.from_records(records)

    def table_exists(self, table_name: str) -> bool:
        """
        Determine if a table or view exists in Postgres
        """

        query = """
        SELECT EXISTS (
            SELECT
                1
            FROM information_schema.tables
            WHERE table_schema = %s
                AND table_name = %s
        )
        """

        schema, table = self.__split_schema_and_table(table_name=table_name)

        resp = self.query(sql=query, parameters=[schema, table])

        return resp

    def read_dataframe(self, sql: str) -> pl.DataFrame:
        """
        Read data from Postgres into Polars DataFrame
        """

        resp = self.query(sql=sql, return_results=True)

        return resp

    def write_dataframe(
        self,
        df: pl.DataFrame,
        schema_name: str,
        table_name: str,
        if_exists: Optional[str] = "append",
        index: Optional[bool] = False,
    ) -> None:
        """
        Write a Polars DataFrame as a table in Postgres
        """

        table_name = self.__combine_schema_and_table(
            schema_name=schema_name, table_name=table_name
        )

        if if_exists == "truncate":
            self.query(f"TRUNCATE TABLE {table_name};", return_results=False)
            if_exists = "append"

        with self.connection() as connection_:
            temp_df = df.to_pandas()
            temp_df.to_sql(
                table_name,
                con=connection_,
                if_exists=if_exists,
                index=index,
            )
