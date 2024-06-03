import os
from contextlib import contextmanager

import polars as pl
import snowflake.connector as snow
from snowflake.connector.pandas_tools import write_pandas

from klondike import logger
from klondike.utilities.utilities import validate_if_exists_behavior

##########


class SnowflakeConnector:
    """
    Leverages connection to Snowflake to read and write Polars DataFrame
    objects to the data warehouse

    `Args`:
        snowflake_user: `str`
            Username to connect to Snowflake (defaults to `SNOWFLAKE_USER` in environment)
        snowflake_password: `str`
            Password to connect to Snowflake (defaults to `SNOWFLAKE_PASSWORD` in environment)
        snowflake_account: `str`
            Account identifier for Snowflake warehouse (defaults to `SNOWFLAKE_ACCOUNT` in environment)
        snowflake_warehouse: `str`
            Snowflake warehouse name (defaults to `SNOWFLAKE_WAREHOUSE` in environment)
        snowflake_database: `str`
            Snowflake database name (defaults to `SNOWFLAKE_DATABASE` in environment)
        row_chunk_size: `int`
            Default row chunk size for reading from / writing to Snowflake
    """

    def __init__(
        self,
        snowflake_user: str = os.getenv("SNOWFLAKE_USER"),
        snowflake_password: str = os.getenv("SNOWFLAKE_PASSWORD"),
        snowflake_account: str = os.getenv("SNOWFLAKE_ACCOUNT"),
        snowflake_database: str = os.getenv("SNOWFLAKE_DATABASE"),
        snowflake_warehouse: str = os.getenv("SNOWFLAKE_WAREHOUSE"),
        row_chunk_size: int = 100_000,
    ):
        self.snowflake_user = snowflake_user
        self.snowflake_password = snowflake_password
        self.snowflake_account = snowflake_account
        self.__snowflake_warehouse = snowflake_warehouse
        self.__snowflake_database = snowflake_database

        self.dialect = "snowflake"
        self.__row_chunk_size = row_chunk_size

    @property
    def snowflake_warehouse(self):
        return self.__snowflake_warehouse

    @snowflake_warehouse.setter
    def snowflake_warehouse(self, warehouse):
        self.__snowflake_warehouse = warehouse

    @property
    def snowflake_database(self):
        return self.__snowflake_database

    @snowflake_database.setter
    def snowflake_database(self, database):
        self.__snowflake_database = database

    @property
    def row_chunk_size(self):
        return self.__row_chunk_size

    @row_chunk_size.setter
    def row_chunk_size(self, row_chunk_size):
        self.__row_chunk_size = row_chunk_size

    @contextmanager
    def connection(self):
        """
        Creates a connection to Snowflake
        """

        conn = snow.connect(
            account=self.snowflake_account,
            warehouse=self.snowflake_warehouse,
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
        Leverages Snowflake connection to execute SQL transactions
        """

        cur = connection.cursor()

        try:
            yield cur
        finally:
            cur.close()

    def __query(self, sql: str):
        """
        Executes SQL command against Snowflake warehouse

        Args:
            sql: `str`
                SQL query in string format
        """

        with self.connection() as _conn:
            with self.cursor(_conn) as _cursor:
                # TODO - Ugly! Clean this up
                _cursor.execute(f"USE DATABASE {self.snowflake_database};")
                _cursor.execute(f"USE WAREHOUSE {self.snowflake_warehouse};")
                _cursor.execute(sql)
                _resp = _cursor.fetch_arrow_batches()

        return pl.from_arrow(_resp)

    def table_exists(self, schema_name: str, table_name: str) -> bool:
        """
        Determines if a Snowflake table exists in the warehouse

        Args:
            schema_name: `str`
                Target schema name
            table_name: `str`
                Target table name

        Returns:
            True if the table exists, False otherwise
        """

        sql = f"""
        SELECT
            *
        FROM INFORMATION_SCHEMA.TABLES
        WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'
        """

        resp = self.__query(sql=sql)

        return not resp.is_empty()

    def read_dataframe(self, sql: str) -> pl.DataFrame:
        """
        Executes a SQL query against the Snowflake warehouse
        and returns the result as a Polars DataFrame object

        Args:
            sql: `str`
                String representation of SQL query

        Returns:
            Polars DataFrame object
        """

        # Execute SQL against warehouse
        logger.debug("Running SQL...", sql)
        df = self.__query(sql=sql)

        logger.info(f"Successfully read {len(df)} rows from Snowflake")

        return df

    def write_dataframe(
        self,
        df: pl.DataFrame,
        table_name: str,
        schema_name: str,
        if_exists: str = "append",
        auto_create_table: bool = True,
        chunk_output: bool = False,
    ) -> None:
        """
        Writes a Polars DataFrame to Snowflake

        TODO - Utilizing `.to_pandas()` may not be ideal, we should consider
        modifying this function to utilize `PyArrow` under the hood (see - `klondike.BigQueryConnector`)

        Args:
            df: `pl.DataFrame`
                Polars DataFrame to be written to Snowflake
            table_name: `str`
                Destination table name
            database_name: `str`
                Destination database name
            schema_name: `str`
                Destination schema name
            if_exists: `str`
                One of `append`, `truncate`, `drop`, `fail`
            auto_create_table: `bool`
                If true, the desintation table will be created if it doesn't already exist
            chunk_output: `int`
                If true, the default chunk size will be applied; otherwise no chunking will occur
        """

        # Use class attribute  for row chunking
        row_chunks = self.row_chunk_size if chunk_output else None

        # Confirm that user has passed in valid logic
        if not validate_if_exists_behavior(if_exists):
            raise ValueError(f"{if_exists} is an invalid input")

        # This logic leverages the behavior of the write_pandas() function ... see below
        if if_exists == "drop" and auto_create_table:
            overwrite = True
            logger.warning(f"{table_name} will be dropped if it exists")

        elif if_exists == "truncate":
            overwrite = True
            auto_create_table = False

        elif if_exists == "append":
            overwrite = False
            auto_create_table = False

        elif if_exists == "fail":
            if self.table_exists(schema_name=schema_name, table_name=table_name):
                raise snow.errors.DatabaseError(f"{table_name} already exists")

        ###

        logger.info(
            f"Writing to {self.snowflake_database}.{schema_name}.{table_name}..."
        )
        with self.connection() as conn:
            resp, num_chunks, num_rows, output = write_pandas(
                conn=conn,
                df=df.to_pandas(),
                database=self.snowflake_database,
                schema=schema_name,
                table_name=table_name,
                auto_create_table=auto_create_table,
                chunk_size=row_chunks,
                overwrite=overwrite,
            )

        ###

        if resp:
            logger.info(f"Successfully wrote {num_rows} rows to {table_name}")
        else:
            logger.error(f"Failed to write to {table_name}", resp)
            raise
