import os
from contextlib import contextmanager
from typing import Optional

import polars as pl
import snowflake.connector as sf
from snowflake.connector.pandas_tools import write_pandas

from klondike import logger
from klondike.base.abc_klondike import KlondikeBaseDBConnector
from klondike.utilities.utilities import validate_if_exists_behavior

##########


class SnowflakeConnector(KlondikeBaseDBConnector):
    """
    Leverages connection to Snowflake to read and write Polars DataFrame
    objects to the data warehouse

    Args:
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
        snowflake_user: Optional[str] = None,
        snowflake_password: Optional[str] = None,
        snowflake_account: Optional[str] = None,
        snowflake_database: Optional[str] = None,
        snowflake_warehouse: Optional[str] = None,
        row_chunk_size: int = 100_000,
    ):
        """
        All Snowflake connection values either need to be supplied as constructor
        arguments or inferred from the environment; if neither occurs, a `ValueError`
        will be raised
        """

        self.snowflake_user = (
            snowflake_user if snowflake_user else os.getenv("SNOWFLAKE_USER")
        )
        self.snowflake_password = (
            snowflake_password
            if snowflake_password
            else os.getenv("SNOWFLAKE_PASSWORD")
        )
        self.snowflake_account = (
            snowflake_account if snowflake_account else os.getenv("SNOWFLAKE_ACCOUNT")
        )
        self.__snowflake_warehouse = (
            snowflake_warehouse
            if snowflake_warehouse
            else os.getenv("SNOWFLAKE_WAREHOUSE")
        )
        self.__snowflake_database = (
            snowflake_database
            if snowflake_database
            else os.getenv("SNOWFLAKE_DATABASE")
        )

        ###

        self.__validate_authentication()

        ###

        self.__dialect = "snowflake"
        self.row_chunk_size = row_chunk_size

    def __validate_authentication(self):
        _auth_vals = [
            self.snowflake_user,
            self.snowflake_password,
            self.snowflake_account,
            self.snowflake_database,
            self.snowflake_warehouse,
        ]

        if any([not x for x in _auth_vals]):
            raise ValueError(
                "Missing authentication values! Make sure all `snowflake_*` values are provided at construction"
            )

    @property
    def dialect(self):
        return self.__dialect

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

        conn = sf.connect(
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

    def query(self, sql: str):
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

        try:
            return pl.from_arrow(_resp)
        except ValueError as ve:
            # NOTE - This appears to be Polars response to empty fetch_arrow_batches()
            # This should be interrogated more, but is functional
            if "Must pass schema, or at least one RecordBatch" in str(ve):
                logger.debug("No results obtained via query")
                return pl.DataFrame()
            raise

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
        df = self.query(sql=sql)

        logger.info(f"Successfully read {len(df)} rows from Snowflake")

        return df

    def write_dataframe(
        self,
        df: pl.DataFrame,
        table_name: str,
        schema_name: str,
        database_name: Optional[str] = None,
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
            database: `str`
                Optional Snowflake database (defaults to preset class attribute)
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
                raise sf.errors.DatabaseError(f"{table_name} already exists")

        database = database_name if database_name else self.snowflake_database

        ###

        logger.info(
            f"Writing to {self.snowflake_database}.{schema_name}.{table_name}..."
        )
        with self.connection() as conn:
            resp, num_chunks, num_rows, output = write_pandas(
                conn=conn,
                df=df.to_pandas(),
                database=database,
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

    def table_exists(
        self, table_name: str, database_name: Optional[str] = None
    ) -> bool:
        """
        Determines if a Snowflake table exists in the warehouse

        Args:
            table_name: `str`
                Snowflake table name in `schema.table` format

        Returns:
            True if the table exists, False otherwise
        """

        schema, table = table_name.split(".")

        if not database_name:
            logger.debug(f"Defaulting to default database [{self.snowflake_database}]")
            database_name = self.snowflake_database

        sql = f"""
        SELECT
            *
        FROM INFORMATION_SCHEMA.TABLES
        WHERE table_schema = '{schema}'
            AND table_name = '{table}'
            AND table_catalog = '{database_name}'
        """

        resp = self.query(sql=sql)

        return not resp.is_empty()

    def list_tables(
        self, schema_name: str, database_name: Optional[str] = None
    ) -> list:
        """
        Gets a list of available tables in a Snowflake schema

        Args:
            schema_name: `str`
            database_name: `str`

        Returns:
            List of table names
        """

        if not database_name:
            database_name = self.snowflake_database

        sql = f"""
        SELECT
            table_catalog,
            table_schema,
            table_name
        FROM INFORMATION_SCHEMA.TABLES
        WHERE table_schema = '{schema_name}'
            AND table_catalog = '{database_name}'
        """

        resp = self.query(sql=sql)

        resp = resp.select(table_name=pl.concat_str(resp.columns, separator="."))

        return pl.Series(resp["table_name"]).to_list()
