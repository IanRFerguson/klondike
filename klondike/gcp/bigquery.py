import io
import json
import os
import tempfile
from typing import Optional, Union

import polars as pl
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig
from google.cloud.exceptions import NotFound

from klondike import logger
from klondike.base.abc_klondike import KlondikeBaseDBConnector
from klondike.utilities.utilities import validate_if_exists_behavior

##########

SCOPES = (
    {
        "scopes": [
            "https://www.googleapis.com/auth/bigquery",
        ]
    },
)[0]


class BigQueryConnector(KlondikeBaseDBConnector):
    """
    Establish and authenticate a connection to a BigQuery warehouse

    Args:
        app_creds: `str`
            Google service account, either as a relative path or a dictionary instance
        project: `str`
            Name of Google Project
        location: `str`
            Location of Google Project
        timeout: `int`
            Temporal threshold to kill a stalled job, defaults to 60s
        client_options: `list`
            API scopes
        google_environment_variable: `str`
            Provided for flexibility, defaults to `GOOGLE_APPLICATION_CREDENTIALS`
        bypass_env_variable: `bool`
            If True, no exception will raise when the `GOOGLE_APPLICATION_CREDENTIALS` env
            variable is missing. This is helpful when running in a GCP environment that doesn't
            explicitly set the environment this way
    """

    def __init__(
        self,
        app_creds: Optional[Union[str, dict]] = None,
        project: Optional[str] = None,
        location: Optional[str] = None,
        timeout: int = 60,
        client_options: dict = SCOPES,
        google_environment_variable: str = "GOOGLE_APPLICATION_CREDENTIALS",
        bypass_env_variable: bool = False,
    ):
        self.app_creds = app_creds
        self.project = project
        self.location = location
        self.client_options = client_options

        self.__dialect = "bigquery"
        self.__client = None
        self.__timeout = timeout

        if not self.app_creds:
            if (
                not os.environ.get(google_environment_variable)
                and not bypass_env_variable
            ):
                raise OSError("No app_creds provided")

            elif bypass_env_variable:
                logger.info(
                    f"Bypassing env variable requirements `{google_environment_variable}` not found in environment"
                )

            else:
                logger.info(
                    f"Using `{google_environment_variable}` variable defined in environment"
                )

        else:
            self.__setup_google_app_creds(
                app_creds=self.app_creds, env_variable=google_environment_variable
            )

    @property
    def dialect(self):
        return self.__dialect

    @property
    def client(self):
        """
        Instantiate BigQuery client and assign it
        as class property
        """

        if not self.__client:
            self.__client = bigquery.Client(
                project=self.project,
                location=self.location,
                client_options=self.client_options,
            )

        return self.__client

    @property
    def timeout(self):
        return self.__timeout

    @timeout.setter
    def timeout(self, timeout):
        self.__timeout = timeout

    def __setup_google_app_creds(self, app_creds: Union[str, dict], env_variable: str):
        "Sets runtime environment variable for Google SDK"

        if isinstance(app_creds, dict):
            creds = json.dumps(app_creds)
            tempp_file = tempfile.TemporaryFile(suffix=".json")

            with open(tempp_file.name, "w") as f:
                f.write(creds)

        elif isinstance(app_creds, str):
            creds = app_creds

        os.environ[env_variable] = creds

    def __set_load_job_config(
        self,
        base_job_config: Optional[LoadJobConfig] = None,
        max_bad_records: int = 0,
        table_schema: list = None,
        if_exists: str = "fail",
        **kwargs,
    ):
        "Defines `LoadConfigJob` when writing to BigQuery"

        def set_write_disposition(if_exists: str):
            DISPOSITION_MAP = {
                "fail": bigquery.WriteDisposition.WRITE_EMPTY,
                "append": bigquery.WriteDisposition.WRITE_APPEND,
                "truncate": bigquery.WriteDisposition.WRITE_TRUNCATE,
            }

            return DISPOSITION_MAP[if_exists]

        def set_table_schema(table_schema: list):
            return [bigquery.SchemaField(**x) for x in table_schema]

        ###

        if not base_job_config:
            logger.debug("No job config provided, starting fresh")
            base_job_config = LoadJobConfig()

        # This is default behavior for this class
        base_job_config.source_format = bigquery.SourceFormat.PARQUET

        # Create table schema mapping if provided
        if table_schema:
            base_job_config.schema = set_table_schema(table_schema=table_schema)
        else:
            base_job_config.schema = None

        base_job_config.max_bad_records = max_bad_records
        base_job_config.write_disposition = set_write_disposition(if_exists=if_exists)

        ###

        # List of available LoadJobConfig attributes
        _attributes = [x for x in dict(vars(LoadJobConfig)).keys()]

        # Attributes that will not be overwritten
        _reserved_attributes = [
            "source_format",
            "schema",
            "max_bad_records",
            "write_disposition",
        ]

        # Loop through keyword arguments and update `LoadJobConfig` object
        for k, v in kwargs.items():
            if k in _attributes and k not in _reserved_attributes:
                logger.debug(f"Updating {k} parameter in job config [value={v}]")
                base_job_config[k] = v

        return base_job_config

    def read_dataframe(self, sql: str) -> pl.DataFrame:
        """
        Reads a Polars DataFrame based on a SQL query

        Args:
            sql: `str`
                String representation of SQL query

        Returns:
            Polars DataFrame object
        """

        df = self.query(sql=sql, return_results=True)

        return df

    def write_dataframe(
        self,
        df: pl.DataFrame,
        table_name: str,
        load_job_config: Optional[LoadJobConfig] = None,
        max_bad_records: int = 0,
        table_schema: list = None,
        if_exists: str = "fail",
        **load_kwargs,
    ) -> None:
        """
        Writes a Polars DataFrame to BigQuery

        Args:
            df: `polars.DataFrame`
                DataFrame to write to BigQuery
            table_name: `str`
                Destination table name to write to - `dataset.table` convention
            load_job_config: `LoadJobConfig`
                Configures load job; if none is supplied, several defaults are applied
            max_bad_records: `int`
                Tolerance for bad records in the load job, defaults to 0
            table_schema: `list`
                List of column names, types, and optional flags to include
            if_exists: `str`
                One of `fail`, `drop`, `append`, `truncate`
            load_kwargs:
                See here for list of accepted values \
                    https://cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.job.LoadJobConfig
        """

        if not validate_if_exists_behavior(user_input=if_exists):
            raise ValueError(f"{if_exists} is an invalid input")

        if if_exists == "drop":
            self.client.delete_table(table=table_name)
            if_exists = "fail"

        load_job_config = self.__set_load_job_config(
            base_load_conifg=load_job_config,
            max_bad_records=max_bad_records,
            table_schema=table_schema,
            if_exists=if_exists,
            **load_kwargs,
        )

        logger.info(f"Writing to {table_name}...")
        with io.BytesIO() as stream_:
            df.write_parquet(stream_)
            stream_.seek(0)
            load_job = self.client.load_table_from_file(
                stream_, destination=table_name, job_config=load_job_config
            )

        load_job.result()
        logger.info(f"Successfuly wrote {len(df)} rows to {table_name}")

    def table_exists(self, table_name: str) -> bool:
        """
        Determines if a BigQuery table exists

        Args:
            table_name: `str`
                BigQuery table name in `schema.table` or `project.schema.table` format
        """

        try:
            _ = self.client.get_table(table=table_name)
            return True

        except NotFound:
            return False

    def list_tables(self, schema_name: str) -> list:
        """
        Gets a list of available tables in a BigQuery schema

        Args:
            schema_name: `str`
                BigQuery schema name

        Returns:
            List of table names
        """

        return [
            x.full_table_id.replace(":", ".")
            for x in self.client.list_tables(dataset=schema_name)
        ]

    def query(
        self, sql: str, timeout: Optional[int] = None, return_results: bool = True
    ) -> pl.DataFrame:
        """
        Executes a SQL query and returns a Polars DataFrame.

        TODO - Make this more flexible and incorporate query params

        Args:
            sql: `str`
                String representation of SQL query

        Returns:
            Polars DataFrame object
        """

        if not timeout:
            timeout = self.timeout

        logger.debug("Running SQL...", sql)

        # Establish query job against BigQuery API
        query_job = self.client.query(query=sql, timeout=timeout)

        # Wait for query to finish
        result = query_job.result()

        # Read as arrow stream into Polars DataFrame
        df = pl.from_arrow(result.to_arrow())

        # Query yielded no results
        if df.is_empty():
            logger.warning("No results returned")
            return pl.DataFrame()

        # Intended for DDL calls (adding columns, creating views, etc.)
        elif not return_results:
            return

        # Query returned results
        else:
            logger.info(f"Successfully read {len(df)} rows from BigQuery")
            return df
