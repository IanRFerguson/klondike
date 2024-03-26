import io
import json
import os
import tempfile
from typing import Optional, Union

import polars as pl
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig

from klondike import logger

##########

SCOPES = (
    {
        "scopes": [
            "https://www.googleapis.com/auth/bigquery",
        ]
    },
)[0]


class BigQueryConnector:
    """
    Establish and authenticate a connection to a BigQuery warehouse

    Args:
        app_creds: Google service account, either as a relative path or a dictionary instance
        project: Name of Google Project
        location: Location of Google Project
        timeout: Temporal threshold to kill a stalled job, defaults to 60s
        client_options: API scopes
        google_environment_variable: Provided for flexibility, defaults to `GOOGLE_APPLICATION_CREDENTIALS`
    """

    def __init__(
        self,
        app_creds: Optional[Union[str, dict]] = None,
        project: Optional[str] = None,
        location: Optional[str] = None,
        timeout: int = 60,
        client_options: dict = SCOPES,
        google_environment_variable: str = "GOOGLE_APPLICATION_CREDENTIALS",
    ):
        self.app_creds = app_creds
        self.project = project
        self.location = location
        self.timeout = timeout
        self.client_options = client_options

        self._client = None
        self.dialect = "bigquery"

        if not self.app_creds:
            if not os.environ.get(google_environment_variable):
                raise OSError("No app_creds provided")
            else:
                logger.info(
                    f"Using `{google_environment_variable}` variable defined in environment"
                )
        else:
            self.__setup_google_app_creds(
                app_creds=self.app_creds, env_variable=google_environment_variable
            )

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

        if not base_job_config:
            logger.debug("No job config provided, starting fresh")
            base_job_config = LoadJobConfig()

        # This is default behavior for this class
        base_job_config.source_format = bigquery.SourceFormat.PARQUET

        # Create table schema mapping if provided
        if table_schema:
            base_job_config.schema = self.__set_table_schema(table_schema=table_schema)
        else:
            base_job_config.schema = None

        base_job_config.max_bad_records = max_bad_records

        base_job_config.write_disposition = self.__set_write_disposition(
            if_exists=if_exists
        )

        # List of LoadJobConfig attributes
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

    def __set_table_schema(self, table_schema: list):
        "TODO - Write about me"

        return [bigquery.SchemaField(**x) for x in table_schema]

    def __set_write_disposition(self, if_exists: str):
        "TODO - Write about me"

        DISPOSITION_MAP = {
            "fail": bigquery.WriteDisposition.WRITE_EMPTY,
            "append": bigquery.WriteDisposition.WRITE_APPEND,
            "truncate": bigquery.WriteDisposition.WRITE_TRUNCATE,
        }

        return DISPOSITION_MAP[if_exists]

    @property
    def client(self):
        """
        Instantiate BigQuery client
        """

        if not self._client:
            self._client = bigquery.Client(
                project=self.project,
                location=self.location,
                client_options=self.client_options,
            )

        return self._client

    def read_dataframe_from_bigquery(self, sql: str) -> pl.DataFrame:
        """
        Executes a SQL query and returns a Polars DataFrame.
        TODO - Make this more flexible and incorporate query params

        Args:
            sql: String representation of SQL query

        Returns:
            Polars DataFrame object
        """

        # Define query job
        logger.debug("Running SQL...", sql)
        query_job = self.client.query(query=sql, timeout=self.timeout)

        # Execute and wait for results
        result = query_job.result()

        # Populate DataFrame using PyArrow
        df = pl.from_arrow(result.to_arrow())

        if df.is_empty():
            logger.info("No results returned from SQL call")
            return

        logger.info(f"Successfully read {len(df)} rows from BigQuery")
        return df

    def write_dataframe_to_bigquery(
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
            df: Polars DataFrame
            table_name: Destination table name to write to - `dataset.table` convention
            load_job_config: `LoadJobConfig` object. If none is supplied, several defaults are applied
            max_bad_records: Tolerance for bad records in the load job, defaults to 0
            table_schema: List of column names, types, and optional flags to include
            if_exists: One of `fail`, `drop`, `append`, `truncate`
            load_kwargs: See here for list of accepted values
                https://cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.job.LoadJobConfig
        """

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
