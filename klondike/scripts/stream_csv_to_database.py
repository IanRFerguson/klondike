from typing import Any, Union

import polars as pl

from klondike.aws.redshift import RedshiftConnector
from klondike.gcp.bigquery import BigQueryConnector
from klondike.snowflake.snowflake import SnowflakeConnector
from klondike.utilities.logger import logger

#####


def stream_csv_to_database(
    connector: Union[BigQueryConnector, SnowflakeConnector, RedshiftConnector],
    csv_path: str,
    destination_table_name: str,
    batch_size: int = 10_000,
    infer_schema_length: int = 0,
    csv_separator: str = ",",
    **bigquery_kwargs: Any,
) -> None:
    """
    Stream a CSV file to a database in batches.

    Args:
        connector: An instance of a database connector (BigQueryConnector, SnowflakeConnector, or RedshiftConnector) to use for uploading data.
        csv_path: Path to the CSV file to be uploaded.
        destination_table_name: Name of the destination table where the data will be created/overwritten.
        batch_size: Number of rows to include in each batch upload (default: 10,000).
        infer_schema_length: Number of rows to read for inferring schema (default: 0, which means no inference and all columns will be treated as strings).
        csv_separator: The delimiter used in the CSV file (default: ",").
        **bigquery_kwargs: Additional keyword arguments to pass to the BigQuery upload method (e.g., write_disposition, etc.).
    """

    records_written = 0
    skip_rows_count = 0
    logger.info(
        f"Streaming data from {csv_path} to table {destination_table_name} in batches of {batch_size}..."
    )

    while True:
        batch_df = pl.read_csv(
            csv_path,
            separator=csv_separator,
            infer_schema_length=infer_schema_length,
            skip_rows=skip_rows_count,
            n_rows=batch_size,
        )

        if len(batch_df) == 0:
            break

        logger.debug(f"Uploading batch to table {destination_table_name}...")
        connector.write_dataframe(
            df=batch_df,
            table_name=destination_table_name,
            **bigquery_kwargs,
        )
        records_written += len(batch_df)
        skip_rows_count += len(batch_df)

    logger.info(
        f"Finished streaming {records_written} rows to {destination_table_name}"
    )
