from typing import Any

import polars as pl

from klondike.gcp.bigquery import BigQueryConnector
from klondike.utilities.logger import logger


def stream_csv_to_bigquery(
    bigquery_connector: BigQueryConnector,
    csv_path: str,
    dataset_name: str,
    table_name: str,
    batch_size: int = 10_000,
    infer_schema_length: int = 0,
    csv_separator: str = ",",
    **bigquery_kwargs: Any,
) -> None:
    """
    Stream a CSV file to BigQuery in batches.

    Args:
        bigquery_connector: An instance of BigQueryConnector to use for uploading data.
        csv_path: Path to the CSV file to be uploaded.
        dataset_name: Name of the BigQuery dataset where the table will be created/overwritten.
        table_name: Name of the BigQuery table to create/overwrite.
        batch_size: Number of rows to include in each batch upload (default: 10,000).
        infer_schema_length: Number of rows to read for inferring schema (default: 0, which means no inference and all columns will be treated as strings).
        csv_separator: The delimiter used in the CSV file (default: ",").
        **bigquery_kwargs: Additional keyword arguments to pass to the BigQuery upload method (e.g., write_disposition, etc.).
    """

    lazy_df = pl.scan_csv(
        csv_path,
        separator=csv_separator,
        infer_schema_length=infer_schema_length,
    )

    records_written = 0
    qualified_table_name = f"{dataset_name}.{table_name}"
    logger.info(
        f"Streaming data from {csv_path} to BigQuery table {qualified_table_name} in batches of {batch_size}..."
    )
    for batch, batched_df in enumerate(
        lazy_df.partition_by_row_count(batch_size).collect()
    ):
        logger.debug(f"Uploading batch {batch + 1} to BigQuery...")
        bigquery_connector.write_dataframe(
            df=batched_df,
            table_name=qualified_table_name,
            **bigquery_kwargs,
        )
        records_written += len(batched_df)

    logger.info(f"Finished streaming {records_written} rows to {qualified_table_name}")
