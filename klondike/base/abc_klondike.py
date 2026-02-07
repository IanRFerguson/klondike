from abc import ABC, abstractmethod
from typing import Union

import polars as pl


class KlondikeBaseDatabaseConnector(ABC):
    """
    Abstract base class for all Klondike database connectors.

    Defines the standard interface that all database implementations
    (Snowflake, BigQuery, Postgres, Redshift) must implement.
    """

    @property
    @abstractmethod
    def dialect(self) -> str:
        """Return the SQL dialect name for this connector (e.g., 'snowflake', 'bigquery')."""
        pass

    @abstractmethod
    def query(self, sql: str, **kwargs) -> Union[pl.DataFrame, None]:
        """
        Execute a SQL query and return results as a Polars DataFrame.

        Args:
            sql: SQL query string to execute
            **kwargs: Additional connector-specific query parameters

        Returns:
            Polars DataFrame with query results, or None for DDL operations
        """
        pass

    @abstractmethod
    def table_exists(self, table_name: str, **kwargs) -> bool:
        """
        Check if a table exists in the database.

        Args:
            table_name: Name of the table to check
            **kwargs: Additional connector-specific parameters (schema, database, etc.)

        Returns:
            True if table exists, False otherwise
        """
        pass

    @abstractmethod
    def list_tables(self, schema_name: str, **kwargs) -> list:
        """
        List all tables in a given schema.

        Args:
            schema_name: Name of the schema to list tables from
            **kwargs: Additional connector-specific parameters

        Returns:
            List of table names
        """
        pass

    @abstractmethod
    def read_dataframe(self, sql: str, **kwargs) -> pl.DataFrame:
        """
        Read data from the database into a Polars DataFrame.

        Args:
            sql: SQL query to execute
            **kwargs: Additional connector-specific parameters

        Returns:
            Polars DataFrame containing query results
        """
        pass

    @abstractmethod
    def write_dataframe(self, df: pl.DataFrame, table_name: str, **kwargs) -> None:
        """
        Write a Polars DataFrame to the database.

        Args:
            df: Polars DataFrame to write
            table_name: Destination table name
            **kwargs: Additional connector-specific parameters (if_exists, schema, etc.)
        """
        pass


class KlondikeBaseStorageConnector(ABC):
    """
    Abstract base class for all Klondike flat file storage connectors.

    Defines the standard interface for cloud storage implementations
    (S3, Cloud Storage, etc.).
    """

    @abstractmethod
    def list_buckets(self, **kwargs) -> list:
        """
        List all available buckets/containers.

        Returns:
            List of bucket names
        """
        pass

    @abstractmethod
    def list_blobs(self, bucket_name: str, **kwargs) -> list:
        """
        List all blobs/objects in a bucket.

        Args:
            bucket_name: Name of the bucket to list
            **kwargs: Additional connector-specific parameters

        Returns:
            List of blob/object names
        """
        pass

    @abstractmethod
    def get_blob(self, bucket_name: str, blob_name: str, **kwargs) -> pl.DataFrame:
        """
        Read a blob/object from storage into a Polars DataFrame.

        Args:
            bucket_name: Name of the bucket containing the blob
            blob_name: Name of the blob to read
            **kwargs: Additional connector-specific parameters

        Returns:
            Polars DataFrame with blob contents
        """
        pass

    @abstractmethod
    def put_blob(
        self, df: pl.DataFrame, bucket_name: str, blob_name: str, **kwargs
    ) -> None:
        """
        Write a Polars DataFrame to storage as a blob/object.

        Args:
            df: Polars DataFrame to write
            bucket_name: Destination bucket name
            blob_name: Destination blob name
            **kwargs: Additional connector-specific parameters
        """
        pass
