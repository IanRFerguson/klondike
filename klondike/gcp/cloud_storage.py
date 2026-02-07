import os
from typing import Any, List, Optional, Union

import polars as pl
from google.storage import Blob, Bucket, Client

from klondike.base.abc_klondike import KlondikeBaseStorageConnector
from klondike.utilities.logger import logger


class CloudStorageConnector(KlondikeBaseStorageConnector):
    def __init__(
        self,
        app_creds: Optional[Union[str, dict[str, Any]]] = None,
        project: Optional[str] = None,
        location: Optional[str] = None,
        google_environment_variable: str = "GOOGLE_APPLICATION_CREDENTIALS",
        bypass_env_var_check: bool = False,
    ):
        """
        Initialize the Cloud Storage connector.

        Args:
            app_creds: Optional credentials for authentication.
                        Can be a path to a JSON key file or a dictionary of credentials.
            project: Optional GCP project name.
            location: Optional GCP location/region.
            google_environment_variable: Name of the environment variable to check for credentials if app_creds is not provided.
            bypass_env_var_check: If True, will not check for environment variable if app_creds is None. Useful for cases where credentials are provided through other means (e.g., metadata server in GCP).
        """

        self.app_creds = app_creds
        self.project = project
        self.location = location
        self.google_environment_variable = google_environment_variable
        self.bypass_env_var_check = bypass_env_var_check

        self._client = None

    @property
    def client(self) -> Client:
        if self._client is None:
            if self.app_creds is not None:
                self._client = (
                    Client.from_service_account_info(self.app_creds)
                    if isinstance(self.app_creds, dict)
                    else Client.from_service_account_file(self.app_creds)
                )
            elif (
                not self.bypass_env_var_check
                and self.google_environment_variable in os.environ
            ):
                self._client = Client()
            else:
                raise ValueError(
                    "No credentials provided for Cloud Storage Connector. Please provide app_creds or set the appropriate environment variable."
                )
        return self._client

    def list_blobs(
        self,
        bucket_name: str,
        prefix: Optional[str] = None,
        pattern: Optional[str] = None,
        **kwargs: Any,
    ) -> list[str]:
        """
        List blobs in a specified bucket, with optional filtering by prefix and pattern.

        Args:
            bucket_name: Name of the bucket to list blobs from.
            prefix: Optional prefix to filter blobs (e.g., folder path).
            pattern: Optional pattern to filter blobs (e.g., file extension).

        Returns:
            List of blob names matching the specified criteria.
        """

        bucket = self.client.bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=prefix)

        if pattern:
            blobs = [blob for blob in blobs if pattern in blob.name]

        return [blob.name for blob in blobs]

    def list_buckets(self, **kwargs: Any) -> list[str]:
        """
        List all buckets in the project.
        Returns:
            List of bucket names in the project.
        """

        return [bucket.name for bucket in self.client.list_buckets()]

    def get_blob(
        self,
        bucket_name: str,
        blob_name: str,
        local_path: Optional[str] = None,
        **kwargs: Any,
    ) -> pl.DataFrame:
        """
        Download a blob from Cloud Storage and read it as a Polars DataFrame.

        Args:
            bucket_name: Name of the bucket containing the blob.
            blob_name: Name of the blob to download.
            local_path: Optional local file path to save the downloaded blob. If not provided, will save to a temp file.
        Returns:
            Polars DataFrame read from the blob.
        """

        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        if local_path is None:
            local_path = f"/tmp/{os.path.basename(blob_name)}"

        blob.download_to_filename(local_path)

        # Read the file as a DataFrame (assuming CSV format, adjust as needed)
        df = pl.read_csv(local_path)

        # Clean up temp file if we created it
        if local_path.startswith("/tmp/"):
            os.remove(local_path)

        return df

    def put_blob(
        self, df: pl.DataFrame, bucket_name: str, blob_name: str, **kwargs: Any
    ) -> None:
        """
        Upload a Polars DataFrame to Cloud Storage as a blob.

        Args:
            df: Polars DataFrame to upload.
            bucket_name: Name of the destination bucket.
            blob_name: Name of the destination blob (including any desired folder path).
        """

        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        # Save DataFrame to a temporary CSV file
        temp_file_path = f"/tmp/{blob_name.replace('/', '_')}"
        df.write_csv(temp_file_path)

        try:
            # Upload the temporary file to Cloud Storage
            blob.upload_from_filename(temp_file_path)

        finally:
            # Clean up the temporary file
            os.remove(temp_file_path)
