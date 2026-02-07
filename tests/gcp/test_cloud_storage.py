import os
import unittest
from unittest.mock import MagicMock, Mock, patch

import polars as pl

from klondike.gcp.cloud_storage import CloudStorageConnector
from tests.common.tests_common import MOCK_DATAFRAME_RESP


class TestCloudStorageConnector(unittest.TestCase):
    def test_init_cloud_storage__app_creds_dict(self):
        """
        Test for valid construction with explicit
        application credentials as dictionary
        """

        cs = CloudStorageConnector(app_creds={"foo": "bar"})

        assert isinstance(cs, CloudStorageConnector)
        assert cs.app_creds == {"foo": "bar"}

    def test_init_cloud_storage__app_creds_path(self):
        """
        Test for valid construction with explicit
        application credentials as file path
        """

        cs = CloudStorageConnector(app_creds="/path/to/creds.json")

        assert isinstance(cs, CloudStorageConnector)
        assert cs.app_creds == "/path/to/creds.json"

    @patch.dict(
        os.environ, {"GOOGLE_APPLICATION_CREDENTIALS": "/path/to/env/creds.json"}
    )
    def test_init_cloud_storage__env_variable(self):
        """
        Test for valid construction when credentials are
        provided via environment variable
        """

        cs = CloudStorageConnector()

        assert isinstance(cs, CloudStorageConnector)
        assert cs.google_environment_variable in os.environ

    def test_init_cloud_storage__bypass_env_check(self):
        """
        Test for valid construction when bypassing
        environment variable check
        """

        cs = CloudStorageConnector(bypass_env_var_check=True)

        assert isinstance(cs, CloudStorageConnector)
        assert cs.bypass_env_var_check is True

    @patch("klondike.gcp.cloud_storage.Client")
    def test_list_buckets(self, MockClient):
        """
        Test for list_buckets method
        """

        # Mock the client and buckets
        mock_client = MagicMock()
        MockClient.from_service_account_info.return_value = mock_client

        # Create mock bucket objects
        mock_bucket_1 = MagicMock()
        mock_bucket_1.name = "bucket-1"

        mock_bucket_2 = MagicMock()
        mock_bucket_2.name = "bucket-2"

        mock_bucket_3 = MagicMock()
        mock_bucket_3.name = "bucket-3"

        mock_client.list_buckets.return_value = [
            mock_bucket_1,
            mock_bucket_2,
            mock_bucket_3,
        ]

        cs = CloudStorageConnector(app_creds={"foo": "bar"})
        buckets = cs.list_buckets()

        mock_client.list_buckets.assert_called_once()

        self.assertEqual(buckets, ["bucket-1", "bucket-2", "bucket-3"])

    @patch("klondike.gcp.cloud_storage.Client")
    def test_list_blobs__no_filter(self, MockClient):
        """
        Test for list_blobs method without prefix or pattern
        """

        # Mock the client and bucket
        mock_client = MagicMock()
        MockClient.from_service_account_info.return_value = mock_client

        mock_bucket = MagicMock()
        mock_client.bucket.return_value = mock_bucket

        # Create mock blob objects
        mock_blob_1 = MagicMock()
        mock_blob_1.name = "file1.csv"

        mock_blob_2 = MagicMock()
        mock_blob_2.name = "file2.csv"

        mock_blob_3 = MagicMock()
        mock_blob_3.name = "folder/file3.csv"

        mock_bucket.list_blobs.return_value = [
            mock_blob_1,
            mock_blob_2,
            mock_blob_3,
        ]

        cs = CloudStorageConnector(app_creds={"foo": "bar"})
        blobs = cs.list_blobs(bucket_name="test-bucket")

        mock_client.bucket.assert_called_once_with("test-bucket")
        mock_bucket.list_blobs.assert_called_once_with(prefix=None)

        self.assertEqual(blobs, ["file1.csv", "file2.csv", "folder/file3.csv"])

    @patch("klondike.gcp.cloud_storage.Client")
    def test_list_blobs__with_prefix(self, MockClient):
        """
        Test for list_blobs method with prefix filter
        """

        # Mock the client and bucket
        mock_client = MagicMock()
        MockClient.from_service_account_info.return_value = mock_client

        mock_bucket = MagicMock()
        mock_client.bucket.return_value = mock_bucket

        # Create mock blob objects (already filtered by prefix)
        mock_blob_1 = MagicMock()
        mock_blob_1.name = "folder/file1.csv"

        mock_blob_2 = MagicMock()
        mock_blob_2.name = "folder/file2.csv"

        mock_bucket.list_blobs.return_value = [mock_blob_1, mock_blob_2]

        cs = CloudStorageConnector(app_creds={"foo": "bar"})
        blobs = cs.list_blobs(bucket_name="test-bucket", prefix="folder/")

        mock_client.bucket.assert_called_once_with("test-bucket")
        mock_bucket.list_blobs.assert_called_once_with(prefix="folder/")

        self.assertEqual(blobs, ["folder/file1.csv", "folder/file2.csv"])

    @patch("klondike.gcp.cloud_storage.Client")
    def test_list_blobs__with_pattern(self, MockClient):
        """
        Test for list_blobs method with pattern filter
        """

        # Mock the client and bucket
        mock_client = MagicMock()
        MockClient.from_service_account_info.return_value = mock_client

        mock_bucket = MagicMock()
        mock_client.bucket.return_value = mock_bucket

        # Create mock blob objects
        mock_blob_1 = MagicMock()
        mock_blob_1.name = "file1.csv"

        mock_blob_2 = MagicMock()
        mock_blob_2.name = "file2.parquet"

        mock_blob_3 = MagicMock()
        mock_blob_3.name = "file3.csv"

        mock_bucket.list_blobs.return_value = [
            mock_blob_1,
            mock_blob_2,
            mock_blob_3,
        ]

        cs = CloudStorageConnector(app_creds={"foo": "bar"})
        blobs = cs.list_blobs(bucket_name="test-bucket", pattern=".csv")

        mock_client.bucket.assert_called_once_with("test-bucket")

        # Should only return CSV files
        self.assertEqual(blobs, ["file1.csv", "file3.csv"])

    @patch("klondike.gcp.cloud_storage.Client")
    @patch("polars.read_csv")
    @patch("os.remove")
    def test_get_blob(self, MockOsRemove, MockPolarsReadCsv, MockClient):
        """
        Test for get_blob method
        """

        # Mock the client, bucket, and blob
        mock_client = MagicMock()
        MockClient.from_service_account_info.return_value = mock_client

        mock_bucket = MagicMock()
        mock_client.bucket.return_value = mock_bucket

        mock_blob = MagicMock()
        mock_bucket.blob.return_value = mock_blob

        # Mock Polars DataFrame
        MockPolarsReadCsv.return_value = MOCK_DATAFRAME_RESP

        cs = CloudStorageConnector(app_creds={"foo": "bar"})
        df = cs.get_blob(bucket_name="test-bucket", blob_name="data/file.csv")

        # Assert correct method calls
        mock_client.bucket.assert_called_once_with("test-bucket")
        mock_bucket.blob.assert_called_once_with("data/file.csv")
        mock_blob.download_to_filename.assert_called_once()

        # Assert DataFrame is returned
        assert isinstance(df, pl.DataFrame)
        assert "name" in df.columns

        # Assert temp file cleanup
        MockOsRemove.assert_called_once()

    @patch("klondike.gcp.cloud_storage.Client")
    @patch("polars.DataFrame.write_csv")
    @patch("os.remove")
    def test_put_blob(self, MockOsRemove, MockWriteCsv, MockClient):
        """
        Test for put_blob method
        """

        # Mock the client, bucket, and blob
        mock_client = MagicMock()
        MockClient.from_service_account_info.return_value = mock_client

        mock_bucket = MagicMock()
        mock_client.bucket.return_value = mock_bucket

        mock_blob = MagicMock()
        mock_bucket.blob.return_value = mock_blob

        cs = CloudStorageConnector(app_creds={"foo": "bar"})
        cs.put_blob(
            df=MOCK_DATAFRAME_RESP,
            bucket_name="test-bucket",
            blob_name="data/output.csv",
        )

        # Assert correct method calls
        mock_client.bucket.assert_called_once_with("test-bucket")
        mock_bucket.blob.assert_called_once_with("data/output.csv")
        mock_blob.upload_from_filename.assert_called_once()

        # Assert temp file cleanup
        MockOsRemove.assert_called_once()

    def test_client_lazy_initialization(self):
        """
        Test that the client property is lazily initialized
        """

        cs = CloudStorageConnector(app_creds={"foo": "bar"})

        # Client should be None before first access
        assert cs._client is None

    @patch.dict(os.environ, {}, clear=True)
    def test_client_no_credentials_error(self):
        """
        Test that an error is raised when no credentials are provided
        """

        cs = CloudStorageConnector()

        with self.assertRaises(ValueError) as context:
            _ = cs.client

        self.assertIn("No credentials provided", str(context.exception))


if __name__ == "__main__":
    unittest.main()
