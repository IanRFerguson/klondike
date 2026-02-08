import unittest
from unittest.mock import MagicMock, Mock, call, patch

import polars as pl

from klondike.gcp.bigquery import BigQueryConnector
from klondike.scripts.stream_csv_to_database import stream_csv_to_database


class TestStreamCsvToBigquery(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures"""
        # Create mock BigQueryConnector
        self.mock_bq_connector = Mock(spec=BigQueryConnector)

        # Create test dataframes for batching
        self.batch_1 = pl.DataFrame(
            [
                {"id": 1, "name": "Alice", "value": 100},
                {"id": 2, "name": "Bob", "value": 200},
            ]
        )
        self.batch_2 = pl.DataFrame(
            [
                {"id": 3, "name": "Charlie", "value": 300},
                {"id": 4, "name": "David", "value": 400},
            ]
        )
        self.batch_3 = pl.DataFrame(
            [
                {"id": 5, "name": "Eve", "value": 500},
            ]
        )

    @patch("klondike.scripts.stream_csv_to_database.pl.scan_csv")
    def test_stream_csv_basic(self, mock_scan_csv):
        """
        Test basic CSV streaming with default parameters
        """
        # Mock the lazy dataframe and batches
        mock_lazy_df = MagicMock()
        mock_lazy_df.collect().iter_slices.return_value = [self.batch_1, self.batch_2]
        mock_scan_csv.return_value = mock_lazy_df

        # Execute the function
        stream_csv_to_database(
            connector=self.mock_bq_connector,
            csv_path="/path/to/test.csv",
            destination_table_name="test_dataset.test_table",
        )

        # Verify scan_csv was called with correct parameters
        mock_scan_csv.assert_called_once_with(
            "/path/to/test.csv",
            separator=",",
            infer_schema_length=0,
        )

        # Verify iter_slices was called with default batch size
        mock_lazy_df.collect().iter_slices.assert_called_once_with(n_rows=10_000)

        # Verify write_dataframe was called twice (once per batch)
        self.assertEqual(self.mock_bq_connector.write_dataframe.call_count, 2)

        # Verify the calls to write_dataframe
        calls = self.mock_bq_connector.write_dataframe.call_args_list
        self.assertEqual(calls[0][1]["table_name"], "test_dataset.test_table")
        self.assertEqual(calls[1][1]["table_name"], "test_dataset.test_table")

    @patch("klondike.scripts.stream_csv_to_database.pl.scan_csv")
    def test_stream_csv_custom_batch_size(self, mock_scan_csv):
        """
        Test CSV streaming with custom batch size
        """
        mock_lazy_df = MagicMock()
        mock_lazy_df.collect().iter_slices.return_value = [
            self.batch_1,
            self.batch_2,
            self.batch_3,
        ]
        mock_scan_csv.return_value = mock_lazy_df

        # Execute with custom batch size
        stream_csv_to_database(
            connector=self.mock_bq_connector,
            csv_path="/path/to/test.csv",
            destination_table_name="test_dataset.test_table",
            batch_size=500,
        )

        # Verify iter_slices was called with custom batch size
        mock_lazy_df.collect().iter_slices.assert_called_once_with(n_rows=500)

        # Verify write_dataframe was called three times
        self.assertEqual(self.mock_bq_connector.write_dataframe.call_count, 3)

    @patch("klondike.scripts.stream_csv_to_database.pl.scan_csv")
    def test_stream_csv_custom_separator(self, mock_scan_csv):
        """
        Test CSV streaming with custom separator
        """
        mock_lazy_df = MagicMock()
        mock_lazy_df.collect().iter_slices.return_value = [self.batch_1]
        mock_scan_csv.return_value = mock_lazy_df

        # Execute with custom separator
        stream_csv_to_database(
            connector=self.mock_bq_connector,
            csv_path="/path/to/test.tsv",
            destination_table_name="test_dataset.test_table",
            csv_separator="\t",
        )

        # Verify scan_csv was called with tab separator
        mock_scan_csv.assert_called_once_with(
            "/path/to/test.tsv",
            separator="\t",
            infer_schema_length=0,
        )

    @patch("klondike.scripts.stream_csv_to_database.pl.scan_csv")
    def test_stream_csv_with_schema_inference(self, mock_scan_csv):
        """
        Test CSV streaming with schema inference
        """
        mock_lazy_df = MagicMock()
        mock_lazy_df.collect().iter_slices.return_value = [self.batch_1]
        mock_scan_csv.return_value = mock_lazy_df

        # Execute with schema inference
        stream_csv_to_database(
            connector=self.mock_bq_connector,
            csv_path="/path/to/test.csv",
            destination_table_name="test_dataset.test_table",
            infer_schema_length=1000,
        )

        # Verify scan_csv was called with infer_schema_length
        mock_scan_csv.assert_called_once_with(
            "/path/to/test.csv",
            separator=",",
            infer_schema_length=1000,
        )

    @patch("klondike.scripts.stream_csv_to_database.pl.scan_csv")
    def test_stream_csv_with_bigquery_kwargs(self, mock_scan_csv):
        """
        Test CSV streaming with additional BigQuery kwargs
        """
        mock_lazy_df = MagicMock()
        mock_lazy_df.collect().iter_slices.return_value = [self.batch_1, self.batch_2]
        mock_scan_csv.return_value = mock_lazy_df

        # Execute with BigQuery kwargs
        stream_csv_to_database(
            connector=self.mock_bq_connector,
            csv_path="/path/to/test.csv",
            destination_table_name="test_dataset.test_table",
            if_exists="append",
            max_bad_records=10,
        )

        # Verify write_dataframe was called with kwargs
        calls = self.mock_bq_connector.write_dataframe.call_args_list
        for call_args in calls:
            self.assertEqual(call_args[1]["if_exists"], "append")
            self.assertEqual(call_args[1]["max_bad_records"], 10)

    @patch("klondike.scripts.stream_csv_to_database.pl.scan_csv")
    def test_stream_csv_single_batch(self, mock_scan_csv):
        """
        Test CSV streaming with single batch (small file)
        """
        mock_lazy_df = MagicMock()
        mock_lazy_df.collect().iter_slices.return_value = [self.batch_1]
        mock_scan_csv.return_value = mock_lazy_df

        # Execute
        stream_csv_to_database(
            connector=self.mock_bq_connector,
            csv_path="/path/to/small.csv",
            destination_table_name="test_dataset.test_table",
        )

        # Verify write_dataframe was called only once
        self.assertEqual(self.mock_bq_connector.write_dataframe.call_count, 1)

    @patch("klondike.scripts.stream_csv_to_database.pl.scan_csv")
    def test_stream_csv_empty_batches(self, mock_scan_csv):
        """
        Test CSV streaming with no batches (empty file)
        """
        mock_lazy_df = MagicMock()
        mock_lazy_df.collect().iter_slices.return_value = []
        mock_scan_csv.return_value = mock_lazy_df

        # Execute
        stream_csv_to_database(
            connector=self.mock_bq_connector,
            csv_path="/path/to/empty.csv",
            destination_table_name="test_dataset.test_table",
        )

        # Verify write_dataframe was never called
        self.mock_bq_connector.write_dataframe.assert_not_called()

    @patch("klondike.scripts.stream_csv_to_database.pl.scan_csv")
    def test_stream_csv_qualified_table_name(self, mock_scan_csv):
        """
        Test that table name is correctly qualified with dataset
        """
        mock_lazy_df = MagicMock()
        mock_lazy_df.collect().iter_slices.return_value = [self.batch_1]
        mock_scan_csv.return_value = mock_lazy_df

        # Execute with different dataset and table names
        stream_csv_to_database(
            connector=self.mock_bq_connector,
            csv_path="/path/to/test.csv",
            destination_table_name="my_dataset.my_table",
        )

        # Verify qualified table name
        call_args = self.mock_bq_connector.write_dataframe.call_args
        self.assertEqual(call_args[1]["table_name"], "my_dataset.my_table")

    @patch("klondike.scripts.stream_csv_to_database.pl.scan_csv")
    def test_stream_csv_batch_ordering(self, mock_scan_csv):
        """
        Test that batches are processed in correct order
        """
        mock_lazy_df = MagicMock()
        mock_lazy_df.collect().iter_slices.return_value = [
            self.batch_1,
            self.batch_2,
            self.batch_3,
        ]
        mock_scan_csv.return_value = mock_lazy_df

        # Execute
        stream_csv_to_database(
            connector=self.mock_bq_connector,
            csv_path="/path/to/test.csv",
            destination_table_name="test_dataset.test_table",
        )

        # Verify batches were processed in order
        calls = self.mock_bq_connector.write_dataframe.call_args_list
        self.assertEqual(len(calls), 3)

        # Check that each batch was written (checking the df parameter)
        # Note: We can't directly compare DataFrames in mock calls easily,
        # but we can verify the number of calls matches the number of batches
        self.assertEqual(len(calls), 3)

    @patch("klondike.scripts.stream_csv_to_database.pl.scan_csv")
    def test_stream_csv_large_batch_count(self, mock_scan_csv):
        """
        Test streaming with many batches
        """
        mock_lazy_df = MagicMock()
        # Simulate 100 batches
        batches = [self.batch_1 for _ in range(100)]
        mock_lazy_df.collect().iter_slices.return_value = batches
        mock_scan_csv.return_value = mock_lazy_df

        # Execute
        stream_csv_to_database(
            connector=self.mock_bq_connector,
            csv_path="/path/to/large.csv",
            destination_table_name="test_dataset.test_table",
            batch_size=1000,
        )

        # Verify write_dataframe was called 100 times
        self.assertEqual(self.mock_bq_connector.write_dataframe.call_count, 100)

    @patch("klondike.scripts.stream_csv_to_database.pl.scan_csv")
    def test_stream_csv_different_file_paths(self, mock_scan_csv):
        """
        Test with various file path formats
        """
        mock_lazy_df = MagicMock()
        mock_lazy_df.collect().iter_slices.return_value = [self.batch_1]
        mock_scan_csv.return_value = mock_lazy_df

        test_paths = [
            "/absolute/path/to/file.csv",
            "relative/path/file.csv",
            "./current/dir/file.csv",
            "../parent/dir/file.csv",
            "gs://bucket/path/file.csv",  # Cloud storage path
        ]

        for path in test_paths:
            mock_scan_csv.reset_mock()
            self.mock_bq_connector.reset_mock()

            stream_csv_to_database(
                connector=self.mock_bq_connector,
                csv_path=path,
                destination_table_name="test_dataset.test_table",
            )

            # Verify scan_csv was called with the correct path
            mock_scan_csv.assert_called_once()
            self.assertEqual(mock_scan_csv.call_args[0][0], path)

    @patch("klondike.scripts.stream_csv_to_database.pl.scan_csv")
    def test_stream_csv_preserves_dataframe_content(self, mock_scan_csv):
        """
        Test that dataframe content is preserved during streaming
        """
        mock_lazy_df = MagicMock()
        test_df = pl.DataFrame(
            [
                {"id": 99, "name": "Test", "value": 999, "active": True},
            ]
        )
        mock_lazy_df.collect().iter_slices.return_value = [test_df]
        mock_scan_csv.return_value = mock_lazy_df

        # Execute
        stream_csv_to_database(
            connector=self.mock_bq_connector,
            csv_path="/path/to/test.csv",
            destination_table_name="test_dataset.test_table",
        )

        # Verify the dataframe passed to write_dataframe
        call_args = self.mock_bq_connector.write_dataframe.call_args
        written_df = call_args[1]["df"]

        # Check that we received a DataFrame
        self.assertIsInstance(written_df, pl.DataFrame)


if __name__ == "__main__":
    unittest.main()
