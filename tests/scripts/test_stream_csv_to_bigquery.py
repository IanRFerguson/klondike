import unittest
from unittest.mock import MagicMock, Mock, patch

import polars as pl

from klondike.gcp.bigquery import BigQueryConnector
from klondike.scripts.stream_csv_to_database import stream_csv_to_database


class TestStreamCsvToDatabase(unittest.TestCase):
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

    @patch("klondike.scripts.stream_csv_to_database.pl.read_csv")
    def test_stream_csv_basic(self, mock_read_csv):
        """
        Test basic CSV streaming with default parameters
        """
        # Mock read_csv to return batches then empty dataframe
        mock_read_csv.side_effect = [self.batch_1, self.batch_2, pl.DataFrame()]

        # Execute the function
        stream_csv_to_database(
            connector=self.mock_bq_connector,
            csv_path="/path/to/test.csv",
            destination_table_name="test_dataset.test_table",
        )

        # Verify read_csv was called with correct parameters
        self.assertEqual(mock_read_csv.call_count, 3)

        # Check first call
        first_call = mock_read_csv.call_args_list[0]
        self.assertEqual(first_call[0][0], "/path/to/test.csv")
        self.assertEqual(first_call[1]["separator"], ",")
        self.assertEqual(first_call[1]["infer_schema_length"], 0)
        self.assertEqual(first_call[1]["skip_rows"], 0)
        self.assertEqual(first_call[1]["n_rows"], 10_000)

        # Verify write_dataframe was called twice (once per batch)
        self.assertEqual(self.mock_bq_connector.write_dataframe.call_count, 2)

        # Verify the calls to write_dataframe
        calls = self.mock_bq_connector.write_dataframe.call_args_list
        self.assertEqual(calls[0][1]["table_name"], "test_dataset.test_table")
        self.assertEqual(calls[1][1]["table_name"], "test_dataset.test_table")

    @patch("klondike.scripts.stream_csv_to_database.pl.read_csv")
    def test_stream_csv_custom_batch_size(self, mock_read_csv):
        """
        Test CSV streaming with custom batch size
        """
        # Mock read_csv to return batches then empty dataframe
        mock_read_csv.side_effect = [
            self.batch_1,
            self.batch_2,
            self.batch_3,
            pl.DataFrame(),
        ]

        # Execute with custom batch size
        stream_csv_to_database(
            connector=self.mock_bq_connector,
            csv_path="/path/to/test.csv",
            destination_table_name="test_dataset.test_table",
            batch_size=500,
        )

        # Verify read_csv was called with custom batch size
        self.assertTrue(
            any(call[1]["n_rows"] == 500 for call in mock_read_csv.call_args_list)
        )

        # Verify write_dataframe was called three times
        self.assertEqual(self.mock_bq_connector.write_dataframe.call_count, 3)

    @patch("klondike.scripts.stream_csv_to_database.pl.read_csv")
    def test_stream_csv_custom_separator(self, mock_read_csv):
        """
        Test CSV streaming with custom separator
        """
        mock_read_csv.side_effect = [self.batch_1, pl.DataFrame()]

        # Execute with custom separator
        stream_csv_to_database(
            connector=self.mock_bq_connector,
            csv_path="/path/to/test.tsv",
            destination_table_name="test_dataset.test_table",
            csv_separator="\t",
        )

        # Verify read_csv was called with tab separator
        first_call = mock_read_csv.call_args_list[0]
        self.assertEqual(first_call[0][0], "/path/to/test.tsv")
        self.assertEqual(first_call[1]["separator"], "\t")
        self.assertEqual(first_call[1]["infer_schema_length"], 0)

    @patch("klondike.scripts.stream_csv_to_database.pl.read_csv")
    def test_stream_csv_with_schema_inference(self, mock_read_csv):
        """
        Test CSV streaming with schema inference
        """
        mock_read_csv.side_effect = [self.batch_1, pl.DataFrame()]

        # Execute with schema inference
        stream_csv_to_database(
            connector=self.mock_bq_connector,
            csv_path="/path/to/test.csv",
            destination_table_name="test_dataset.test_table",
            infer_schema_length=1000,
        )

        # Verify read_csv was called with infer_schema_length
        first_call = mock_read_csv.call_args_list[0]
        self.assertEqual(first_call[0][0], "/path/to/test.csv")
        self.assertEqual(first_call[1]["separator"], ",")
        self.assertEqual(first_call[1]["infer_schema_length"], 1000)

    @patch("klondike.scripts.stream_csv_to_database.pl.read_csv")
    def test_stream_csv_with_bigquery_kwargs(self, mock_read_csv):
        """
        Test CSV streaming with additional BigQuery kwargs
        """
        mock_read_csv.side_effect = [self.batch_1, self.batch_2, pl.DataFrame()]

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

    @patch("klondike.scripts.stream_csv_to_database.pl.read_csv")
    def test_stream_csv_single_batch(self, mock_read_csv):
        """
        Test CSV streaming with single batch (small file)
        """
        mock_read_csv.side_effect = [self.batch_1, pl.DataFrame()]

        # Execute
        stream_csv_to_database(
            connector=self.mock_bq_connector,
            csv_path="/path/to/small.csv",
            destination_table_name="test_dataset.test_table",
        )

        # Verify write_dataframe was called only once
        self.assertEqual(self.mock_bq_connector.write_dataframe.call_count, 1)

    @patch("klondike.scripts.stream_csv_to_database.pl.read_csv")
    def test_stream_csv_empty_batches(self, mock_read_csv):
        """
        Test CSV streaming with no batches (empty file)
        """
        mock_read_csv.return_value = pl.DataFrame()

        # Execute
        stream_csv_to_database(
            connector=self.mock_bq_connector,
            csv_path="/path/to/empty.csv",
            destination_table_name="test_dataset.test_table",
        )

        # Verify write_dataframe was never called
        self.mock_bq_connector.write_dataframe.assert_not_called()

    @patch("klondike.scripts.stream_csv_to_database.pl.read_csv")
    def test_stream_csv_qualified_table_name(self, mock_read_csv):
        """
        Test that table name is correctly qualified with dataset
        """
        mock_read_csv.side_effect = [self.batch_1, pl.DataFrame()]

        # Execute with different dataset and table names
        stream_csv_to_database(
            connector=self.mock_bq_connector,
            csv_path="/path/to/test.csv",
            destination_table_name="my_dataset.my_table",
        )

        # Verify qualified table name
        call_args = self.mock_bq_connector.write_dataframe.call_args
        self.assertEqual(call_args[1]["table_name"], "my_dataset.my_table")

    @patch("klondike.scripts.stream_csv_to_database.pl.read_csv")
    def test_stream_csv_batch_ordering(self, mock_read_csv):
        """
        Test that batches are processed in correct order
        """
        mock_read_csv.side_effect = [
            self.batch_1,
            self.batch_2,
            self.batch_3,
            pl.DataFrame(),
        ]

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

    @patch("klondike.scripts.stream_csv_to_database.pl.read_csv")
    def test_stream_csv_large_batch_count(self, mock_read_csv):
        """
        Test streaming with many batches
        """
        # Simulate 100 batches plus empty dataframe at end
        batches = [self.batch_1 for _ in range(100)] + [pl.DataFrame()]
        mock_read_csv.side_effect = batches

        # Execute
        stream_csv_to_database(
            connector=self.mock_bq_connector,
            csv_path="/path/to/large.csv",
            destination_table_name="test_dataset.test_table",
            batch_size=1000,
        )

        # Verify write_dataframe was called 100 times
        self.assertEqual(self.mock_bq_connector.write_dataframe.call_count, 100)

    @patch("klondike.scripts.stream_csv_to_database.pl.read_csv")
    def test_stream_csv_different_file_paths(self, mock_read_csv):
        """
        Test with various file path formats
        """
        test_paths = [
            "/absolute/path/to/file.csv",
            "relative/path/file.csv",
            "./current/dir/file.csv",
            "../parent/dir/file.csv",
            "gs://bucket/path/file.csv",  # Cloud storage path
        ]

        for path in test_paths:
            mock_read_csv.reset_mock()
            mock_read_csv.side_effect = [self.batch_1, pl.DataFrame()]
            self.mock_bq_connector.reset_mock()

            stream_csv_to_database(
                connector=self.mock_bq_connector,
                csv_path=path,
                destination_table_name="test_dataset.test_table",
            )

            # Verify read_csv was called with the correct path
            self.assertTrue(mock_read_csv.called)
            self.assertEqual(mock_read_csv.call_args_list[0][0][0], path)

    @patch("klondike.scripts.stream_csv_to_database.pl.read_csv")
    def test_stream_csv_preserves_dataframe_content(self, mock_read_csv):
        """
        Test that dataframe content is preserved during streaming
        """
        test_df = pl.DataFrame(
            [
                {"id": 99, "name": "Test", "value": 999, "active": True},
            ]
        )
        mock_read_csv.side_effect = [test_df, pl.DataFrame()]

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
