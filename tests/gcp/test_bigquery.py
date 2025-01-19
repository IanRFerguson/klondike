import unittest
from unittest.mock import MagicMock, patch

import polars as pl

from klondike.gcp.bigquery import BigQueryConnector

##########


class TestBigQueryConnector(unittest.TestCase):
    def test_init_bigquery__app_creds(self):
        """
        Test for valid construction with explicit
        application credentials
        """

        bq = BigQueryConnector(app_creds={"foo": "bar"})

        assert isinstance(bq, BigQueryConnector)

    @patch("google.auth.default")
    def test_init_bigquery__bybass_environment(self, MockGoogleAuth):
        """
        Test for valid construction when no app creds are
        passed in
        """

        # Mock Google's authentication strategy
        mock_credentials = MagicMock()
        MockGoogleAuth.return_value = (mock_credentials, "mock_project_id")

        bq = BigQueryConnector(bypass_env_variable=True)

        assert isinstance(bq, BigQueryConnector)

    def test_query(self):
        """
        Test for direct query against BigQuery warehouse
        """

        pass

    @patch("klondike.gcp.bigquery.bigquery.Client")
    def test_table_exists(self, MockBigQueryClient):
        """
        Test for table_exists method
        """

        mock_bq_client = MagicMock()
        MockBigQueryClient.return_value = mock_bq_client

        mock_table = MagicMock()
        mock_table.full_table_id = "schema.table"

        bq = BigQueryConnector(app_creds={"foo": "bar"})
        table_exists = bq.table_exists(table_name="schema.table")

        mock_bq_client.get_table.assert_called_once_with(table="schema.table")

        self.assertEqual(table_exists, True)

    @patch("google.auth.default")
    @patch("klondike.gcp.bigquery.bigquery.Client")
    def test_list_tables(self, MockBigQueryConnector, MockGoogleAuth):
        """
        Test for list_tables method
        """

        # Mock Google's authentication strategy
        mock_credentials = MagicMock()
        MockGoogleAuth.return_value = (mock_credentials, "mock_project_id")

        # Mock the underlying bigquery.Client object
        mock_bq_client = MagicMock()
        MockBigQueryConnector.return_value = mock_bq_client

        ###

        # Set up mock BigQuery tables
        mock_table_1 = MagicMock()
        mock_table_1.full_table_id = "project.dataset.table_1"

        mock_table_2 = MagicMock()
        mock_table_2.full_table_id = "project.dataset.table_2"

        mock_table_3 = MagicMock()
        mock_table_3.full_table_id = "project.dataset.table_3"

        # Assign return values from the API call
        mock_bq_client.list_tables.return_value = [
            mock_table_1,
            mock_table_2,
            mock_table_3,
        ]

        ###

        bq = BigQueryConnector(bypass_env_variable=True)
        table_values = bq.list_tables(schema_name="test_schema")

        # Assert that the API client was called
        mock_bq_client.list_tables.assert_called_once_with(dataset="test_schema")

        # Assert that table IDs are returned correctly
        self.assertEqual([x.split(".")[-1] for x in table_values], ["table_1", "table_2", "table_3"])

    @patch("klondike.gcp.bigquery.bigquery.Client")
    @patch("polars.from_arrow")
    def test_read_dataframe__no_results(self, MockPolarsObject, MockBigQueryClient):
        """
        Test for read_dataframe method with no results returned
        """

        mock_bq_client = MagicMock()
        MockBigQueryClient.return_value = mock_bq_client

        mock_polars = MagicMock()
        MockPolarsObject.return_value = mock_polars

        bq = BigQueryConnector(app_creds={"foo": "bar"})
        resp = bq.read_dataframe("select * from schema.table;")

        mock_bq_client.query.assert_called_once_with(query="select * from schema.table;", timeout=bq.timeout)

        assert not resp

    @patch("klondike.gcp.bigquery.bigquery.Client")
    @patch("polars.from_arrow")
    def test_read_dataframe__valid_results(self, MockPolarsObject, MockBigQueryClient):
        """
        Test for read_dataframe method with valid results returned
        """

        mock_bq_client = MagicMock()
        MockBigQueryClient.return_value = mock_bq_client

        mock_polars = pl.DataFrame(
            [
                {"name": "Ian", "city": "Brooklyn"},
                {"name": "Spencer", "city": "Tysons"},
                {"name": "Gavin", "city": "Arlington"},
                {"name": "Zach", "city": "Harrisonburg"},
            ]
        )
        MockPolarsObject.return_value = mock_polars

        bq = BigQueryConnector(app_creds={"foo": "bar"})
        resp = bq.read_dataframe("select * from schema.table;")

        mock_bq_client.query.assert_called_once_with(query="select * from schema.table;", timeout=bq.timeout)

        assert isinstance(resp, pl.DataFrame)
        assert "name" in resp.columns

    @patch("klondike.gcp.bigquery.bigquery.Client")
    @patch("polars.DataFrame.write_parquet")
    def test_write_dataframe(self, MockParquetStream, MockBigQueryClient):
        """
        Test for write_dataframe method
        """

        mock_bq_client = MagicMock()
        MockBigQueryClient.return_value = mock_bq_client

        # NOTE - We'll mock this to avoid anything being written in CI/CD
        mock_parquet_stream = MagicMock()
        MockParquetStream.return_value = mock_parquet_stream

        bq = BigQueryConnector(app_creds={"foo": "bar"})
        df = pl.DataFrame(
            [
                {"name": "Ian", "city": "Brooklyn"},
                {"name": "Spencer", "city": "Tysons"},
                {"name": "Gavin", "city": "Arlington"},
                {"name": "Zach", "city": "Harrisonburg"},
            ]
        )

        bq.write_dataframe(df=df, table_name="test.table")

        mock_bq_client.load_table_from_file.assert_called_once()


if __name__ == "__main__":
    unittest.main()
