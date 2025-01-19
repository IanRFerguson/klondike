import unittest
from unittest.mock import MagicMock, patch

import polars as pl

from klondike.snowflake.snowflake import SnowflakeConnector

##########

TEST_CREDENTIALS = {
    "snowflake_user": "hello",
    "snowflake_password": "world",
    "snowflake_account": "dev",
    "snowflake_database": "dev",
    "snowflake_warehouse": "dev",
}

MOCK_DATAFRAME_RESP = pl.DataFrame(
    [
        {"name": "Ian", "city": "Brooklyn"},
        {"name": "Spencer", "city": "Tysons"},
        {"name": "Gavin", "city": "Arlington"},
        {"name": "Zach", "city": "Harrisonburg"},
    ]
)


class TestSnowflakeConnector(unittest.TestCase):
    def test_init_snowflake(self):
        """
        Test for valid construction
        """

        sf = SnowflakeConnector(**TEST_CREDENTIALS)

        assert sf

    @patch("snowflake.connector.connect")
    @patch("polars.from_arrow")
    def test_query__valid_results(self, MockPolarsArrow, MockSnowflakeConnection):
        """
        Test for query response
        """

        MockPolarsArrow.return_value = MOCK_DATAFRAME_RESP

        mock_snowflake_connection = MagicMock()
        MockSnowflakeConnection.return_value = mock_snowflake_connection

        sf = SnowflakeConnector(**TEST_CREDENTIALS)
        resp = sf.query("select * from test.table;")

        mock_snowflake_connection.cursor.assert_called_once()

        assert isinstance(resp, pl.DataFrame)

    @patch("snowflake.connector.connect")
    @patch("polars.from_arrow")
    def test_query__no_results(self, MockPolarsArrow, MockSnowflakeConnection):
        """
        Test for query response with no results
        """

        mock_polars_arrow = MagicMock()
        MockPolarsArrow.return_value = mock_polars_arrow

        mock_snowflake_connection = MagicMock()
        MockSnowflakeConnection.return_value = mock_snowflake_connection

        sf = SnowflakeConnector(**TEST_CREDENTIALS)
        resp = sf.query("select * from test.table;")

        assert resp.is_empty()

    def test_read_dataframe__valid_results(self):
        pass

    def test_read_dataframe__no_results(self):
        pass

    def test_write_dataframe(self):
        pass

    def test_table_exists(self):
        pass

    def test_list_tables(self):
        pass
