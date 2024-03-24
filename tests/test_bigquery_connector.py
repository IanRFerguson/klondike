import os
from unittest import mock

import polars as pl
import pyarrow as pa

from klondike import BigQueryConnector

from .test_utils import KlondikeTestCase

##########


class TestBigQuery(KlondikeTestCase):
    def setUp(self):
        super().setUp()
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self._credentials_path

    def tearDown(self):
        super().tearDown()
        del os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

    def _build_mock_cursor(self, query_results=None):
        cursor = mock.MagicMock()
        cursor.execute.return_value = None
        cursor.fetchmany.side_effect = [query_results, []]

        if query_results:
            cursor.description = query_results

        # Create a mock that will play the role of the connection
        connection = mock.MagicMock()
        connection.cursor.return_value = cursor

        # Create a mock that will play the role of our GoogleBigQuery client
        client = mock.MagicMock()

        bq = BigQueryConnector()
        bq._client = client

        return bq

    def test_read_dataframe_from_bigquery(self):
        # sql = "select * from my_table"
        # tbl = pa.table(
        #     {
        #         "city": ["Brooklyn", "San Francisco", "Richmond"],
        #         "state": ["New York", "California", "Virginia"],
        #     }
        # )

        # bq = self._build_mock_cursor(query_results=tbl)
        # df = bq.read_dataframe_from_bigquery(sql=sql)

        # assert isinstance(df, pl.DataFrame)
        pass

    def test_write_dataframe_to_bigquery(self):
        pass
