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
            cursor.description = [(key, None) for key in query_results[0]]

        # Create a mock that will play the role of the connection
        connection = mock.MagicMock()
        connection.cursor.return_value = cursor

        # Create a mock that will play the role of our GoogleBigQuery client
        client = mock.MagicMock()

        bq = BigQueryConnector()
        bq.connection = connection
        bq._client = client

        return bq

    def test_read_dataframe_from_bigquery(self):
        "Tests read functionality for the `BigQueryConnector` object"

        sql = "select * from my_table"
        tbl = [
            {"city": "BROOKLYN", "state": "NY"},
            {"city": "SAN FRANCSICO", "state": "CA"},
            {"city": "RICHMOND", "state": "VA"},
        ]
        query_results = pa.RecordBatch.from_pylist(tbl)

        bq = self._build_mock_cursor(query_results=query_results)
        df = bq.read_dataframe_from_bigquery(sql=sql)

        assert isinstance(df, pl.DataFrame)

    def test_write_dataframe_to_bigquery(self):
        "Tests write functionality for the `BigQueryConnector` object"
        pass
