import json
import os
import tempfile
import unittest
from unittest import mock

import polars as pl

from klondike import BigQueryConnector

##########


class KlondikeTestCase(unittest.TestCase):
    def setUp(self):
        self._temp_directory = tempfile.TemporaryDirectory()

        self._credentials_path = os.path.join(
            self._temp_directory.name, "service_account.json"
        )

        self._service_account = {
            "type": "foo",
            "project_id": "bar",
            "private_key_id": "biz",
            "private_key": "bap",
            "client_email": "bim",
            "client_id": "top",
            "auth_uri": "hat",
            "token_uri": "tap",
            "auth_provider_x509_cert_url": "dance",
            "client_x509_cert_url": "good",
            "universe_domain": "stuff",
        }

        with open(self._credentials_path, "w") as f:
            json.dump(self._service_account, f)

    def tearDown(self):
        self._temp_directory.cleanup()


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

        if query_results is not None:
            cursor.description = query_results

        # Create a mock that will play the role of the connection
        connection = mock.MagicMock()
        connection.cursor.return_value = cursor

        # Create a mock that will play the role of our GoogleBigQuery client
        client = mock.MagicMock()

        bq = BigQueryConnector()
        bq.connection = connection
        bq._client = client

        return bq

    @mock.patch("polars.from_arrow")
    def test_read_dataframe_from_bigquery(self, mock_from_arrow):
        "Tests read functionality for the `BigQueryConnector` object"

        sql = "select * from my_table"
        tbl = pl.DataFrame(
            [
                {"city": "Brooklyn", "state": "New York"},
                {"city": "San Francisco", "state": "California"},
                {"city": "Richmond", "state": "Virginia"},
            ]
        )

        bq = self._build_mock_cursor(query_results=tbl)
        df = bq.read_dataframe_from_bigquery(sql=sql)

        assert isinstance(df, type(None))

    @mock.patch("polars.DataFrame.write_parquet")
    def test_write_dataframe_to_bigquery(self, mock_write_parquet):
        "Tests write functionality for the `BigQueryConnector` object"

        df = mock.MagicMock()
        table_name = "foo.bar"

        bq = self._build_mock_cursor()
        bq.write_dataframe_to_bigquery(df=df, table_name=table_name)
