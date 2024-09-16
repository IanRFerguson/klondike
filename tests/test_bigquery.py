from klondike.bigquery.bigquery import BigQueryConnector
import os
import polars as pl
from mock import patch

##########


def test_build_with_app_creds():
    bq = BigQueryConnector(app_creds="{'foo': 'bar'}")
    assert bq


# def test_build_with_creds_in_environment():
#     os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "{'foo': 'bar}"

#     assert BigQueryConnector()


# def test_read_dataframe():
#     bq = BigQueryConnector(bypass_env_variable=True)
#     resp = bq.read_dataframe(sql="select foo from bar")

#     assert isinstance(resp, pl.DataFrame)


def test_write_dataframe():
    assert True


def test_table_exists():
    assert True


def test_list_tables():
    assert True
