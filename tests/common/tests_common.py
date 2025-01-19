import unittest
from abc import ABC, abstractmethod

import polars as pl

##########

MOCK_DATAFRAME_RESP = pl.DataFrame(
    [
        {"name": "Ian", "city": "Brooklyn"},
        {"name": "Spencer", "city": "Tysons"},
        {"name": "Gavin", "city": "Arlington"},
        {"name": "Zach", "city": "Harrisonburg"},
    ]
)


class TestFramework(unittest.TestCase, ABC):
    @abstractmethod
    def test_init(self):
        pass

    @abstractmethod
    def test_table_exists(self):
        pass

    @abstractmethod
    def test_list_tables(self):
        pass

    @abstractmethod
    def test_query__no_results(self):
        pass

    @abstractmethod
    def test_query__valid_results(self):
        pass

    @abstractmethod
    def test_read_dataframe__no_results(self):
        pass

    @abstractmethod
    def test_read_dataframe__valid_results(self):
        pass

    @abstractmethod
    def test_write_dataframe(self):
        pass
