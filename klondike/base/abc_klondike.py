from abc import ABC, abstractmethod
from typing import Union

import polars as pl

##########


class KlondikeBaseDBConnector(ABC):
    """
    This is the abstract class that all Klondike Database
    connectors inherit from
    """

    @property
    @abstractmethod
    def dialect(self) -> str:
        pass

    @abstractmethod
    def query(self) -> Union[pl.DataFrame, None]:
        pass

    @abstractmethod
    def table_exists(self) -> bool:
        pass

    @abstractmethod
    def list_tables(self) -> list:
        pass

    @abstractmethod
    def read_dataframe(self) -> pl.DataFrame:
        pass

    @abstractmethod
    def write_dataframe(self) -> None:
        pass


class KlondikeBaseFlatFileConnector(ABC):
    @abstractmethod
    def list_buckets(self) -> list:
        pass

    @abstractmethod
    def list_blobs(self) -> list:
        pass

    @abstractmethod
    def get_blob(self) -> pl.DataFrame:
        pass

    @abstractmethod
    def put_blob(self) -> None:
        pass
