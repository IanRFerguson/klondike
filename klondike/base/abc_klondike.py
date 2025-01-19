from abc import ABC, abstractmethod

##########


class KlondikeBaseDBConnector(ABC):
    @abstractmethod
    def query(self):
        pass

    @abstractmethod
    def table_exists(self):
        pass

    @abstractmethod
    def list_tables(self):
        pass

    @abstractmethod
    def read_dataframe(self):
        pass

    @abstractmethod
    def write_dataframe(self):
        pass


class KlondikeBaseFlatFileConnector(ABC):
    pass
