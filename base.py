from abc import ABC, abstractmethod

class DataFetcher(ABC):
    """Abstract base for data sources (MySQL, CSV, API, ...)."""

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def get_table_list(self):
        pass

    @abstractmethod
    def get_table_structure(self, table_name):
        pass

    @abstractmethod
    def fetch_data_in_batch(self, table_name, offset, batch_size):
        pass

    @abstractmethod
    def get_total_rows(self, table_name):
        pass

    @abstractmethod
    def fetch_rows_by_ids(self, table_name, id_list, id_column="id"):
        pass


class DataWriter(ABC):
    """Abstract base for data targets (Postgres, Parquet, ...)."""

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def create_table(self, table_name, mysql_conn):
        """Create table in target using information from source (mysql_conn)."""
        pass

    @abstractmethod
    def insert_rows(self, df, table_name):
        pass

    @abstractmethod
    def update_sequence(self, cursor, table_name):
        pass
