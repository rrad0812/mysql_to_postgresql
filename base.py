from abc import ABC, abstractmethod
from typing import Any, List, Tuple


class MigrationManager(ABC):
    """Abstract base class for all migration types (MySQL->Postgres, CSV->Postgres, etc.)."""
    
    @abstractmethod
    def __init__(self, fetcher, writer, batch_size=10000, threads=4):
        """Initialize migration manager with source and target."""
        pass
    
    @abstractmethod
    def create_connections(self) -> None:
        """Create connections to source and target."""
        pass
    
    @abstractmethod
    def close_connections(self) -> None:
        """Close all connections."""
        pass
    
    def __enter__(self):
        """Enter context manager - establish connections."""
        self.create_connections()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager - close all connections."""
        self.close_connections()
        return False
    
    @abstractmethod
    def create_tables(self) -> None:
        """Create all tables in target."""
        pass
    
    @abstractmethod
    def migrate_table(self, table_name: str) -> None:
        """Migrate single table."""
        pass
    
    @abstractmethod
    def migrate_all(self) -> None:
        """Migrate all tables."""
        pass


class DataFetcher(ABC):
    """Abstract base for data sources (MySQL, CSV, API, ...)."""

    @abstractmethod
    def connect(self) -> Any:
        """Connect to data source and return connection object."""
        ...

    @abstractmethod
    def close(self) -> None:
        """Close connection to data source."""
        ...

    def __enter__(self):
        """Enter context manager - establish connection."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager - close connection."""
        self.close()
        return False

    @abstractmethod
    def get_table_list(self) -> List[str]:
        """Get list of all tables."""
        ...

    @abstractmethod
    def get_table_structure(self, table_name: str) -> Tuple[Any, Any]:
        """Get table structure (columns and indexes)."""
        ...

    @abstractmethod
    def fetch_data_in_batch(self, table_name: str, offset: int, batch_size: int) -> List[Tuple[Any, ...]]:
        """Fetch batch of data from table."""
        ...

    @abstractmethod
    def get_total_rows(self, table_name: str) -> int:
        """Get total number of rows in table."""
        ...

    @abstractmethod
    def fetch_rows_by_ids(self, table_name: str, id_list: List[Any], id_column: str = "id") -> List[Tuple[Any, ...]]:
        """Fetch specific rows by their IDs."""
        ...


class DataWriter(ABC):
    """Abstract base for data targets (Postgres, Parquet, ...)."""

    @abstractmethod
    def connect(self) -> Any:
        """Connect to data target and return connection object."""
        ...

    @abstractmethod
    def close(self) -> None:
        """Close connection to data target."""
        ...

    def __enter__(self):
        """Enter context manager - establish connection."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager - close connection."""
        self.close()
        return False

    @abstractmethod
    def create_table(self, table_name: str, columns: Tuple[Any, Any], indexes: Tuple[Any, Any]) -> None:
        """Create table in target using provided structure (columns and indexes)."""
        ...

    @abstractmethod
    def insert_into_table(self, df: Any, table_name: str) -> None:
        """Insert rows from DataFrame into target table."""
        ...

    @abstractmethod
    def update_sequence(self, cursor: Any, table_name: str) -> None:
        """Update primary key sequence after data migration."""
        ...
