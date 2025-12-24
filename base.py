from abc import ABC, abstractmethod


class MigrationManager(ABC):
    """Abstract base class for all migration types (MySQL->Postgres, CSV->Postgres, etc.)."""
    
    @abstractmethod
    def __init__(self, fetcher, writer, batch_size=10000, threads=4):
        """Initialize migration manager with source and target."""
        pass
    
    @abstractmethod
    def create_connections(self):
        """Create connections to source and target."""
        pass
    
    @abstractmethod
    def close_connections(self):
        """Close all connections."""
        pass
    
    @abstractmethod
    def create_tables(self):
        """Create all tables in target."""
        pass
    
    @abstractmethod
    def migrate_table(self, table_name):
        """Migrate single table."""
        pass
    
    @abstractmethod
    def migrate_all(self):
        """Migrate all tables."""
        pass


class DataFetcher(ABC):
    """Abstract base for data sources (MySQL, CSV, API, ...)."""

    @abstractmethod
    def connect(self):
        """Connect to data source and return connection object."""
        ...

    @abstractmethod
    def close(self):
        """Close connection to data source."""
        ...

    @abstractmethod
    def get_table_list(self):
        """Get list of all tables."""
        ...

    @abstractmethod
    def get_table_structure(self, table_name):
        """Get table structure (columns and indexes)."""
        ...

    @abstractmethod
    def fetch_data_in_batch(self, table_name, offset, batch_size):
        """Fetch batch of data from table."""
        ...

    @abstractmethod
    def get_total_rows(self, table_name):
        """Get total number of rows in table."""
        ...

    @abstractmethod
    def fetch_rows_by_ids(self, table_name, id_list, id_column="id"):
        """Fetch specific rows by their IDs."""
        ...


class DataWriter(ABC):
    """Abstract base for data targets (Postgres, Parquet, ...)."""

    @abstractmethod
    def connect(self):
        """Connect to data target and return connection object."""
        ...

    @abstractmethod
    def close(self):
        """Close connection to data target."""
        ...

    @abstractmethod
    def create_table(self, table_name, mysql_conn):
        """Create table in target using information from source (mysql_conn)."""
        ...

    @abstractmethod
    def insert_rows(self, df, table_name):
        """Insert rows from DataFrame into target table."""
        ...

    @abstractmethod
    def update_sequence(self, cursor, table_name):
        """Update primary key sequence after data migration."""
        ...
