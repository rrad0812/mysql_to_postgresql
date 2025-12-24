from base import DataFetcher
import mysql_to_postgresql as core

class MySQLFetcher(DataFetcher):
    def __init__(self):
        self.conn = None

    def connect(self):
        self.conn = core.create_mysql_connection()
        return self.conn

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def get_table_list(self):
        return core.get_table_list(self.conn)

    def get_table_structure(self, table_name):
        return core.get_table_structure(self.conn, table_name)

    def fetch_data_in_batch(self, table_name, offset, batch_size):
        return core.fetch_data_in_batch(self.conn, table_name, offset, batch_size)

    def get_total_rows(self, table_name):
        return core.get_total_rows(self.conn, table_name)

    def fetch_rows_by_ids(self, table_name, id_list, id_column="id"):
        return core.fetch_missing_rows(self.conn, table_name, id_list, id_column)
