from base import DataWriter
import mysql_to_postgresql as core

class PostgresWriter(DataWriter):
    def __init__(self):
        self.conn = None

    def connect(self):
        self.conn = core.create_postgres_connection()
        return self.conn

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def create_table(self, table_name, mysql_conn):
        # Delegate to existing function which already creates the table
        return core.create_postgres_table(self.conn, mysql_conn, table_name)

    def insert_rows(self, df, table_name):
        return core.insert_into_postgres(df, self.conn, table_name)

    def update_sequence(self, cursor, table_name):
        return core.update_sequence(cursor, table_name)
