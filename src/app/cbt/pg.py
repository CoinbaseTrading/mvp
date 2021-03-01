import logging

import psycopg2


class PostgresClient:

    def __init__(self, host, database, user, password, port=25060):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port

    def _get_connection(self):
        return psycopg2.connect(
            host=self.host, database=self.database, user=self.user, password=self.password, port=self.port
        )

    def copy_from(self, file, table, sep="|"):

        logging.info(f"Copying data from {file} to {table}...")
        conn = self._get_connection()
        with open(file) as f:
            with conn:
                with conn.cursor() as curs:
                    curs.copy_from(f, table, sep=sep)

    def execute_sql(self, sql_file):

        logging.info(f"Executing {sql_file}...")
        conn = self._get_connection()
        with open(sql_file) as f:
            with conn:
                with conn.cursor() as curs:
                    curs.execute(f.read())
