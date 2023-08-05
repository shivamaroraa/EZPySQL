import sqlite3
import pymysql
from config import config
import os

base_dir = os.path.dirname(os.path.abspath(__file__))
cert_path = os.path.join(base_dir, '..', 'cert.pem')


class Database:
    def __init__(self, db_type):
        self.db_type = db_type
        self.config = config[self.db_type]
        self._conn = self.connect()
        self._cursor = self._conn.cursor()

    def connect(self):
        if self.db_type == 'sqlite':
            conn = sqlite3.connect(self.config['database'])
            conn.row_factory = self._dict_factory

        else:
            conn = pymysql.connect(
                host=self.config['host'],
                password=self.config['password'],
                user=self.config['user'],
                database=self.config['database'],
                ssl_ca= cert_path,
                cursorclass=pymysql.cursors.DictCursor
            )
        return conn

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def connection(self):
        return self._conn

    @property
    def cursor(self):
        return self._cursor

    def commit(self):
        self.connection.commit()

    def close(self, commit=True):
        if commit:
            self.commit()
        self.connection.close()

    def execute(self, sql, params=None):
        self.cursor.execute(sql, params or ())

    def fetchall(self):
        return self.cursor.fetchall()

    def fetchone(self):
        return self.cursor.fetchone()

    def query(self, sql, params=None):
        self.cursor.execute(sql, params or ())
        return self.fetchall()

    def get_num_rows_effected(self):
        return self.cursor.rowcount

    def rollback(self):
        return self.connection.rollback()

    def executemany(self, sql, params_list):
        self.cursor.executemany(sql, params_list)

    @staticmethod
    def _dict_factory(cursor, row):
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d
