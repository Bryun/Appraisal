import sqlite3
from decimal import Decimal

from pandas import DataFrame


class SQLite:
    def __init__(self, path: str = '../appraisals.db'):
        self.connection = sqlite3.connect(path)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()

    async def bulk_insert(self, table: str, values: list[dict]) -> int:

        select: str = [f"SELECT {', '.join([f'{v} AS {k}' for k, v in row.items()])}" for row in values]
        sql: str = '\nUNION ALL '.join(select)
        sql = f"INSERT INTO {table} ({', '.join(values[0].keys())}) {sql}"

        cursor = self.connection.cursor()
        print(f"\n{sql}\n")
        cursor.execute(sql)
        self.connection.commit()
        return cursor.lastrowid

    async def bulk_insert_dataframe(self, table: str, frame: DataFrame) -> int:

        rows: list = []

        for i, row in frame.iterrows():
            columns: list = []

            for k, v in row.items():

                formatted: str = v.strip().replace("'", "''") if type(v) is str else v

                if formatted not in [None, '']:
                    formatted = f"'{formatted}'" if type(formatted) is not int else formatted
                else:
                    formatted = "NULL"

                columns.append(f"{formatted} AS {k.strip()}")

            rows.append(f"SELECT {', '.join(columns)}")

            if i % 499 == 0 or i == len(frame.index) - 1:
                sql: str = '\nUNION ALL '.join(rows)
                sql = f"INSERT INTO {table} ({', '.join(x.strip() for x in list(frame.columns))}) {sql}"

        cursor = self.connection.cursor()
        print(f"\n{sql}\n")
        cursor.execute(sql)
        self.connection.commit()
        rows.clear()

    async def select_builder(self, table: str, filter: dict) -> str:
        sql: str = f"SELECT ID FROM {table} WHERE {' AND '.join([f'{k} = {v}' for k, v in filter.items()])};"
        return sql

    async def select(self, sql: str) -> list[tuple]:
        cursor = self.connection.cursor()
        print(f"\n{sql}\n")
        cursor.execute(sql)
        rows = cursor.fetchall()
        return rows

    async def dataframe(self, sql: str, parameters: list = None) -> DataFrame:
        cursor = self.connection.cursor()
        print(f"\n{sql}\n")
        cursor.execute(sql, parameters)
        rows = cursor.fetchall()
        return DataFrame(rows, columns=list(map(lambda x: x[0], cursor.description)))

    async def drop_and_recreate(self, table: str, o: object):
        cursor = self.connection.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {table};")

        columns: list = []
        for k, v in o.__dict__.items():
            if type(v) is str:
                columns.append(f"{k} TEXT")
            elif type(v) is int:
                columns.append(f"{k} INTEGER")
            elif type(v) in [Decimal, bool]:
                columns.append(f"{k} NUMERIC")

        sql: str = f"CREATE TABLE {table} (ID INTEGER PRIMARY KEY AUTOINCREMENT,{', '.join(columns)});"

        cursor.execute(sql)
        self.connection.commit()

    async def insert(self, table: str, o: object) -> int:

        sql: str = f"INSERT INTO {table} ({', '.join(o.__dict__.keys())}) VALUES ({', '.join(['?' for k in o.__dict__.keys()])});"

        cursor = self.connection.cursor()
        parameters = tuple([v for v in o.__dict__.values()])
        cursor.execute(sql, parameters)
        self.connection.commit()
        return cursor.lastrowid

    # select: str = [f"SELECT {', '.join([f'{v} AS {k}' for k, v in row.items()])}" for row in values]
    # sql: str = '\nUNION ALL '.join(select)
    # sql = f"INSERT INTO {table} ({', '.join(values[0].keys())}) {sql}"
