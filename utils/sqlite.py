import sqlite3
from decimal import Decimal
from json import dumps

from pandas import DataFrame


class SQLite:
    def __init__(self, path: str = "../appraisals.db"):
        self.connection = sqlite3.connect(path)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()

    async def bulk_insert(self, table: str, values: list[dict]) -> int:
        select = [
            "SELECT {}".format(
                ", ".join(
                    [
                        "NULL AS {0}".format(k)
                        if v is None
                        else "'{0}' AS {1}".format(str(dumps(v)), k)
                        if type(v) is dict
                        else "{0} AS {1}".format(v, k)
                        if type(v) is int
                        else "{0} AS {1}".format(v, k)
                        if v.isnumeric()
                        else "'{0}' AS {1}".format(v, k)
                        for k, v in row.items()
                    ]
                )
            )
            for row in values
        ]
        sql: str = "\nUNION ALL ".join(select)
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

                if formatted not in [None, ""]:
                    formatted = (
                        f"'{formatted}'" if type(formatted) is not int else formatted
                    )
                else:
                    formatted = "NULL"

                columns.append(f"{formatted} AS {k.strip()}")

            rows.append(f"SELECT {', '.join(columns)}")

            if i % 499 == 0 or i == len(frame.index) - 1:
                sql: str = "\nUNION ALL ".join(rows)
                sql = f"INSERT INTO {table} ({', '.join(x.strip() for x in list(frame.columns))}) {sql}"

        cursor = self.connection.cursor()
        print(f"\n{sql}\n")
        cursor.execute(sql)
        self.connection.commit()
        rows.clear()

    async def select_builder(self, table: str, filter: dict) -> str:
        sql: str = f"SELECT ID FROM {table} WHERE {' AND '.join([f'{k} = {v}' for k, v in filter.items()])};"
        return sql

    async def select(self, sql: str, parameters: list) -> tuple:
        cursor = self.connection.cursor()
        cursor.execute(sql, parameters)
        row = cursor.fetchone()
        return row

    async def select_all(self, sql: str) -> list[tuple]:
        cursor = self.connection.cursor()
        print(f"\n{sql}\n")
        cursor.execute(sql)
        rows = cursor.fetchall()
        return rows

    async def exists(self, sql: str) -> bool:
        cursor = self.connection.cursor()
        print(f"\n{sql}\n")
        cursor.execute(sql)
        row = cursor.fetchone()
        return row[0] > 0

    async def dataframe(self, sql: str, parameters: list = None) -> DataFrame:
        cursor = self.connection.cursor()
        print(f"\n{sql}\n")
        if parameters is None:
            cursor.execute(sql)
        else:
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

    async def insert(self, sql: str, parameters: list) -> int:
        cursor = self.connection.cursor()
        cursor.execute(sql, parameters)
        self.connection.commit()
        return cursor.lastrowid

    async def query(self, sql: str) -> int:
        cursor = self.connection.cursor()
        print(f"\n{sql}\n")
        cursor.execute(sql)
        self.connection.commit()
        return cursor.lastrowid

    async def select_by_parameter(self, sql: str, parameters: list) -> DataFrame:
        cursor = self.connection.cursor()
        cursor.execute(sql, parameters)
        rows = cursor.fetchall()
        return DataFrame(rows, columns=list(map(lambda x: x[0], cursor.description)))
