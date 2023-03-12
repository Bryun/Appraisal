import json
from asyncio import run
from pandas import DataFrame
from scripts.queues import CONVERTER, STORAGE
from utils.consumer import Consumer
from utils.publisher import Publisher
from utils.sqlite import SQLite
from multiprocessing import Process


class Converter(Consumer):

    def __init__(self, name: str, cache: DataFrame):
        self.blueprint = cache
        self.publisher = Publisher(name=STORAGE)
        super().__init__(name=name)

    def callback(self, ch, method, properties, body):
        payload = body.decode('utf-8')
        package = self.jsonify(payload)
        self.publisher.publish(json.dumps(package))
        super().callback(ch, method, properties, body)

    def jsonify(self, line: str):
        filler: int = 1
        map: dict = {}

        for i, row in self.blueprint.iterrows():

            key: str = row['Field_Name']
            start: int = row['Start'] - 1
            end: int = row['End']
            value = line[start:end]

            if row['Field_Name'] == 'filler':
                key = f"filler_{filler}"
                filler += 1

            if row['Datatype'].startswith('char'):
                map[key] = value.strip() if len(value.strip()) > 0 else None
            elif row['Datatype'].startswith('int'):
                map[key] = int(value) if value.isnumeric() else None
            elif row['Datatype'].startswith('numeric'):
                map[key] = int(value) if value.isnumeric() else None

        return map


async def main():
    with SQLite(path='../appraisals.db') as sql:
        blueprint = await sql.dataframe(sql=f'''
        SELECT
            Field_Name, Datatype, "Start", "End", "Length", Description
        FROM Blueprint b
        WHERE b.PayloadID = (
            SELECT p.ID FROM Payload p WHERE p.CountyID = (
                SELECT ID FROM County c WHERE c.Name = '{'Angelina'}'
            )
        );''')

    Converter(name=CONVERTER, cache=blueprint)


run(main())
