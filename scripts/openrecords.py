from asyncio import get_event_loop
from os import listdir
from re import search
from time import sleep

from selenium.common import NoSuchElementException, TimeoutException
from selenium.webdriver.common.by import By

from scripts.queues import CONVERTER
from utils.publisher import Publisher
from utils.scraper import Scraper, RESOURCE, DOWNLOADS


async def main():
    # with OpenRecords(url='http://www.angelinacad.org/open-records') as worker:
    #     await worker.scrape()

    # with Excel(location='../resources/Appraisal_Export_Layout_-_8.0.25.xlsx') as reader:
    #     data = await reader.layout(start=('A', 55), end=('F', 491))
    #     data.rename({'Field Name': 'Field_Name'}, axis='columns', inplace=True)
    #     print(data.tail())

    # with SQLite(path='../appraisals.db') as sql:
    #     # await sql.drop_and_recreate(table='Payload', o=Payload())
    #     # await sql.drop_and_recreate(table='Blueprint', o=Blueprint())
    #
    #     # frame = await sql.dataframe("SELECT ID, Name, URL, Located FROM County c WHERE c.Name = 'Angelina';",
    #     #                             ['ID', 'Name', 'URL', 'Located'])
    #     #
    #     # for i, row in frame.iterrows():
    #     #
    #     #     payload = Payload()
    #     #     payload.CountyID = row['ID']
    #     #     payload.Name = 'APPRAISAL_INFO'
    #     #
    #     #     await sql.insert(table='Payload', o=payload)
    #
    #     # frame = await sql.dataframe("""SELECT p.ID, p.CountyID, p.Name FROM Payload p
    #     # WHERE p.CountyID = (SELECT ID FROM County c WHERE c.Name = 'Angelina');""",
    #     #                             ['ID', 'CountyID', 'Name'])
    #     #
    #     # for i, row in frame.iterrows():
    #     #     data.insert(1, 'PayloadID', row['ID'])
    #     #
    #     #     print(data.head())
    #     #
    #     #     await sql.bulk_insert_dataframe(table='Blueprint', frame=data)
    #
    #     blueprint = await sql.dataframe(sql=f'''SELECT
    #         Field_Name, Datatype, "Start", "End", "Length", Description
    #     FROM Blueprint b
    #     WHERE b.PayloadID = (
    #         SELECT p.ID FROM Payload p WHERE p.CountyID = (
    #             SELECT ID FROM County c WHERE c.Name = '{'Angelina'}'
    #         )
    #     );''')

    # with Mongo() as connection:
    #     database = connection['Appraisal']
    #     collection = database['Roll']

    # publisher = Publisher(name=CONVERTER)
    #
    # with open('../resources/2022-08-16_000973_APPRAISAL_INFO.TXT', 'r') as reader:
    #     for line in reader:
    #         publisher.publish(line)
            # sleep(0.05)

        # collection.insert_many([await insert(blueprint, line) for line in reader])

        # with Mongo() as connection:
        #     database = connection['Appraisal']
        #     collection = database['Roll']
        #     collection.insert_one()

        # with open('', 'r') as reader:
        #     for line in reader.readline():
        #         for i, row in data.iterrows():
        #             pass
        #
        # pass

        # county = await sql.dataframe()
    #
    # with ZipFile('../resources/2022-08-16_000973_APPRAISAL_INFO.zip') as archive:
    #     name = [x for x in archive.namelist() if search('_APPRAISAL_INFO', x)][0]
    #     archive.extract(member=name, path='../resources')

    print('Completed...')


async def insert(blueprint, line):
    map: dict = {}
    filler: int = 1

    for i, row in blueprint.iterrows():

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

    print(map)
    return map


# run(main())
get_event_loop().run_until_complete(main())
