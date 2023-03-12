from asyncio import run
from os import listdir, environ, path, remove
from re import search
from time import sleep
from selenium.common import NoSuchElementException, TimeoutException
from selenium.webdriver.common.by import By
from utils.excel import Excel
from utils.scraper import Scraper, DOWNLOADS
from dotenv import load_dotenv, find_dotenv
from utils.sqlite import SQLite

load_dotenv(find_dotenv())

YEAR = environ.get('YEAR')
COUNTY = 'Angelina'


class Angelina(Scraper):

    def __init__(self, url: str):
        super().__init__(url=url)
        self.url = url

    async def blueprint(self):
        try:
            with SQLite() as sql:
                blueprint = await sql.dataframe(sql='''
                SELECT 
                    ID, PayloadID, Field_Name, Datatype, "Start", "End", "Length", Description, CountyID 
                FROM Blueprint b 
                WHERE b.CountyID IN (
                    SELECT c.ID FROM County c 
                    WHERE c.Name = ?
                );''', parameters=[COUNTY])

            if len(blueprint.index) == 0:
                layout = self.driver.find_element(By.XPATH, "//a[contains(text(), 'Appraisal Export Layout')]")

                href = layout.get_attribute('href')
                file = search(r"[\w\-\.]+$", href)[0]
                properties = file.split('.')

                layout.click()

                while file not in listdir('../downloads'):
                    sleep(1)
                    print('waiting...')

                with Excel(location=f'../downloads/{file}') as reader:
                    data = await reader.layout(start=('A', 55), end=('F', 491))
                    data.rename({'Field Name': 'Field_Name'}, axis='columns', inplace=True)
                    print(data.tail())

                with SQLite() as sql:
                    await sql.bulk_insert_dataframe(table='Blueprint', frame=data)

                print('Blueprint loaded')

                remove(f'../downloads/{file}')

        except NoSuchElementException as e:
            print(str(e))
        except TimeoutException as e:
            print(str(e))
        except Exception as e:
            print(str(e))

    async def appraisal(self):
        try:
            layout = self.driver.find_element(By.XPATH, f"//a[contains(text(), '{YEAR} Appraisal Roll Data Export')]")

            href = layout.get_attribute('href')
            file = search(r"[\w\-\.]+$", href)[0]
            properties = file.split('.')

            layout.click()

            sleep(2)

            while file not in listdir(DOWNLOADS):
                sleep(1)
                print('waiting...')

            # while path.exists(f"{RESOURCE}/{properties[0]}.crdownload"):
            #     sleep(1)
            #     print('waiting...')

        except NoSuchElementException as e:
            print(str(e))
        except TimeoutException as e:
            print(str(e))
        except Exception as e:
            print(str(e))


async def main():
    with SQLite() as sql:
        county = await sql.dataframe(sql='''
        SELECT 
            ID, Name, URL, Located, Script 
        FROM County c 
        WHERE Name = ?;
        ''', parameters=[COUNTY])

    with Angelina(url=county['URL'][0]) as worker:
        await worker.blueprint()

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
    #         # sleep(0.05)

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

    return map


run(main())
