from asyncio import run
from os import listdir
from re import search
import re
from time import sleep
from selenium.webdriver.common.by import By
from scrapers.icounty import ICounty
from utils.excel import Excel
from utils.scraper import Scraper
from utils.sqlite import SQLite
from utils.timer import Timer
from zipfile import ZipFile


class Angelina(ICounty):
    def __init__(self):
        super().__init__()
        self.COUNTY: str = "Angelina"
        self.URL: str = ""
        self.DOWNLOADS_DIR = f"D:/Forge/WorkBench/Appraisal/downloads/{self.COUNTY}/"

        self.BLUEPRINT_DIR: str = f"{self.DOWNLOADS_DIR}/blueprint/"
        self.LAYOUT_ZIP = ""

        self.DATA_DIR: str = f"{self.DOWNLOADS_DIR}/data/"
        self.APPRAISAL_ROLL = ""

    async def has_blueprint(self) -> bool:
        with SQLite(path="../appraisals.db") as sql:
            return await sql.exists(
                f"SELECT COUNT(*) AS Layout FROM Blueprint b INNER JOIN County c ON b.CountyID = c.ID WHERE c.Name = '{self.COUNTY}';"
            )

    async def download_blueprint(self):
        with SQLite(path="../appraisals.db") as sql:
            row = await sql.select(
                f"SELECT URL FROM County c WHERE c.Name = '{self.COUNTY}';"
            )
            self.URL = row[0]

        if self.URL is not None:
            with Scraper(url=self.URL, download_dir=self.BLUEPRINT_DIR) as browser:
                layout = browser.driver.find_element(
                    By.PARTIAL_LINK_TEXT, "Appraisal Export Layout"
                )

                href = layout.get_attribute("href")
                self.LAYOUT_ZIP = search(r"[\w\-.]+$", href)[0]
                clock = Timer()

                layout.click()
                clock.start()

                while self.LAYOUT_ZIP not in listdir(self.BLUEPRINT_DIR):
                    sleep(5)
                    clock.display()

                clock.stop()

    async def read_blueprint(self):
        with ZipFile(f"{self.BLUEPRINT_DIR}{self.LAYOUT_ZIP}") as archive:
            files = [
                x
                for x in archive.namelist()
                if search(re.compile("^Appraisal Export Layout"), x)
            ]

            for file in files:
                archive.extract(member=file, path=self.BLUEPRINT_DIR)

        with Excel(
            location=f"{self.BLUEPRINT_DIR}\\Appraisal Export Layout - 8.0.26.xlsx"
        ) as reader:
            data = await reader.read(
                name="PACS File Layout", start=("A", 55), end=("F", 490)
            )

    async def save_blueprint(self, data):
        with SQLite(path="../appraisals.db") as db:
            for i in data.index:
                sql = f"""
                INSERT INTO Blueprint (Field_Name, Datatype, 'Start', 'End', 'Length', Description, CountyID) 
                VALUES
                (
                    {data['Field Name'][i]}, {data['Datatype'][i]}, {data['Start'][i]}, {data['End'][i]}, 
                    {data['Length'][i]}, {data['Description'][i]}, 
                    (SELECT ID FROM County WHERE Name = '{self.COUNTY}')
                );"""
                await db.query(sql)

    async def download_data(self):
        with SQLite(path="../appraisals.db") as sql:
            row = await sql.select(
                f"SELECT URL FROM County c WHERE c.Name = '{self.COUNTY}';"
            )
            self.URL = row[0]

        if self.URL is not None:
            with Scraper(url=self.URL, download_dir=self.DATA_DIR) as browser:
                layout = browser.driver.find_element(
                    By.LINK_TEXT, "2022 Appraisal Roll Data Export"
                )

                href = layout.get_attribute("href")
                self.APPRAISAL_ROLL = search(r"[\w\-.]+$", href)[0]
                clock = Timer()

                layout.click()
                clock.start()

                while self.APPRAISAL_ROLL not in listdir(self.DATA_DIR):
                    sleep(5)
                    clock.display()

                clock.stop()

    async def read_data(self):
        # with ZipFile(
        #     f"{self.BLUEPRINT_DIR}\\Appraisal_Export_Layout_-_8.0.26.zip"
        # ) as archive:
        #     files = [
        #         x
        #         for x in archive.namelist()
        #         if search(re.compile("^Appraisal Export Layout"), x)
        #     ]
        #
        #     for file in files:
        #         archive.extract(member=file, path=self.BLUEPRINT_DIR)
        #
        # with Excel(
        #     location=f"{self.BLUEPRINT_DIR}\\Appraisal Export Layout - 8.0.26.xlsx"
        # ) as reader:
        #     data = await reader.read(
        #         name="PACS File Layout", start=("A", 55), end=("F", 490)
        #     )
        pass

    async def save_date(self):
        pass


# TODO: Complete layout data insert.
x = Angelina()
run(x.read_blueprint())
