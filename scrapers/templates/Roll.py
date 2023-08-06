from os import makedirs, listdir
from os.path import exists
from re import search
from time import sleep
from selenium.webdriver.common.by import By
from scrapers.icounty import ICounty
from utils.scraper import Scraper
from utils.sqlite import SQLite
from utils.timer import Timer


class Roll(ICounty):
    def __init__(self, county="Roll"):
        super().__init__()
        self.COUNTY: str = county
        self.DATA_DIR: str = f"D:/Forge/WorkBench/Appraisal/downloads/{self.COUNTY}/"

    async def download_data(self):
        with SQLite(path="../appraisals.db") as sql:
            row = await sql.select(
                "SELECT URL FROM County c WHERE c.Name = ?;", [self.COUNTY]
            )
            self.URL = row[0]

        if self.URL is not None:
            with Scraper(url=self.URL, download_dir=self.DATA_DIR) as browser:
                element = browser.driver.find_element(
                    By.XPATH,
                    "//a[contains(text(), '2022 Mineral Appraisal Roll (Excel)')]",
                )

            browser.driver.execute_script("arguments[0].scrollIntoView();", element)

            href = element.get_attribute("href")
            self.FOLDER = search(r"[0-9A-Z\-.zip]+", href)
            clock = Timer()

            element.click()
            clock.start()

            if not exists(self.DATA_DIR):
                makedirs(self.DATA_DIR)

            while self.FOLDER not in listdir(self.DATA_DIR):
                sleep(5)
                clock.display()

            clock.stop()
