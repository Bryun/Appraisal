from asyncio import run
from os import listdir, makedirs
from os.path import exists
from re import search
import re
from time import sleep
from pandas import DataFrame
from selenium.webdriver.common.by import By
from scrapers.icounty import ICounty
from utils.excel import Excel
from utils.mongo import Mongo
from utils.pdf import PDF
from utils.scraper import Scraper
from utils.sqlite import SQLite
from utils.timer import Timer
from zipfile import ZipFile
from json import dumps, loads
from multiprocessing import Process


class Gonzales(ICounty):
    def __init__(self):
        super().__init__()
        self.COUNTY: str = "Gonzales"
        self.URL: str = ""
        self.DOWNLOADS_DIR = f""

        self.BLUEPRINT_DIR: str = f"{self.DOWNLOADS_DIR}blueprint/"
        self.LAYOUT_ZIP = ""

        self.DATA_DIR: str = f"D:/Forge/WorkBench/Appraisal/downloads/{self.COUNTY}/"
        self.FOLDER = ""

    async def download_data(self):
        with SQLite(path="../appraisals.db") as sql:
            row = await sql.select(
                f"SELECT URL FROM County c WHERE c.Name = '{self.COUNTY}';"
            )
            self.URL = row[0]

        if self.URL is not None:
            with Scraper(url=self.URL, download_dir=self.DATA_DIR) as browser:
                element = browser.driver.find_element(
                    By.XPATH,
                    "//h4[contains(text(), '2022 Certified Appraisal Rolls')]",
                )

                browser.driver.execute_script("arguments[0].scrollIntoView();", element)

                # browser.actions.move_to_element(element).perform()

                # TODO: Get element call could be more elegant
                package = element.find_elements(
                    By.XPATH, "./ancestor::div/following-sibling::div/ul/li/a"
                )[3]

                href = package.get_attribute("href")
                self.FOLDER = search(r"(?:fileName=)([0-9A-Z+._zip]+)", href).group(1)
                clock = Timer()

                package.click()
                clock.start()

                if not exists(self.DATA_DIR):
                    makedirs(self.DATA_DIR)

                while self.FOLDER not in listdir(self.DATA_DIR):
                    sleep(5)
                    clock.display()

                clock.stop()

    async def extract_files(self):
        with ZipFile(
            f"{self.DATA_DIR}1674763628_GONZALES CAD 2022 CERTIFIED MINERAL OPEN RECORDS.zip"
        ) as archive:
            files = [
                x
                for x in archive.namelist()
                if search(
                    re.compile(r"MINERAL DATA FLAT TEXT FILE|LAYOUT - TDMO19U"),
                    x,
                )
            ]

            for file in files:
                archive.extract(member=file, path=self.DATA_DIR)

    async def read_blueprint(self):
        content = PDF(
            f"{self.DATA_DIR}GONZALES CAD 2022 CERTIFIED MINERAL OPEN RECORDS/2. LAYOUT - TDMO19U.PDF"
        ).read()
        print(dumps(content, indent=4))

        headers = ["BEG", "END", "DESCRIPTION", "LENGTH", "DEC", "A/N/P"]

        rows = [
            (1, 1, "FILLER BLK OF A/C/D", 1, None, "A"),
            (2, 8, "JOB NUMBER", 7, 0, "N"),
            (9, 12, "YEAR", 4, 0, "S"),
            (13, 13, "FILLER", 1, None, "A"),
            (14, 14, "RENDERED CODE", 1, None, "A"),
            (15, 15, "FILLER", 1, None, "A"),
            (16, 16, "TYPE PROPERTY", 1, None, "A"),
            (17, 17, "INTEREST TYPE", 1, 0, "N"),
            (18, 21, "YEAR LEASE STARTED", 4, 0, "N"),
            (22, 22, "PROTEST=P", 1, None, "A"),
            (23, 25, "TEA CODE", 3, None, "A"),
            (26, 28, "AGENT NUMBER", 3, 0, "N"),
            (29, 29, "SORT CODE", 1, None, "A"),
            (30, 33, "FILLER", 4, None, "A"),
            (34, 40, "DECIMAL INTEREST", 7, 6, "N"),
            (41, 70, "LEASE NAME(DESC 3) IF TYPE=2,3", 30, None, "A"),
            (71, 90, "OPER NAME (DESC 4)", 20, None, "A"),
            (91, 120, "DESCRIPTION 1", 30, None, "A"),
            (121, 150, "DESCRIPTION 2", 30, None, "A"),
            (151, 180, "OWNER NAME", 30, None, "A"),
            (181, 210, "IN CARE OF", 30, None, "A"),
            (211, 240, "STREET ADDRESS", 30, None, "A"),
            (241, 270, "CITY,ST,ZIP CDE OR OUT COUNTRY", 30, None, "A"),
            (271, 282, "RRC#", 12, None, "A"),
            (283, 290, "FILLER", 8, None, "A"),
            (291, 295, "DV AMOUNT", 5, 0, "S"),
            (296, 296, "COMMUNICATION CODE", 1, None, "A"),
            (297, 298, "JURISDICTION 1", 2, 0, "N"),
            (299, 300, "JURISDICTION 2", 2, 0, "N"),
            (301, 302, "JURISDICTION 3", 2, 0, "N"),
            (303, 304, "JURISDICTION 4", 2, 0, "N"),
            (305, 306, "JURISDICTION 5", 2, 0, "N"),
            (307, 308, "JURISDICTION 6", 2, 0, "N"),
            (309, 310, "JURISDICTION 7", 2, 0, "N"),
            (311, 312, "JURISDICTION 8", 2, 0, "N"),
            (313, 314, "JURISDICTION 9", 2, 0, "N"),
            (315, 316, "JURISDICTION 10", 2, 0, "N"),
            (317, 318, "JURISDICTION 11", 2, 0, "N"),
            (319, 320, "JURISDICTION 12", 2, 0, "N"),
            (321, 331, "TAXABLE VALUE-JURISDICTION 1", 11, 0, "N"),
            (332, 342, "TAXABLE VALUE-JURISDICTION 2", 11, 0, "N"),
            (343, 353, "TAXABLE VALUE-JURISDICTION 3", 11, 0, "N"),
            (354, 364, "TAXABLE VALUE-JURISDICTION 4", 11, 0, "N"),
            (365, 375, "TAXABLE VALUE-JURISDICTION 5", 11, 0, "N"),
            (376, 386, "TAXABLE VALUE-JURISDICTION 6", 11, 0, "N"),
            (387, 397, "TAXABLE VALUE-JURISDICTION 7", 11, 0, "N"),
            (398, 408, "TAXABLE VALUE-JURISDICTION 8", 11, 0, "N"),
            (409, 419, "TAXABLE VALUE-JURISDICTION 9", 11, 0, "N"),
            (420, 430, "TAXABLE VALUE-JURISDICTION 10", 11, 0, "N"),
            (431, 441, "TAXABLE VALUE-JURISDICTION 11", 11, 0, "N"),
            (442, 452, "TAXABLE VALUE-JURISDICTION 12", 11, 0, "N"),
            (453, 463, "MARKET VALUE-JURISDICTION 1", 11, 0, "N"),
            (464, 474, "MARKET VALUE-JURISDICTION 2", 11, 0, "N"),
            (475, 485, "MARKET VALUE-JURISDICTION 3", 11, 0, "N"),
            (486, 496, "MARKET VALUE-JURISDICTION 4", 11, 0, "N"),
            (497, 507, "MARKET VALUE-JURISDICTION 5", 11, 0, "N"),
            (508, 518, "MARKET VALUE-JURISDICTION 6", 11, 0, "N"),
            (519, 529, "MARKET VALUE-JURISDICTION 7", 11, 0, "N"),
            (530, 540, "MARKET VALUE-JURISDICTION 8", 11, 0, "N"),
            (541, 551, "MARKET VALUE-JURISDICTION 9", 11, 0, "N"),
            (552, 562, "MARKET VALUE-JURISDICTION 10", 11, 0, "N"),
            (563, 573, "MARKET VALUE-JURISDICTION 11", 11, 0, "N"),
            (574, 584, "MARKET VALUE-JURISDICTION 12", 11, 0, "N"),
            (585, 593, "ACRES", 9, 3, "N"),
            (594, 600, "OWNER NUMBER", 7, 0, "N"),
            (601, 607, "LEASE NUMBER", 7, 0, "N"),
            (608, 608, "FILLER", 1, None, "A"),
            (609, 609, "ABSOLUTE EXEMPTION CODE", 1, None, "A"),
            (610, 610, "EXEMPTION/MIN OWNER FLAG JUR 1", 1, None, "A"),
            (611, 611, "EXEMPTION/MIN OWNER FLAG JUR 2", 1, None, "A"),
            (612, 612, "EXEMPTION/MIN OWNER FLAG JUR 3", 1, None, "A"),
            (613, 613, "EXEMPTION/MIN OWNER FLAG JUR 4", 1, None, "A"),
            (614, 614, "EXEMPTION/MIN OWNER FLAG JUR 5", 1, None, "A"),
            (615, 615, "EXEMPTION/MIN OWNER FLAG JUR 6", 1, None, "A"),
            (616, 616, "EXEMPTION/MIN OWNER FLAG JUR 7", 1, None, "A"),
            (617, 617, "EXEMPTION/MIN OWNER FLAG JUR 8", 1, None, "A"),
            (618, 618, "EXEMPTION/MIN OWNER FLAG JUR 9", 1, None, "A"),
            (619, 619, "EXEMPTION/MIN OWNER FLAG JUR10", 1, None, "A"),
            (620, 620, "EXEMPTION/MIN OWNER FLAG JUR11", 1, None, "A"),
            (621, 621, "EXEMPTION/MIN OWNER FLAG JUR12", 1, None, "A"),
            (622, 646, "CUSTOMER GEO#", 25, None, "A"),
            (647, 655, "TCEQ VALUE-POLLUTION CONTROL", 9, 0, "N"),
            (656, 656, "P&A MINIMUM OWNER FLAG JUR 1", 1, None, "A"),
            (657, 657, "P&A MINIMUM OWNER FLAG JUR 2", 1, None, "A"),
            (658, 658, "P&A MINIMUM OWNER FLAG JUR 3", 1, None, "A"),
            (659, 659, "P&A MINIMUM OWNER FLAG JUR 4", 1, None, "A"),
            (660, 660, "P&A MINIMUM OWNER FLAG JUR 5", 1, None, "A"),
            (661, 661, "P&A MINIMUM OWNER FLAG JUR 6", 1, None, "A"),
            (662, 662, "P&A MINIMUM OWNER FLAG JUR 7", 1, None, "A"),
            (663, 663, "P&A MINIMUM OWNER FLAG JUR 8", 1, None, "A"),
            (664, 664, "P&A MINIMUM OWNER FLAG JUR 9", 1, None, "A"),
            (665, 665, "P&A MINIMUM OWNER FLAG JUR 10", 1, None, "A"),
            (666, 666, "P&A MINIMUM OWNER FLAG JUR 11", 1, None, "A"),
            (667, 667, "P&A MINIMUM OWNER FLAG JUR 12", 1, None, "A"),
            (668, 674, "MINERAL ACCOUNT NUMBER", 7, None, "S"),
            (675, 681, "MINERAL ACCOUNT SEQUENCE #", 7, None, "S"),
            (682, 688, "PREVIOUS MINERAL ACCOUNT SEQ#", 7, None, "S"),
            (689, 695, "PREVIOUS MINERAL ACCOUNT #", 7, None, "S"),
            (696, 696, "PRIVACY CODE", 1, None, "A"),
            (697, 698, "COMPLIANCE CODE", 2, None, "A"),
            (699, 709, "TAXABLE VALUE NEW JUR01", 11, None, "S"),
            (710, 720, "TAXABLE VALUE NEW JUR02", 11, None, "S"),
            (721, 731, "TAXABLE VALUE NEW JUR03", 11, None, "S"),
            (732, 742, "TAXABLE VALUE NEW JUR04", 11, None, "S"),
            (743, 753, "TAXBALE VALUE NEW JUR05", 11, None, "S"),
            (754, 764, "TAXABLE VALUE NEW JUR06", 11, None, "S"),
            (765, 775, "TAXABLE VALUE NEW JUR07", 11, None, "S"),
            (776, 786, "TAXABLE VALUE NEW JUR08", 11, None, "S"),
            (787, 797, "TAXABLE VALUE NEW JUR09", 11, None, "S"),
            (798, 808, "TAXABLE VALUE NEW JUR10", 11, None, "S"),
            (809, 819, "TAXABLE VALUE NEW JUR11", 11, None, "S"),
            (820, 830, "TAXABLE VALUE NEW JUR12", 11, None, "S"),
            (831, 841, "ABT 1 NEW ABT VALUE", 11, None, "S"),
            (842, 852, "ABT 2 NEW ABT VALUE", 11, None, "S"),
            (853, 863, "ABT 3 NEW ABT VALUE", 11, None, "S"),
            (864, 874, "ABT 4 NEW ABT VALUE", 11, None, "S"),
            (875, 885, "ABT 5 NEW ABR VALUE", 11, None, "S"),
            (886, 896, "ABT 6 NEW ABT VALUE", 11, None, "S"),
            (897, 907, "ABT 7 NEW ABT VALUE", 11, None, "S"),
            (908, 918, "ABT 8 NEW ABT VALUE", 11, None, "S"),
            (919, 929, "ABT 9 NEW ABT VALUE", 11, None, "S"),
            (930, 940, "ABT 10 NEW ABT VALUE", 11, None, "S"),
            (941, 951, "ABT 11 NEW ABT VALUE", 11, None, "S"),
            (952, 962, "ABT 12 NEW ABT VALUE", 11, None, "S"),
            (963, 964, "NEW TCEQ FLAG Y/N", 2, None, "A"),
            (965, 975, "NEW EXEMPT VALUE JUR 1", 11, None, "S"),
            (976, 986, "NEW EXEMPT VALUE JUR 2", 11, None, "S"),
            (987, 997, "NEW EXEMPT VALUE JUR 3", 11, None, "S"),
            (998, 1008, "NEW EXEMPT VALUE JUR 4", 11, None, "S"),
            (1009, 1019, "NEW EXEMPT VALUE JUR 5", 11, None, "S"),
            (1020, 1030, "NEW EXEMPT VALUE JUR 6", 11, None, "S"),
            (1031, 1041, "NEW EXEMPT VALUE JUR 7", 11, None, "S"),
            (1042, 1052, "NEW EXEMPT VALUE JUR 8", 11, None, "S"),
            (1053, 1063, "NEW EXEMPT VALUE JUR 9", 11, None, "S"),
            (1064, 1074, "NEW EXEMPT VALUE JUR 10", 11, None, "S"),
            (1075, 1085, "NEW EXEMPT VALUE JUR 11", 11, None, "S"),
            (1086, 1096, "NEW EXEMPT VALUE JUR 12", 11, None, "S"),
            (1097, 1099, "NEW IMPROVEMENT %", 3, 2, "S"),
            (1100, 1100, "EOF -", 1, None, "A"),
        ]

        return DataFrame(rows, columns=headers)

    async def save_blueprint(self, data):
        with SQLite(path="../appraisals.db") as db:
            for i in data.index:
                map = {"DEC": data["DEC"][i], "A/N/P": data["A/N/P"][i]}

                sql = f""" INSERT INTO Blueprint (Field_Name, Datatype, 'Start', 'End', 'Length', Description, 
                CountyID, Metadata) VALUES (?, ?, ?, ?, ?, ?, (SELECT ID FROM County WHERE Name = ?), ?);"""
                await db.insert(
                    sql,
                    [
                        data["DESCRIPTION"][i],
                        None,
                        int(data["BEG"][i]),
                        int(data["END"][i]),
                        int(data["LENGTH"][i]),
                        data["DESCRIPTION"][i],
                        self.COUNTY,
                        str(map),
                    ],
                )

    async def read_data(self, path: str):
        blueprint = await self.get_blueprint()

        with open(path, "r") as reader:
            lines = reader.readlines()

        clock = Timer()
        clock.start()

        for i in range(len(lines)):
            print(f"{i+1} / {len(lines)}")
            lines[i] = await self.jsonify(lines[i], blueprint)
            # Process(target=self.jsonify, args=(lines[i], blueprint))
            clock.display()

        clock.stop()

        return lines

    async def save_data(self, payload: list):
        # payload = loads(dumps(data, indent=4).decode("utf-8"))

        with Mongo() as sql:
            sql.insert_all(payload)


x = Gonzales()
# run(x.download_data())
run(x.extract_files())

layout = run(x.read_blueprint())
run(x.save_blueprint(layout))

data = run(
    x.read_data(
        f"../downloads/{x.COUNTY}/GONZALES CAD 2022 CERTIFIED MINERAL OPEN RECORDS/3. MINERAL DATA FLAT TEXT FILE.TXT"
    )
)
run(x.save_data(data))

print("Save completed...")
