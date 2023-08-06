from asyncio import run
from os import listdir, makedirs
from os.path import exists
from re import search
import re
from time import sleep

from pandas import DataFrame
from selenium.webdriver.common.by import By
from scrapers.icounty import ICounty
from scrapers.templates.TMDO19U import TMDO19U
from utils.excel import Excel
from utils.mongo import Mongo
from utils.pdf import PDF
from utils.scraper import Scraper
from utils.sqlite import SQLite
from utils.timer import Timer
from zipfile import ZipFile
from json import dumps, loads


class Jack(TMDO19U):
    def __init__(self):
        super().__init__(county="Jack")


x = Jack()
run(x.download_data())
run(x.extract_files())

layout = run(x.read_blueprint())
run(x.save_blueprint(layout))

data = run(x.read_data())
run(x.save_data(data))

print("Save completed...")
