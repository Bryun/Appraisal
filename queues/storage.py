from asyncio import run, create_task
from json import loads
from pymongo import MongoClient
from scripts.queues import STORAGE
from utils.consumer import Consumer
from os import environ
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

MONGO = environ.get('MONGO')


class Storage(Consumer):

    def __init__(self, name: str, exchange: str = STORAGE):
        self.connection = MongoClient(f"mongodb://{MONGO}")
        self.database = self.connection['Appraisal']
        # self.collection = self.database['Roll']
        self.collection = self.database['TMDO19U']
        self.error = self.database['Error']
        # self.collection.create_index([("prop_id", 1), ("prop_val_yr", 1), ("geo_id", 1)], unique=True)

        super().__init__(name=name, exchange=exchange)

    def callback(self, ch, method, properties, body):
        try:
            payload = loads(body.decode('utf-8'))
            self.collection.insert_one(payload)
        except Exception as e:
            print("\n\n{}\n\n".format(str(e)))
            self.error.insert_one(payload)
        super().callback(ch, method, properties, body)


async def main():
    Storage(name=STORAGE)


run(main())
