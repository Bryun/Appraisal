from json import loads
from os import environ

from dotenv import find_dotenv, load_dotenv
from pymongo import MongoClient

load_dotenv(find_dotenv())

MONGO = environ.get("MONGO")


class Mongo:
    def __init__(self, database: str = "Appraisal", collection: str = "TMDO19U"):
        # self.connection = f"mongodb+srv://{user}:{password}@cluster.mongodb.net/{db}"
        self.connection: MongoClient = MongoClient(f"mongodb://{MONGO}")
        self.database = database
        self.collection = collection
        self.instance = None

    def __enter__(self):
        try:
            self.connection = MongoClient("mongodb://localhost:27017")
            self.instance = self.connection[self.database][self.collection]
        except Exception as e:
            print(str(e))

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection is not None:
            self.connection.close()

    def insert(self, payload):
        self.instance.insert_one(payload)

    def insert_all(self, payload):
        self.instance.insert_many(payload)
