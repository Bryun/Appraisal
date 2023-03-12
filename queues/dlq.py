from asyncio import run
from json import loads
from pymongo import MongoClient

from utils.consumer import Consumer


class DLQ(Consumer):

    def __init__(self, name: str):
        self.connection = MongoClient("mongodb://localhost:27017")
        self.database = self.connection['Appraisal']
        self.error = self.database['Error']
        super().__init__(name=name)

    def callback(self, ch, method, properties, body):
        payload = loads(body.decode('utf-8'))
        print(" [x] %r" % (properties,))
        print(" [reason] : %s : %r" % (properties.headers['x-death'][0]['reason'], body))
        self.error.insert_one(payload)
        super().callback(ch, method, properties, body)


async def main():
    DLQ(name='DLQ')


run(main())
