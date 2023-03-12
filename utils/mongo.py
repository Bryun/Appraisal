from pymongo import MongoClient


class Mongo:
    def __init__(self):
        self.user: str = ''
        self.password: str = ''
        self.db: str = ''
        # self.connection = f"mongodb+srv://{user}:{password}@cluster.mongodb.net/{db}"
        self.connection: MongoClient = None

    def __enter__(self):

        try:
            self.connection = MongoClient("mongodb://localhost:27017")
        except Exception as e:
            print(str(e))

        return self.connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection is not None:
            self.connection.close()