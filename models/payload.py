from models.jsonify import Jsonify


class Payload(Jsonify):

    def __init__(self):
        self.CountyID: int = -1
        self.Name: str = ''