from models.jsonify import Jsonify


class Blueprint(Jsonify):

    def __init__(self):
        self.PayloadID: int = -1
        self.Field_Name: str = ''
        self.Datatype: str = ''
        self.Start: int = -1
        self.End: int = -1
        self.Length: int = -1
        self.Description: str = ''
