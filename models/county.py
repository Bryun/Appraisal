from models.jsonify import Jsonify


class County(Jsonify):

    def __init__(self):
        super().__init__()
        self.Name: str = ''
        self.URL: str = ''
        self.Located: str = ''
