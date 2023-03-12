import json


class Jsonify:
    def to_json(self):
        return json.loads(json.dumps(self.__dict__, indent=4))