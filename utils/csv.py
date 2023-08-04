from csv import reader, DictReader


class CSV:

    @staticmethod
    async def read(path: str, delimiter: str = ',') -> list:
        collection: list = []

        with open(path) as file:
            read = reader(file, delimiter=delimiter)

            data = [list(map(lambda x: x.strip(), row)) for row in read]

            for y in range(1, len(data)):
                payload = {}
                for k in data[0]:
                    index = data[0].index(k)
                    value = data[y][index]

                    if value.isnumeric():
                        payload[k] = int(value)
                    elif value == '':
                        payload[k] = None
                    else:
                        payload[k] = value
                collection.append(payload)

        return collection

    @staticmethod
    async def dictionary(path: str) -> list:
        with open(path) as file:
            read = DictReader(file)
            return [row for row in read]
