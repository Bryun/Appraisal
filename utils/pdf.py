from pdfplumber import open


class PDF:
    def __init__(self, path: str):
        self.path: str = path

    def read(self) -> list:
        collection: list = []

        with open(self.path) as reader:
            for page in reader.pages:
                text = page.extract_text(x_tolerance=2, y_tolerance=2)
                rows: list = text.split("\n")
                collection.append(rows)

        return collection
