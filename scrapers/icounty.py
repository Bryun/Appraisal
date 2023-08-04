from pandas import DataFrame

from utils.sqlite import SQLite


class ICounty:
    def __init__(self):
        self.COUNTY: str = ""
        self.URL: str = ""

    async def has_blueprint(self) -> bool:
        """Checks if blueprint for county exists."""
        return False

    async def download_blueprint(self):
        """Downloads layout data for county."""
        pass

    async def read_blueprint(self):
        """Reads layout data for county."""
        pass

    async def save_blueprint(self, data):
        """Save layout data to sqllite database."""
        pass

    async def download_data(self):
        """Downloads appraisal roll data for county."""
        pass

    async def read_data(self):
        """Reads appraisal roll data."""
        pass

    async def save_data(self):
        """Saves appraisal roll data into mongodb database."""
        pass

    async def extract_files(self):
        """Extract files from downloased package."""
        pass

    async def get_blueprint(self) -> DataFrame:
        """Get blueprint information."""
        with SQLite(path="../appraisals.db") as query:
            sql = (
                "SELECT Field_Name, Datatype, Start, End, Length, Description FROM Blueprint b "
                + "WHERE b.CountyID = (SELECT c.ID FROM County c WHERE c.Name = ?);"
            )
            blueprint = await query.select_by_parameter(sql, [self.COUNTY])

            return blueprint

    async def jsonify(self, line: str, blueprint: DataFrame):
        filler: int = 1
        map: dict = {}

        for i, row in blueprint.iterrows():
            key: str = row["Field_Name"]
            start: int = row["Start"] - 1
            end: int = row["End"]
            value = line[start:end]

            if row["Field_Name"] == "filler":
                key = f"filler_{filler}"
                filler += 1

            if row["Datatype"] is not None:
                if row["Datatype"].startswith("char"):
                    map[key] = value.strip() if len(value.strip()) > 0 else None
                elif row["Datatype"].startswith("int"):
                    map[key] = int(value) if value.isnumeric() else None
                elif row["Datatype"].startswith("numeric"):
                    map[key] = int(value) if value.isnumeric() else None
            else:
                map[key] = value.strip()

        return map
