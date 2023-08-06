from asyncio import run
from scrapers.templates.TMDO19U import TMDO19U


class Wheeler(TMDO19U):
    def __init__(self):
        super().__init__(county="Wheeler")


x = Wheeler()
run(x.download_data())
run(x.extract_files())

layout = run(x.read_blueprint())
run(x.save_blueprint(layout))

data = run(x.read_data())
run(x.save_data(data))

print("Save completed...")
