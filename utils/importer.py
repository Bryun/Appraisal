from asyncio import run

from models.county import County
from utils.excel import Excel
from utils.sqlite import SQLite


async def main():
    try:
        with Excel(location='Book1.xlsx') as reader:
            data = await reader.read(start=('A', 1), end=('C', 255))

            for x in data.columns:
                data[x] = data[x].str.replace('\xa0', '', regex=True)
                data[x] = data[x].str.strip()

                if x == 'Name':
                    data[x] = data[x].str.replace('^\d+\s', '', regex=True)

            with SQLite(path='appraisals.db') as sql:
                await sql.drop_and_recreate(table='County', o=County())
                await sql.bulk_insert_dataframe(table='County', frame=data)
    except Exception as e:
        print(str(e))

    print('Completed...')


run(main())
