from utils.csv import CSV
from utils.excel import Excel
from utils.sqlite import SQLite
from asyncio import run
from json import dumps


# COUNTY: str = 'Angelina'


async def main():
    try:

        # collection = await CSV.dictionary('downloads/ARMSTONG CAD 2022 CERTIFIED BPP OPEN RECORDS/layout.csv')
        collection = await CSV.read('downloads/ARMSTONG CAD 2022 CERTIFIED BPP OPEN RECORDS/layout.csv', delimiter='|')
        print(dumps(collection, indent=4))

        data: list = []

        with SQLite(path='appraisals.db') as sql:
            result = await sql.select("SELECT ID FROM County c WHERE c.Name = 'Armstrong';")
            for row in collection:
                data.append({
                    'Field_Name': row['DESCRIPTION'],
                    'Datatype': None,
                    'Start': row['BEG'],
                    'End': row['END'],
                    'Length': row['LENGTH'],
                    'Description': row['DESCRIPTION'],
                    'Metadata': {
                        'DEC': row['DEC'],
                        'A/N/P': row['A/N/P']
                    },
                    'CountyID': result[0][0]
                })

            await sql.bulk_insert(table='Blueprint', values=data)

        print(dumps(data, indent=4))

        print('Formatted...')

    except Exception as e:
        print(str(e))


async def read():
    with Excel(location='./resources/County.xlsx') as reader:
        data = await reader.read(start=('A', 1), end=('C', 255))
        print(data.to_markdown())

if __name__ == '__main__':
    run(read())

print('Completed...')
