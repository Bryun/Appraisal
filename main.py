from time import sleep
from scripts.queues import FILE, CONVERTER
from utils.publisher import Publisher

COUNTY: str = 'Angelina'


async def main():
    # try:
    #     with Excel(location='Book1.xlsx') as reader:
    #         data = await reader.read(start=('A', 1), end=('C', 255))
    #
    #         for x in data.columns:
    #             data[x] = data[x].str.replace('\xa0', '', regex=True)
    #             data[x] = data[x].str.strip()
    #
    #             if x == 'Name':
    #                 data[x] = data[x].str.replace('^\d+\s', '', regex=True)
    #
    #         with SQLite(path='appraisals.db') as sql:
    #             await sql.drop_and_recreate(table='County', o=County())
    #             await sql.bulk_insert_dataframe(table='County', frame=data)
    # except Exception as e:
    #     print(str(e))

    # publisher = Publisher(name=FILE)
    #
    # with open('resources/2022-08-16_000973_APPRAISAL_INFO.TXT', 'r') as reader:
    #     for line in reader:
    #         publisher.publish(line)
    #         # sleep(0.05)

    print('Completed...')


# run(main())

# publisher = Publisher(name=CONVERTER)
#
# with open('resources/2022-08-16_000973_APPRAISAL_INFO.TXT', 'r') as reader:
#     for line in reader:
#         publisher.publish(line)
#         sleep(0.14)

print('Completed...')