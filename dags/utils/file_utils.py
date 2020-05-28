import csv
from logger import logger


# Remove once python-tool is moved to here
def dump_to_csv_file(filename: str, headers: list, iterable):
    logger.info(f'Writing to:{filename}')
    with open(filename, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for row in iterable:
            writer.writerow(row)


# Remove once python-tool is moved to here
def load_csv_file(filename, has_header=True):
    result = []
    headers = []
    with open(filename) as f:
        reader = csv.reader(f)
        if has_header:
            headers = next(reader)
        for row in reader:
            result.append(row)
    return (headers, result)
