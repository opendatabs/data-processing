import logging

import common


def main():
    r = common.requests_get('http://lexfind/api/fe/de/entities')
    r.raise_for_status()
    entities = r.json()
    print(entities)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
    logging.info("Job successful!")
