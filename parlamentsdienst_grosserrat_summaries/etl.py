import logging

import truststore
from corpus_extract import job

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", force=True)
    for client in ("httpx", "httpx2"):
        logging.getLogger(client).setLevel(logging.WARNING)
    truststore.inject_into_ssl()
    job.main()
    logging.info("Job successful.")
