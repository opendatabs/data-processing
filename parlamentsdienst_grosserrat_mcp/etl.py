import logging

import truststore
from corpus_mcp import job

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", force=True)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    truststore.inject_into_ssl()
    job.reindex()
    logging.info("Job successful.")
