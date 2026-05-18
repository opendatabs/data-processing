"""Job 1: refresh STAC/Dataspot catalog and HUWISE bindings workbook."""

from __future__ import annotations

import logging

from dotenv import load_dotenv

from stac_sync import sync_catalog

load_dotenv()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    sync_catalog()


if __name__ == "__main__":
    main()
