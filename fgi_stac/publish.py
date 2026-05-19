"""Job 3: upload GeoJSON to FTP and publish metadata/schema to HUWISE."""

from __future__ import annotations

import argparse
import logging

import publish_dataset
from dotenv import load_dotenv

load_dotenv()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish prepared datasets to FTP and HUWISE.")
    parser.add_argument(
        "--huwise-id",
        type=str,
        default="",
        help="Publish only this HUWISE dataset id (optional).",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    args = parse_args()
    publish_dataset.run(huwise_id_filter=args.huwise_id)


if __name__ == "__main__":
    main()
