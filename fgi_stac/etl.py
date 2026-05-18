"""Full pipeline: sync catalog → prepare assets → publish."""

from __future__ import annotations

import argparse
import logging

from dotenv import load_dotenv

import publish_dataset
from catalog import load_flat_publish_catalog
from paths import ORIG_CATALOG_FILE
from stac_sync import CATALOG_FILE, prepare_assets, sync_catalog

load_dotenv()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run catalog sync, asset preparation, and HUWISE publish.")
    parser.add_argument(
        "--huwise-id",
        type=str,
        default="",
        help="Limit prepare and publish to this HUWISE dataset id (optional).",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    args = parse_args()
    sync_catalog()
    active_count = len(load_flat_publish_catalog(CATALOG_FILE))
    print(f"Catalog updated: {ORIG_CATALOG_FILE} ({active_count} active HUWISE datasets)")
    prepare_assets(huwise_id_filter=args.huwise_id)
    publish_dataset.run(huwise_id_filter=args.huwise_id)


if __name__ == "__main__":
    main()
