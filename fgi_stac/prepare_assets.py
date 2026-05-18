"""Job 2: download GeoJSON, run transforms, sync schema YAML, and add map_links."""

from __future__ import annotations

import argparse
import logging

from dotenv import load_dotenv

from stac_sync import prepare_assets

load_dotenv()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare GeoJSON and schema assets for publish.")
    parser.add_argument(
        "--huwise-id",
        type=str,
        default="",
        help="Prepare only this HUWISE dataset id (optional).",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    args = parse_args()
    prepare_assets(huwise_id_filter=args.huwise_id)


if __name__ == "__main__":
    main()
