import logging
import os
from pathlib import Path

import common
import common.change_tracking as ct
import pandas as pd
from decentlab import query
from dotenv import load_dotenv

load_dotenv()

DOMAIN = 'bl-lufthygieneamt.decentlab.com'
API_KEY = os.getenv('API_KEY_DECENTLAB')
DEVICES = ['16300', '16303']


def normalize_column_name(column_name, device):
    prefix = f'{device}.'
    if column_name.startswith(prefix):
        return column_name[len(prefix):]
    return column_name


def transform_device_df(df, device):
    transformed = df.copy()
    transformed.columns = [normalize_column_name(col, device) for col in transformed.columns]
    transformed = transformed.reset_index().rename(columns={'time': 'timestamp'})
    transformed.insert(0, 'standort', device)
    return transformed


def extract_metadata_rows(df, device):
    rows = []
    tags = getattr(df, 'tags', {})
    for source_column, metadata in tags.items():
        rows.append(
            {
                'standort': device,
                'column': normalize_column_name(source_column, device),
                'unit': metadata.get('unit'),
                'sensor': metadata.get('sensor'),
                'channel': metadata.get('channel'),
                'title': metadata.get('title'),
            }
        )
    return rows


def main():
    transformed_frames = []
    metadata_rows = []

    for device in DEVICES:
        df_device = query(
            domain=DOMAIN,
            api_key=API_KEY,
            device=f'/^{device}$/',
        )
        logging.info(f"Device {device}: {len(df_device)} rows, columns: {list(df_device.columns)}")

        transformed_frames.append(transform_device_df(df_device, device))
        metadata_rows.extend(extract_metadata_rows(df_device, device))

    combined_df = pd.concat(transformed_frames, ignore_index=True, sort=False)
    combined_df = combined_df.sort_values(['timestamp', 'standort']).reset_index(drop=True)

    metadata_df = pd.DataFrame(metadata_rows)
    metadata_df = metadata_df.drop_duplicates().sort_values(['column', 'standort']).reset_index(drop=True)

    output_dir = Path(__file__).resolve().parent / 'data'
    output_dir.mkdir(parents=True, exist_ok=True)

    data_path = output_dir / '100523_feinstaub.csv'
    metadata_path = output_dir / 'metadata_feinstaub.csv'

    combined_df.to_csv(data_path, index=False)
    metadata_df.to_csv(metadata_path, index=False)

    logging.info(f"Saved transformed data to {data_path}")
    logging.info(f"Saved metadata to {metadata_path}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job successful.")

