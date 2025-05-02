import os

import pytest
from bag_coronavirus.src import copy_bag_datasets as bag
import pandas as pd
import conftest
import filecmp


def test_regression_copy_bag_datasets():
    datasets = bag.get_dataset_metadata()
    for dataset in datasets:
        name = dataset['name']
        df_raw = bag.extract(url=dataset['base_path'][name])
        df_transformed = bag.transform(df_raw, dataset['suffix'])
        export_file_name = bag.load(name, df_transformed, dataset['suffix'])
        fixtures_file = os.path.join(conftest.CURR_DIR, 'fixtures', export_file_name)
        assert filecmp.cmp(fixtures_file, export_file_name, shallow=False)
