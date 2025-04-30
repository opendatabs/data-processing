import os

import pandas as pd
import pytest

CURR_DIR = os.path.dirname(os.path.realpath(__file__))


@pytest.fixture
def pool_df():
    pickle_path = os.path.join(CURR_DIR, 'fixtures/massentests_pool.pickle')
    return pd.read_pickle(pickle_path)


@pytest.fixture
def single_df():
    pickle_path = os.path.join(CURR_DIR, 'fixtures/massentests_single.pickle')
    return pd.read_pickle(pickle_path)
