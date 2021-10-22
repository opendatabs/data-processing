import logging
import os
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import pandas as pd
from random import random
import pytest

import common

CURR_DIR = os.path.dirname(os.path.realpath(__file__))
EMBARGO_DIR = os.path.join(CURR_DIR, 'fixtures', 'embargo')


@pytest.fixture
def data_file_path(tmp_path):
    df = pd.DataFrame({'ID': [1, 2, 3], 'Name': ['Jonas', 'Peter', 'Franz']})
    file_path = os.path.join(tmp_path, f'test-{random()}.csv')
    df.to_csv(file_path, index=False)
    yield file_path
    os.remove(file_path)


@pytest.fixture
def embargo_file(tmp_path, data_file_path, request):
    embargo_file_path = data_file_path.replace('.csv', '_embargo.txt')
    minutes_add = int(request.node.get_closest_marker('minutes_add').args[0])
    datetime_str = (datetime.now(timezone.utc).astimezone(ZoneInfo('Europe/Zurich')) + timedelta(minutes=minutes_add)).strftime('%Y-%m-%dT%H:%M')
    with open(embargo_file_path, 'w') as f:
        f.write(datetime_str)
    yield embargo_file_path
    os.remove(embargo_file_path)


@pytest.mark.minutes_add(-1)
def test_past_embargo_is_lifted(data_file_path, embargo_file):
    assert common.is_embargo_over(data_file_path)


@pytest.mark.minutes_add(1)
def test_future_embargo_is_not_lifted(data_file_path, embargo_file):
    assert not common.is_embargo_over(data_file_path)
