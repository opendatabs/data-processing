import logging
import os
import pathlib
import shutil
from datetime import datetime
from random import random
import common.change_tracking as ct
import pytest
import common
import time
import pandas as pd

CURR_DIR = os.path.dirname(os.path.realpath(__file__))
CHANGE_TRACKING_DIR = os.path.join(CURR_DIR, 'fixtures', 'change_tracking')

# Show Logs in PyCharm when debugging PyTests: https://intellij-support.jetbrains.com/hc/en-us/community/posts/360007644040/comments/360002681399

def teardown_function():
    """Delete all sfv files used for the tests."""
    shutil.rmtree(CHANGE_TRACKING_DIR, ignore_errors=True)


@pytest.fixture
def text_file(tmp_path):
    file_path = os.path.join(tmp_path, f'test-{random()}.txt')
    with open(file_path, 'w') as f:
        f.write(f'{datetime.now()}: Hello World!')
    yield file_path
    os.remove(file_path)


def test_wrong_method(text_file):
    with pytest.raises(ValueError):
        result = ct.has_changed(text_file, hash_file_dir=CHANGE_TRACKING_DIR, method='no_method')


def test_no_file(tmp_path):
    file_path = os.path.join(tmp_path, f'test-{random()}.txt')
    with pytest.raises(FileNotFoundError):
        result = ct.has_changed(file_path, hash_file_dir=CHANGE_TRACKING_DIR)


def test_no_file_timestamp(tmp_path):
    file_path = os.path.join(tmp_path, f'test-{random()}.txt')
    with pytest.raises(FileNotFoundError):
        result = ct.has_changed(file_path, hash_file_dir=CHANGE_TRACKING_DIR, method='modification_date')


def test_new_file(text_file):
    assert ct.has_changed(text_file, hash_file_dir=CHANGE_TRACKING_DIR)


def test_new_file_timestamp(text_file):
    assert ct.has_changed(text_file, hash_file_dir=CHANGE_TRACKING_DIR, method='modification_date')


def test_unchanged_file(text_file):
    # Access file first to generate, then a 2nd time it should not have changed.
    assert ct.has_changed(text_file, hash_file_dir=CHANGE_TRACKING_DIR)
    assert not ct.has_changed(text_file, hash_file_dir=CHANGE_TRACKING_DIR)
    assert not ct.has_changed(text_file, hash_file_dir=CHANGE_TRACKING_DIR)


def test_unchanged_file_timestamp(text_file):
    # Access file first to generate, then a 2nd time it should not have changed.
    assert ct.has_changed(text_file, hash_file_dir=CHANGE_TRACKING_DIR, method='modification_date')
    assert not ct.has_changed(text_file, hash_file_dir=CHANGE_TRACKING_DIR, method='modification_date')
    time.sleep(2)
    assert not ct.has_changed(text_file, hash_file_dir=CHANGE_TRACKING_DIR, method='modification_date')


def test_changed_file(text_file):
    # Access file first to generate, then a 2nd time it should not have changed.
    assert ct.has_changed(text_file, hash_file_dir=CHANGE_TRACKING_DIR)
    assert not ct.has_changed(text_file, hash_file_dir=CHANGE_TRACKING_DIR)
    with open(text_file, 'w') as f:
        f.write(f'{datetime.now()}: Hello World!')
    assert ct.has_changed(text_file, hash_file_dir=CHANGE_TRACKING_DIR)
    assert not ct.has_changed(text_file, hash_file_dir=CHANGE_TRACKING_DIR)
    with open(text_file, 'a') as f:
        f.write(f'{datetime.now()}_Hello World! \n')
    assert ct.has_changed(text_file, hash_file_dir=CHANGE_TRACKING_DIR)
    assert not ct.has_changed(text_file, hash_file_dir=CHANGE_TRACKING_DIR)
    assert not ct.has_changed(text_file, hash_file_dir=CHANGE_TRACKING_DIR)


def test_changed_file_timestamp(text_file):
    # Access file first to generate, then a 2nd time it should not have changed.
    assert ct.has_changed(text_file, hash_file_dir=CHANGE_TRACKING_DIR, method='modification_date')
    assert not ct.has_changed(text_file, hash_file_dir=CHANGE_TRACKING_DIR, method='modification_date')
    with open(text_file, 'w') as f:
        f.write(f'{datetime.now()}: Hello World!')
    assert ct.has_changed(text_file, hash_file_dir=CHANGE_TRACKING_DIR, method='modification_date')
    assert not ct.has_changed(text_file, hash_file_dir=CHANGE_TRACKING_DIR, method='modification_date')
    with open(text_file, 'a') as f:
        f.write(f'{datetime.now()}_Hello World! \n')
    assert ct.has_changed(text_file, hash_file_dir=CHANGE_TRACKING_DIR, method='modification_date')
    assert not ct.has_changed(text_file, hash_file_dir=CHANGE_TRACKING_DIR, method='modification_date')
    assert not ct.has_changed(text_file, hash_file_dir=CHANGE_TRACKING_DIR, method='modification_date')


def create_df(id_values, value_values, index=None):
    return pd.DataFrame({'id': id_values, 'value': value_values}, index=index)


def test_find_new_rows():
    df_old = create_df(['1', '2', '3'], ['a', 'b', 'c'])
    df_new = create_df(['2', '3', '4'], ['b', 'c', 'd'])
    expected = create_df(['4'], ['d'], index=[2])
    result = ct.find_new_rows(df_old, df_new, ['id'])
    pd.testing.assert_frame_equal(result, expected)

    df_new = create_df(['1', '2', '3'], ['a', 'b', 'c'])
    expected = pd.DataFrame(columns=['id', 'value'])
    result = ct.find_new_rows(df_old, df_new, ['id'])
    pd.testing.assert_frame_equal(result, expected)

    with pytest.raises(KeyError):
        ct.find_new_rows(df_old, df_new, ['non_existent_column'])


def test_find_modified_rows():
    df_old = create_df(['1', '2', '3'], ['a', 'b', 'c'])
    df_new = create_df(['1', '2', '3'], ['a', 'b', 'd'])
    expected = create_df(['3'], ['d'], index=[2])
    result = ct.find_modified_rows(df_old, df_new, ['id'])
    pd.testing.assert_frame_equal(result, expected)

    df_new = create_df(['1', '2', '3'], ['a', 'b', 'c'])
    expected = pd.DataFrame(columns=['id', 'value'])
    result = ct.find_modified_rows(df_old, df_new, ['id'])
    pd.testing.assert_frame_equal(result, expected)

    with pytest.raises(KeyError):
        ct.find_modified_rows(df_old, df_new, ['non_existent_column'])


def test_find_deleted_rows():
    df_old = create_df(['1', '2', '3'], ['a', 'b', 'c'])
    df_new = create_df(['2', '3', '4'], ['b', 'c', 'd'])
    expected = create_df(['1'], ['a'], index=[0])
    result = ct.find_deleted_rows(df_old, df_new, ['id'])
    pd.testing.assert_frame_equal(result, expected)

    df_new = create_df(['1', '2', '3'], ['a', 'b', 'c'])
    expected = pd.DataFrame(columns=['id', 'value'])
    result = ct.find_deleted_rows(df_old, df_new, ['id'])
    pd.testing.assert_frame_equal(result, expected)

    with pytest.raises(KeyError):
        ct.find_deleted_rows(df_old, df_new, ['non_existent_column'])
