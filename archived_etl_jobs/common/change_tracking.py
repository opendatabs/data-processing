import logging
import pathlib
import urllib3
from hashlib import blake2b
from filehash import FileHash
import os
import time
import pandas as pd
from common.retry import retry

logging.basicConfig(level=logging.DEBUG)


def get_check_file_dir() -> str:
    curr_dir = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(curr_dir, 'change_tracking')


def update_hash_file(file_name, sfv_file_name='') -> str:
    if not sfv_file_name:
        sfv_file_name = get_check_file(file_name, get_check_file_dir())
    # logging.info(f'Ensuring that hash file path exists...')
    pathlib.Path(os.path.dirname(sfv_file_name)).mkdir(parents=True, exist_ok=True)
    with open(sfv_file_name, 'w') as f:
        logging.info(f'Calculating hash of file {file_name} and writing to sfv file {sfv_file_name}...')
        crc32_hasher = FileHash(hash_algorithm='crc32')
        file_hash = crc32_hasher.hash_file(file_name)
        f.write(f'{file_name} {file_hash}')
    return file_hash


def update_mod_timestamp_file(file_name, check_file_name='') -> str:
    if not check_file_name:
        check_file_name = get_check_file(file_name, get_check_file_dir(), extension='txt')
    # logging.info(f'Ensuring that check file path exists...')
    pathlib.Path(os.path.dirname(check_file_name)).mkdir(parents=True, exist_ok=True)
    with open(check_file_name, 'w') as f:
        logging.info(
            f'Getting modification timestamp of file {file_name} and writing to check file {check_file_name}...')
        # crc32_hasher = FileHash(hash_algorithm='crc32')
        # file_hash = crc32_hasher.hash_file(file_name)
        epoch = os.path.getmtime(file_name)
        iso = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(epoch))
        time_string = f'{epoch},{iso},{file_name}'
        logging.info(
            f'Writing the following time string into the check file (Epoch, ISO rounded to seconds, file path): {time_string}')
        f.write(time_string)
    return time_string


def update_check_file(file_name, check_file_name='', method='hash') -> str:
    if method == 'hash':
        return update_hash_file(file_name, check_file_name)
    elif method == 'modification_date':
        return update_mod_timestamp_file(file_name, check_file_name)
    else:
        raise ValueError(f'"{method}" is not a valid method.')


def get_check_file(filename, folder='', extension='sfv') -> str:
    if not folder:
        folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'change_tracking')
    f = blake2b(filename.encode('utf-8')).hexdigest()
    check_filename = os.path.join(folder, f'{f}.{extension}')
    return check_filename


@retry(OSError, tries=6, delay=600, backoff=1)
def has_changed(filename: str, hash_file_dir='', do_update_hash_file=False, method='hash') -> bool:
    if not os.path.exists(filename):
        raise FileNotFoundError(f'File does not exist: {filename}')
    if method not in ['hash', 'modification_date']:
        raise ValueError(f'"{method}" is not a valid method.')
    logging.info(f'Checking for changes in file {filename}...')
    if not hash_file_dir:
        # logging.debug(f'Using default hash_file_dir {get_hash_file_dir()}...')
        hash_file_dir = get_check_file_dir()
    check_file_extension = 'sfv' if method == 'hash' else 'txt'
    check_filename = get_check_file(filename, hash_file_dir, extension=check_file_extension)
    if not os.path.exists(check_filename):
        logging.info(f'Check file does not exist.')
        if do_update_hash_file:
            update_check_file(filename, check_file_name=check_filename, method=method)
        return True
    logging.info(f'Check file exists, checking for changes using method "{method}" and check file {check_filename}...')
    check_numbers_differ = True
    if method == 'hash':
        crc32_hasher = FileHash(hash_algorithm='crc32')
        check_numbers_differ = not crc32_hasher.verify_sfv(sfv_filename=check_filename)[0].hashes_match
    elif method == 'modification_date':
        with open(check_filename, 'r') as f:
            lines = f.readlines()
        time_string = lines[0]
        logging.info(f'Read the following time_string from the check file: {time_string}')
        check_timestamp = time_string.split(',')[0]
        check_iso = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(float(check_timestamp)))
        current_timestamp = os.path.getmtime(filename)
        current_timestamp_str = str(current_timestamp)
        current_iso = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(current_timestamp))
        check_numbers_differ = current_timestamp_str != check_timestamp
        logging.info(
            f'Comparing timestamps (Epoch, ISO rounded to seconds, file path): current / last: {current_timestamp_str} / {check_timestamp}, {current_iso} / {check_iso}. Different? {check_numbers_differ}')
    if check_numbers_differ:
        logging.info(f'Check numbers do not match, file has changed.')
        if do_update_hash_file:
            update_check_file(filename, check_file_name=check_filename, method=method)
        return True
    else:
        logging.info(f'Check numbers match, no changes detected.')
        return False


def find_new_rows(df_old, df_new, id_columns):
    # Find new rows by checking for rows in df_new that are not in df_old
    merged = pd.merge(df_old[id_columns], df_new, on=id_columns, how='right', indicator=True)
    new_rows = merged[merged['_merge'] == 'right_only'].drop(columns=['_merge'])
    logging.info(f'Found {len(new_rows)} new rows:')
    logging.info(new_rows)
    return new_rows


def find_modified_rows(df_old, df_new, id_columns, columns_to_compare=None):
    if columns_to_compare is None:
        columns_to_compare = [col for col in df_new.columns if col not in id_columns]
    # Merge the dataframes on the id columns
    merged = pd.merge(df_old, df_new, on=id_columns, suffixes=('_old', '_new'), how='inner')
    mask = pd.DataFrame(index=merged.index)
    changed_columns = []  # To store column names where changes occur
    for col in columns_to_compare:
        # If a new row gets added to the dataframe, the old column will be NaN
        if f'{col}_old' not in merged.columns:
            merged[f'{col}_old'] = ''
            merged[f'{col}_new'] = merged[col]
        old_col = merged[f'{col}_old']
        new_col = merged[f'{col}_new']
        if new_col.dtype == 'category':
            # Handle categorical columns by ensuring they have the same categories
            common_categories = sorted(set(new_col.cat.categories.tolist() + old_col.cat.categories.tolist()))
            new_col = new_col.cat.set_categories(common_categories)
            old_col = old_col.cat.set_categories(common_categories)
        # Compare columns and record where changes occurred
        mask[col] = ~((old_col == new_col) | (pd.isna(old_col) & pd.isna(new_col)))
        # If there are changes in this column, add the column name to the list
        if mask[col].any():
            changed_columns.append(col)
    # Filter rows with any changes in the specified columns
    modified_rows = merged[mask.any(axis=1)]
    
    # Log the columns that had changes
    if changed_columns:
        logging.info(f'Columns with changes: {changed_columns}')
    else:
        logging.info('No columns were modified.')
    logging.info(f'Found {len(modified_rows)} modified rows:')
    # Deprecated rows (old values)
    deprecated_rows = modified_rows[([id_columns] if isinstance(id_columns, str) else id_columns) +
                                    [f'{col}_old' for col in columns_to_compare]].rename(
        columns={f'{col}_old': col for col in columns_to_compare})
    logging.info(f'Deprecated rows:')
    logging.info(deprecated_rows)
    # Updated rows (new values)
    updated_rows = modified_rows[([id_columns] if isinstance(id_columns, str) else id_columns) +
                                 [f'{col}_new' for col in columns_to_compare]].rename(
        columns={f'{col}_new': col for col in columns_to_compare})
    logging.info(f'Updated rows:')
    logging.info(updated_rows)
    # Print detailed changes for debugging
    for idx, row in modified_rows.iterrows():
        row_id = row[id_columns] if isinstance(id_columns, str) else tuple(row[id_columns])
        logging.info(f'\nChanges for row with ID {row_id}:')
        for col in changed_columns:
            old_value = row[f'{col}_old']
            new_value = row[f'{col}_new']
            if not pd.isna(old_value) or not pd.isna(new_value):
                logging.info(f" - Column '{col}': old value = {old_value}, new value = {new_value}")
    
    return deprecated_rows, updated_rows



def find_deleted_rows(df_old, df_new, id_columns):
    # Find deleted rows by checking for rows in df_old that are not in df_new
    merged = pd.merge(df_old, df_new[id_columns], on=id_columns, how='left', indicator=True)
    deleted_rows = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge'])
    logging.info(f'Found {len(deleted_rows)} deleted rows:')
    logging.info(deleted_rows)
    return deleted_rows
