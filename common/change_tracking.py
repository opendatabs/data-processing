import logging
import pathlib
from hashlib import blake2b
from filehash import FileHash
import os
import time

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
        logging.info(f'Getting modification timestamp of file {file_name} and writing to check file {check_file_name}...')
        # crc32_hasher = FileHash(hash_algorithm='crc32')
        # file_hash = crc32_hasher.hash_file(file_name)
        epoch = os.path.getmtime(file_name)
        iso = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(epoch))
        time_string = f'{epoch},{iso},{file_name}'
        logging.info(f'Writing the following time string into the check file (Epoch, ISO rounded to seconds, file path): {time_string}')
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
        logging.info(f'Comparing timestamps (Epoch, ISO rounded to seconds, file path): current / last: {current_timestamp_str} / {check_timestamp}, {current_iso} / {check_iso}. Different? {check_numbers_differ}')
    if check_numbers_differ:
        logging.info(f'Check numbers do not match, file has changed.')
        if do_update_hash_file:
            update_check_file(filename, check_file_name=check_filename, method=method)
        return True
    else:
        logging.info(f'Check numbers match, no changes detected.')
        return False
