import logging
import pathlib
from hashlib import blake2b
from filehash import FileHash
import os

logging.basicConfig(level=logging.DEBUG)


def get_hash_file_dir() -> str:
    curr_dir = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(curr_dir, 'change_tracking')


def write_hash_file(file_name, sfv_file_name='') -> str:
    if not sfv_file_name:
        sfv_file_name = get_hash_file(file_name, get_hash_file_dir())
    crc32_hasher = FileHash(hash_algorithm='crc32')
    # logging.info(f'Ensuring that hash file path exists...')
    pathlib.Path(os.path.dirname(sfv_file_name)).mkdir(parents=True, exist_ok=True)
    with open(sfv_file_name, 'w') as f:
        logging.info(f'Calculating hash of file {file_name} and writing to sfv file {sfv_file_name}...')
        file_hash = crc32_hasher.hash_file(file_name)
        f.write(f'{file_name} {file_hash}')
    return file_hash


def get_hash_file(filename, folder='') -> str:
    if not folder:
        folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'change_tracking')
    f = blake2b(filename.encode('utf-8')).hexdigest()
    sfv_filename = os.path.join(folder, f'{f}.sfv')
    return sfv_filename


def has_changed(filename: str, hash_file_dir='', update_hash_file=True) -> bool:
    if not os.path.exists(filename):
        raise FileNotFoundError(f'File does not exist: {filename}')
    logging.info(f'Checking for changes in file {filename}...')
    if not hash_file_dir:
        # logging.debug(f'Using default hash_file_dir {get_hash_file_dir()}...')
        hash_file_dir = get_hash_file_dir()
    sfv_filename = get_hash_file(filename, hash_file_dir)
    crc32_hasher = FileHash(hash_algorithm='crc32')
    if not os.path.exists(sfv_filename):
        logging.info(f'SFV file does not exist.')
        if update_hash_file:
            write_hash_file(filename)
        return True
    logging.info(f'SFV file exists, checking for changes: {sfv_filename}...')
    hashes_differ = not crc32_hasher.verify_sfv(sfv_filename=sfv_filename)[0].hashes_match
    if hashes_differ:
        logging.info(f'Hashes do not match.')
        if update_hash_file:
            write_hash_file(filename)
        return True
    else:
        logging.info(f'Hashes match...')
        return False
