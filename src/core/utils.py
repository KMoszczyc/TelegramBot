import os
import logging

log = logging.getLogger(__name__)


def read_str_file(path):
    with open(path, 'r') as f:
        lines = f.read().splitlines()
    return lines


def create_dir(path):
    if os.path.isdir(path):
        return

    os.makedirs(path)
    log.info(f'Created directory in path: {path}')
