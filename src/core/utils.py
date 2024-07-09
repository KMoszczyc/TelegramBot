import os
import logging
import random
import re
from datetime import datetime

from definitions import ArgType
from src.models.command_args import CommandArgs

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


def preprocess_input(command_args: CommandArgs):
    command_args = parse_args(command_args)
    filtered_phrases = filter_phrases(command_args)
    return filtered_phrases


def parse_args(command_args: CommandArgs) -> CommandArgs:
    command_args.joined_args = ' '.join(command_args.args)
    command_args.joined_args_lower = ' '.join(command_args.args).lower()
    command_args.arg_type = ArgType.REGEX if is_inside_square_brackets(command_args.joined_args) else ArgType.TEXT

    return command_args


def filter_phrases(command_args: CommandArgs):
    log.info(f'Command received: {command_args.arg_type} - {command_args.joined_args}')
    match command_args.arg_type:
        case ArgType.TEXT:
            return text_filter(command_args)
        case ArgType.REGEX:
            return regex_filter(command_args)


def text_filter(command_args):
    return [phrase for phrase in command_args.phrases if command_args.joined_args_lower in phrase.lower()], command_args


def regex_filter(command_args):
    pattern = command_args.joined_args[1:-1]  # removes brackets
    try:
        return [phrase for phrase in command_args.phrases if re.search(pattern, phrase, flags=re.IGNORECASE)], command_args
    except re.error as e:
        command_args.error = f'{pattern} - is and invalid regex pattern.'
        log.info(f'{command_args.error} - {e}')

        return [], command_args


def is_inside_square_brackets(text):
    return text.startswith('[') and text.endswith(']')


def select_random_phrase(phrases, error_message):
    return random.choice(phrases) if phrases else error_message


def generate_unique_number(user_id):
    today = int(datetime.now().strftime('%Y%m%d'))
    user_id_cut = user_id % 100

    log.info(f'Generating lucky number for user [{user_id}] - {today + user_id_cut}')
    return today + user_id_cut


def is_prime(n):
    return all(False for i in range(2, n) if n % i == 0) and n >= 2
