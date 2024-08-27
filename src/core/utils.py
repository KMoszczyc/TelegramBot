import copy
import locale
import os
import logging
import random
import re
from datetime import datetime

from definitions import ArgType, MessageType, CHAT_IMAGES_DIR_PATH, CHAT_VIDEOS_DIR_PATH, CHAT_GIFS_DIR_PATH, CHAT_AUDIO_DIR_PATH
from src.models.command_args import CommandArgs

log = logging.getLogger(__name__)
MAX_INT = 24 * 365 * 20
locale.setlocale(
    category=locale.LC_ALL,
    locale="Polish"
)

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
    command_args = parse_named_args(command_args)
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


def are_you_lucky(user_id):
    today = int(datetime.now().strftime('%Y%m%d'))
    user_hash = user_id + today
    random.seed(user_hash)
    rand_value = random.random()

    if rand_value < 0.1:
        message = "Dzisiaj masz wielkiego pecha. Lepiej zostań w domu i nic nie rób. (łeee jestem grzybem ;-;)"
    elif rand_value < 0.3:
        message = "Dzisiaj masz lekkiego pecha. Zachowaj ostrożność."
    elif rand_value < 0.7:
        message = "Normalny dzień dla normalnego chłopa."
    elif rand_value < 0.9:
        message = "Dzisiaj masz lekkie szczęście. Możesz spróbować coś zrobić, ale może się to nie powieść."
    else:
        message = "Dzisiaj masz ogromne szczęście! Wyjdź z domu i spróbuj zrobić coś nowego, na pewno Ci się uda!"

    log.info(f'User [{user_hash}] ({rand_value}) - {message}')
    return message


def is_prime(n):
    return all(False for i in range(2, n) if n % i == 0) and n >= 2


def is_gif(message):
    return message.document and message.document.mime_type == "video/mp4" and message.gif


def is_video(message):
    return message.document and message.document.mime_type == "video/mp4" and not message.gif


def message_id_to_path(message_id, message_type: MessageType):
    match message_type:
        case MessageType.IMAGE:
            filename = f'{message_id}.jpg'
            return os.path.join(CHAT_IMAGES_DIR_PATH, filename)
        case MessageType.GIF:
            filename = f'{message_id}.mp4'
            return os.path.join(CHAT_GIFS_DIR_PATH, filename)
        case MessageType.VIDEO | MessageType.VIDEO_NOTE:
            filename = f'{message_id}.mp4'
            return os.path.join(CHAT_VIDEOS_DIR_PATH, filename)
        case MessageType.AUDIO:
            filename = f'{message_id}.ogg'
            return os.path.join(CHAT_AUDIO_DIR_PATH, filename)
    return None


def get_message_type(message):
    if message.photo:
        return MessageType.IMAGE
    if message.document and message.document.mime_type == "video/mp4" and message.gif:
        return MessageType.GIF
    if message.document and message.document.mime_type == "video/mp4" and not message.gif and not message.video_note:
        return MessageType.VIDEO
    if message.document and message.document.mime_type == "video/mp4" and not message.gif and message.video_note:
        return MessageType.VIDEO_NOTE
    if message.voice:
        return MessageType.AUDIO

    return MessageType.TEXT


async def download_media(message, message_type):
    match message_type:
        case MessageType.IMAGE:
            path = message_id_to_path(message.id, MessageType.IMAGE)
        case MessageType.GIF:
            path = message_id_to_path(message.id, MessageType.GIF)
        case MessageType.VIDEO | MessageType.VIDEO_NOTE:
            path = message_id_to_path(message.id, MessageType.VIDEO)
        case MessageType.AUDIO:
            path = message_id_to_path(message.id, MessageType.AUDIO)
        case _:
            return None

    if not os.path.exists(path):
        await message.download_media(file=path)


def parse_arg(command_args_ref, arg_str, arg_type: ArgType) -> CommandArgs:
    command_args = copy.deepcopy(command_args_ref)
    match arg_type:
        case ArgType.POSITIVE_INT:
            command_args = parse_number(command_args, arg_str, positive_only=True)
        case ArgType.STRING:
            command_args = parse_string(command_args, arg_str)
        case _:
            command_args = command_args

    return command_args


def parse_named_args(command_args_ref: CommandArgs):
    # TODO: Just do it
    command_args = copy.deepcopy(command_args_ref)
    shortened_available_named_args = [arg[0] for arg in command_args.available_named_args]
    args = copy.deepcopy(command_args.args)
    for i, arg in enumerate(args):
        named_arg = parse_named_arg(arg, shortened_available_named_args, command_args.available_named_args)
        if named_arg is None:
            continue
        if command_args.available_named_args[named_arg] == ArgType.NONE:
            command_args.named_args[named_arg] = None
        elif i + 1 < len(args) and not is_named_arg(args[i + 1], shortened_available_named_args, command_args.available_named_args):  # this arg has a value
            arg_type = command_args.available_named_args[named_arg]
            command_args = parse_arg(command_args, args[i + 1], arg_type)
            command_args.args.remove(args[i + 1])
            if get_error(command_args) == '':
                command_args.named_args[named_arg] = command_args.number
        else:
            command_args.errors.append(f'Argument {named_arg} requires a value')
        command_args.args.remove(arg)

    command_args.error = get_error(command_args)
    return command_args


def parse_named_arg(arg, shortened_available_named_args, available_named_args):
    if is_shortened_named_arg(arg, shortened_available_named_args) or is_normal_named_arg(arg, available_named_args):
        return arg.replace('-', '')
    return None


def is_shortened_named_arg(arg, shortened_available_named_args):
    return (arg.startswith('-') and arg[1:] in shortened_available_named_args)


def is_normal_named_arg(arg, available_named_args):
    return arg.startswith('--') and arg[2:] in available_named_args


def is_named_arg(arg, shortened_available_named_args, available_named_args):
    return is_shortened_named_arg(arg, shortened_available_named_args) or is_normal_named_arg(arg, available_named_args)


def parse_number(command_args, arg_str, positive_only=False) -> CommandArgs:
    if arg_str == '':
        return command_args

    number, error = parse_int(arg_str, positive_only)
    if error != '':
        command_args.errors.append(error)
        return command_args

    if number > command_args.number_limit:
        error = f"Given number is too big ({x_to_light_years_str(number)}), make it smaller!"
        command_args.errors.append(error)
        log.error(error)
        return command_args

    command_args.number = number
    command_args.errors.append('')
    return command_args


def get_error(command_args: CommandArgs) -> str:
    return '\n'.join(command_args.errors).strip()


def parse_int(num_str, positive_only=False):
    error = ''
    num = None
    try:
        num = int(num_str)
        if num > MAX_INT:
            error = f"Kuba's dick is too big ({x_to_light_years_str(num)}), make it smaller!"
            log.error(error)
        if positive_only and num < 0:
            error = "Number cannot be negative!"
            num = -1
            log.error(error)
    except ValueError:
        error = f"{num_str} is not a number."
        log.error(error)

    return num, error


def x_to_light_years_str(x):
    """Kinda to last years, keep small numbers the same."""
    if x < 10000000:
        return str(x)

    ly = x / 9460730472580.8
    ly = round(ly, 6) if ly < 1 else round(ly, 2)
    return f'{ly} light years'


def parse_string(command_args: CommandArgs, text: str) -> CommandArgs:
    error = ''
    if len(text) < command_args.min_string_length:
        error = f'{command_args.label} {text} is too short, it should have at least {command_args.min_string_length} characters.'
    if len(text) > command_args.max_string_length:
        error = f'{command_args.label} {text} is too long, it should have {command_args.max_string_length} characters or less.'

    command_args.errors.append(error)
    command_args.string = text
    return command_args

def display_shopping_sunday(dt):
    return dt.strftime('%d %B')

def display_bible_df(df, filter_phrase):
    response = f'All bible verses that contain "{filter_phrase}":\n\n'
    for i, row in df.sample(frac=1).iterrows():
        verse = f"[{row['abbreviation']} {row['chapter']}, {row['verse']}] {row['text']}"
        if len(response + verse) > 4096:
            break
        response += f"[{row['abbreviation']} {row['chapter']}, {row['verse']}] {row['text']}\n\n"
    return response
