import os
import logging
import random
import re
from datetime import datetime

from definitions import ArgType, MessageType, CHAT_IMAGES_DIR_PATH, CHAT_VIDEOS_DIR_PATH, CHAT_GIFS_DIR_PATH, CHAT_AUDIO_DIR_PATH
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


def are_you_lucky(user_id):
    today = int(datetime.now().strftime('%Y%m%d'))
    user_hash = user_id + today
    random.seed(user_hash)
    rand_value = random.random()

    if rand_value < 0.2:
        message = "Dzisiaj masz pecha :("
    elif rand_value < 0.8:
        message = "Normalny dzień dla normalnego chłopa."
    else:
        message = "Dzisiaj masz szczęście!"

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
