import copy
import locale
import os
import logging
import random
import re
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

from definitions import ArgType, MessageType, CHAT_IMAGES_DIR_PATH, CHAT_VIDEOS_DIR_PATH, CHAT_GIFS_DIR_PATH, CHAT_AUDIO_DIR_PATH, PeriodFilterMode, TIMEZONE, DatetimeFormat
from src.models.command_args import CommandArgs

log = logging.getLogger(__name__)
MAX_INT = 24 * 365 * 20
if sys.platform == 'win32':
    locale.setlocale(locale.LC_ALL, 'Polish_Poland')
else:
    locale.setlocale(locale.LC_ALL, 'pl_PL.UTF-8')


def read_str_file(path):
    with open(path, 'r') as f:
        lines = f.read().splitlines()
    return lines


def create_dir(path):
    if os.path.isdir(path):
        return

    os.makedirs(path)
    log.info(f'Created directory in path: {path}')


def read_df(path):
    return pd.read_parquet(path) if os.path.exists(path) else None


def save_df(df, path):
    dir_path = os.path.split(path)[0]
    create_dir(dir_path)
    df.to_parquet(path)


def preprocess_input(users_df: pd.DataFrame, command_args: CommandArgs):
    command_args = parse_args(users_df, command_args)
    filtered_phrases, command_args = filter_phrases(command_args)
    return filtered_phrases, command_args


# def parse_args(users_df: pd.DataFrame, command_args: CommandArgs) -> CommandArgs:
#     command_args = merge_spaced_args(command_args)
#     command_args = parse_named_args(users_df, command_args)
#     command_args.joined_args = ' '.join(command_args.args)
#     command_args.joined_args_lower = ' '.join(command_args.args).lower()
#     command_args.arg_type = ArgType.REGEX if is_inside_square_brackets(command_args.joined_args) else ArgType.TEXT
#     command_args.error = get_error(command_args)
#
#     return command_args

def parse_args(users_df, command_args: CommandArgs) -> CommandArgs:
    """
    A function to parse arguments and return a tuple with period mode, mode time, user, and error.
    Parameters:
        users_df: DataFrame - The DataFrame containing user data.
        command_args: CommandArgs - Dataclass with the command arguments related data

    Returns:
        command_args: Dataclass with the command arguments related data
    """
    command_args = merge_spaced_args(command_args)
    command_args = parse_named_args(users_df, command_args)
    command_args.joined_args = ' '.join(command_args.args)
    command_args.joined_args_lower = ' '.join(command_args.args).lower()
    if command_args.is_text_arg:
        command_args.args = [command_args.joined_args]
        command_args.arg_type = ArgType.REGEX if is_inside_square_brackets(command_args.joined_args) else ArgType.TEXT
        return command_args

    args_num = len(command_args.args)
    expected_args_num = len(command_args.expected_args)
    if not command_args.optional and args_num != expected_args_num:
        command_args.error = f"Invalid number of arguments. Expected {command_args.expected_args}, got {command_args.args}"
        return command_args

    # Handle args
    command_args, success = handle_args(users_df, command_args)
    command_args.error = get_error(command_args)
    return command_args

    # # Parse args
    # for i, arg_type in enumerate(command_args.handled_expected_args):
    #     arg = ' '.join(command_args.args[i:]) if arg_type == ArgType.TEXT_MULTISPACED else command_args.args[i]
    #     _, command_args = parse_arg(users_df, command_args, arg, arg_type)

    # command_args.error = get_error(command_args)
    # return command_args


def handle_args(users_df, command_args_ref: CommandArgs):
    """Handle optional arguments like Period or User."""
    if len(command_args_ref.args) == 0 or len(command_args_ref.args) == len(command_args_ref.expected_args) or sum(command_args_ref.optional) == 0:
        command_args_ref.handled_expected_args = command_args_ref.expected_args
        return command_args_ref, True

    command_args = copy.deepcopy(command_args_ref)
    successes = []
    expected_args = command_args.expected_args.copy()
    handled_expected_args = command_args.expected_args.copy()
    for i, arg_type in enumerate(expected_args):
        if not command_args.optional[i]:
            arg = ' '.join(command_args.args[i:]) if arg_type == ArgType.TEXT_MULTISPACED else command_args.args[i]
            _, command_args = parse_arg(users_df, command_args, arg, arg_type)
            continue

        # handle optional arg
        for arg in command_args.args:
            _, command_args = parse_arg(users_df, command_args, arg, arg_type)

            if command_args.errors[-1] != '':
                handled_expected_args.remove(arg_type)
                successes.append(False)
            else:
                successes.append(True)

    if sum(successes) == len(command_args.args):
        log.info("All optional args were parsed successfully.")
        command_args_ref.handled_expected_args = handled_expected_args
        return command_args_ref, True

    log.info("None of the optional args were parsed successfully, despite there being an argument send by user.")
    return command_args, False


def merge_spaced_args(command_args: CommandArgs):
    joined_args = ' '.join(command_args.args)
    new_args = []
    quotation_opened = False
    current_spaced_args = []
    for arg in command_args.args:
        if "\"" in arg and not quotation_opened:
            quotation_opened = True
            current_spaced_args.append(arg.replace('"', ''))
        elif "\"" in arg and quotation_opened:
            current_spaced_args.append(arg.replace('"', ''))
            new_args.append(' '.join(current_spaced_args))
            quotation_opened = False
            current_spaced_args = []
        elif quotation_opened:
            current_spaced_args.append(arg)
        else:
            new_args.append(arg)
    if len(current_spaced_args) == 1:
        new_args.append(current_spaced_args[0])
    command_args.args = new_args

    return command_args


def filter_phrases(command_args: CommandArgs):
    log.info(f'Command received: {command_args.arg_type} - {command_args.joined_args}')
    match command_args.arg_type:
        case ArgType.TEXT:
            return text_filter(command_args)
        case ArgType.REGEX:
            return regex_filter(command_args)


def is_word_in_list_of_multiple_words(word, list_of_multiple_words):
    word_lower = word.lower()
    return any(word_lower for words in list_of_multiple_words if word_lower in words.lower())


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


def are_you_lucky(user_id, with_args=False):
    today = int(datetime.now().strftime('%Y%m%d'))
    user_hash = user_id + today
    random.seed(user_hash)
    rand_value = random.random()

    if rand_value < 0.1:
        message = 'Nie. 🗿' if with_args else "Dzisiaj masz wielkiego pecha. Lepiej zostań w domu i nic nie rób. (łeee jestem grzybem ;-;)"
    elif rand_value < 0.3:
        message = 'Raczej nie.' if with_args else "Dzisiaj masz lekkiego pecha. Zachowaj ostrożność."
    elif rand_value < 0.7:
        message = 'Rabini są niezdecydowani w tej kwestii.' if with_args else "Normalny dzień dla normalnego chłopa."
    elif rand_value < 0.9:
        message = 'Raczej tak.' if with_args else "Dzisiaj masz lekkie szczęście. Możesz spróbować coś zrobić, ale może się to nie powieść."
    else:
        message = 'Tak. 🗿' if with_args else "Dzisiaj masz ogromne szczęście! Wyjdź z domu i spróbuj zrobić coś nowego, na pewno Ci się uda!"

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


def parse_arg(users_df, command_args_ref, arg_str, arg_type: ArgType) -> tuple[str | int, CommandArgs]:
    command_args = copy.deepcopy(command_args_ref)
    value = None
    match arg_type:
        case ArgType.USER:
            command_args = parse_user(users_df, command_args, arg_str)
        case ArgType.PERIOD:
            command_args = parse_period(command_args, arg_str)
        case ArgType.POSITIVE_INT:
            value, command_args = parse_number(command_args, arg_str, positive_only=True)
        case ArgType.STRING | ArgType.TEXT | ArgType.TEXT_MULTISPACED:
            value, command_args = parse_string(command_args, arg_str)
        case _:
            command_args = command_args

    return value, command_args


def parse_named_args(users_df, command_args_ref: CommandArgs):
    command_args = copy.deepcopy(command_args_ref)
    command_args.args = [arg.replace('—', '--') for arg in command_args.args]
    if not command_args.available_named_args_aliases:
        command_args.available_named_args_aliases = {arg[0]: arg for arg in command_args.available_named_args}
    args = copy.deepcopy(command_args.args)
    for i, arg in enumerate(args):
        named_arg = parse_named_arg(arg, command_args)
        if named_arg is None:
            continue
        if command_args.available_named_args[named_arg] == ArgType.NONE:
            command_args.named_args[named_arg] = None
        elif i + 1 < len(args) and not is_named_arg(args[i + 1], command_args):  # this arg has a value
            arg_type = command_args.available_named_args[named_arg]
            value, command_args = parse_arg(users_df, command_args, args[i + 1], arg_type)
            command_args.args.remove(args[i + 1])
            if get_error(command_args) == '':
                command_args.named_args[named_arg] = value
        else:
            command_args.errors.append(f'Argument {named_arg} requires a value')
        command_args.args.remove(arg)

    command_args.error = get_error(command_args)
    return command_args


def parse_named_arg(arg, command_args):
    if is_normal_named_arg(arg, command_args.available_named_args):
        return arg.replace('-', '')
    elif is_aliased_named_arg(arg, command_args.available_named_args_aliases):
        alias = arg.replace('-', '')
        return command_args.available_named_args_aliases[alias]
    return None


def is_aliased_named_arg(arg, shortened_available_named_args):
    return arg.startswith('-') and arg[1:] in shortened_available_named_args


def is_normal_named_arg(arg, available_named_args):
    return arg.startswith('--') and arg[2:] in available_named_args


def is_named_arg(arg, commands_args):
    return is_aliased_named_arg(arg, commands_args.available_named_args_aliases) or is_normal_named_arg(arg, commands_args.available_named_args)


def parse_period(command_args, arg_str) -> CommandArgs:
    error = ''
    if arg_str == '':
        error = "Period cannot be empty."
        command_args.errors.append(error)
        log.error(error)
        return command_args

    period_mode_str = arg_str
    try:
        if 's' in arg_str and has_numbers(arg_str):
            command_args.period_time, error = parse_int(arg_str.replace('s', ''), positive_only=True)
            period_mode_str = 'second'
        elif 'm' in arg_str and has_numbers(arg_str):
            command_args.period_time, error = parse_int(arg_str.replace('m', ''), positive_only=True)
            period_mode_str = 'minute'
        elif 'h' in arg_str and has_numbers(arg_str):
            command_args.period_time, error = parse_int(arg_str.replace('h', ''), positive_only=True)
            period_mode_str = 'hour'
        elif 'd' in arg_str and has_numbers(arg_str):
            command_args.period_time, error = parse_int(arg_str.replace('d', ''), positive_only=True)
            period_mode_str = 'day'
        elif 'w' in arg_str and has_numbers(arg_str):
            command_args.period_time, error = parse_int(arg_str.replace('w', ''), positive_only=True)
            period_mode_str = 'week'

        if error == '':
            command_args.period_mode = PeriodFilterMode(period_mode_str)

        if command_args.period_mode == PeriodFilterMode.ERROR and ';' in arg_str:
            command_args.start_dt, command_args.end_dt, command_args.dt_format, error = parse_date_range(arg_str)
            command_args.period_mode = PeriodFilterMode.DATE_RANGE
        elif command_args.period_mode == PeriodFilterMode.ERROR:
            command_args.dt, command_args.dt_format, error = parse_date(arg_str)
            command_args.period_mode = PeriodFilterMode.DATE

        command_args.parse_error = error
    except ValueError:
        command_args.parse_error = f"There is no such time period as {arg_str}."
        command_args.errors.append(command_args.parse_error)
        log.error(command_args.parse_error)

    if command_args.parse_error != '':
        command_args.errors.append(command_args.parse_error)
        command_args.period_mode = PeriodFilterMode.ERROR
    else:
        command_args.errors.append('')

    command_args.error = get_error(command_args)
    return command_args


def parse_date(date_str: str) -> tuple[datetime, DatetimeFormat, str] | tuple[None, None, str]:
    dt_formats = [
        DatetimeFormat.DATE,
        DatetimeFormat.HOUR,
        DatetimeFormat.MINUTE,
        DatetimeFormat.SECOND,
    ]

    for dt_format in dt_formats:
        try:
            return datetime.strptime(date_str, dt_format.value).replace(tzinfo=ZoneInfo(TIMEZONE)), dt_format, ''
        except ValueError:
            pass
    return None, None, f"Could not parse date: {date_str}"


def parse_date_range(date_range_str: str) -> tuple[datetime, datetime, DatetimeFormat, str] | tuple[None, None, None, str]:
    date_range_split = date_range_str.split(';')
    if len(date_range_split) != 2:
        error = f"Could not parse date range: {date_range_str}"
        return None, None, None, error
    start_date, dt_format, start_date_error = parse_date(date_range_split[0])
    end_date, dt_format, end_date_error = parse_date(date_range_split[1])
    error = start_date_error + end_date_error

    if error == '' and start_date > end_date:
        error = 'The start date cannot be after the end date of the range u dummy!'

    return start_date, end_date, dt_format, error


def parse_user(users_df, command_args, arg_str) -> CommandArgs:
    if arg_str == '':
        error = "User cannot be empty."
        command_args.errors.append(error)
        log.error(command_args.error)
        return command_args

    user_str = arg_str.replace('@', '')

    exact_matching_users = users_df[users_df['final_username'].str.lower() == user_str.lower()]
    partially_matching_users = users_df[users_df['final_username'].str.contains(user_str, case=False)]

    if not exact_matching_users.empty:
        command_args.user = exact_matching_users.iloc[0]['final_username']
        command_args.user_id = exact_matching_users.index[0]
    elif len(user_str) >= 3 and not partially_matching_users.empty:
        command_args.user = partially_matching_users.iloc[0]['final_username']
        command_args.user_id = partially_matching_users.index[0]
    else:
        error = f"User {user_str} doesn't exist and cannot hurt you. Existing users are: {users_df['final_username'].tolist()}"
        command_args.errors.append(error)
        log.error(command_args.error)
        return command_args

    command_args.errors.append('')
    return command_args


def parse_number(command_args, arg_str, positive_only=False) -> tuple[int, CommandArgs]:
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
    return number, command_args


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


def parse_string(command_args: CommandArgs, text: str) -> tuple[str, CommandArgs]:
    error = ''
    if len(text) < command_args.min_string_length:
        error = f'{command_args.label} {text} is too short, it should have at least {command_args.min_string_length} characters.'
    if len(text) > command_args.max_string_length:
        error = f'{command_args.label} {text} is too long, it should have {command_args.max_string_length} characters or less.'

    command_args.errors.append(error)
    command_args.string = text
    return text, command_args


def display_shopping_sunday(dt):
    return dt.strftime('%d %B')


def display_bible_df(df, bot_state, label='Filtered bible verses', show_siglum=True):
    response = f'{label}:\n\n'
    for i, row in df.iterrows():
        verse = f"[{get_siglum(row)}] {row['text']}\n\n" if show_siglum else f"{row['verse']}. {row['text']}\n"
        if len(response + verse) > 4096:
            break
        response += verse
        bot_state.last_bible_verse_id = row.name

    return response


def get_siglum(row):
    return f"{row['abbreviation']} {row['chapter']}, {row['verse']}"


def get_full_siglum(row):
    return f"{row['book']} {row['chapter']}, {row['verse']}"


def get_bible_map(bible_df):
    return bible_df.drop_duplicates('book')[['book', 'abbreviation']].set_index('abbreviation')


def datetime_to_ms(dt):
    return int(dt.timestamp() * 1000)


def match_substr_to_list_of_texts(substr: str, texts: list, lower_case: bool = True) -> str:
    if lower_case:
        matched_texts = [text for text in texts if substr.lower() in text.lower()]
    else:
        matched_texts = [text for text in texts if substr in text]
    return matched_texts[0] if matched_texts else None


def get_username(first_name, last_name):
    username = first_name if first_name is not None else ''
    if last_name is not None:
        username += f' {last_name}'
    return username.strip()


def has_numbers(num_str):
    return any(char.isdigit() for char in num_str)


def file_exists(path):
    return os.path.exists(path)


def text_to_number(text):
    numbers = [ord(character) for character in text]
    return sum(numbers)


def generate_period_headline(command_args):
    match command_args.period_mode:
        case PeriodFilterMode.HOUR:
            return f"past {command_args.period_time}h"
        case PeriodFilterMode.DATE:
            return command_args.dt.strftime(command_args.dt_format.value)
        case PeriodFilterMode.DATE_RANGE:
            return f"{command_args.start_dt.strftime(command_args.dt_format.value)} - {command_args.end_dt.strftime(command_args.dt_format.value)}"
        case _:
            return command_args.period_mode.value


def get_dt_now():
    return datetime.now(ZoneInfo(TIMEZONE))


def period_offset_to_dt(command_args):
    dt_now = get_dt_now()
    match command_args.period_mode:
        case PeriodFilterMode.SECOND:
            return dt_now + timedelta(seconds=command_args.period_time), ''
        case PeriodFilterMode.MINUTE:
            return dt_now + timedelta(minutes=command_args.period_time), ''
        case PeriodFilterMode.HOUR:
            return dt_now + timedelta(hours=command_args.period_time), ''
        case PeriodFilterMode.DAY:
            return dt_now + timedelta(days=command_args.period_time), ''
        case PeriodFilterMode.WEEK:
            return dt_now + timedelta(weeks=command_args.period_time), ''
        case PeriodFilterMode.DATE:
            return command_args.dt, ''
        case _:
            return None, 'Wrong period offset. Use one of the following: second, minute, hour, day, week, date'


async def send_response_message(context, chat_id, message_id, message):
    await context.bot.send_message(chat_id=chat_id, reply_to_message_id=message_id, text=message)


def dt_to_pretty_str(dt):
    return dt.strftime("%d-%m-%Y %H:%M:%S")
