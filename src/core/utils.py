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

from definitions import ArgType, MessageType, CHAT_IMAGES_DIR_PATH, CHAT_VIDEOS_DIR_PATH, CHAT_GIFS_DIR_PATH, CHAT_AUDIO_DIR_PATH, PeriodFilterMode, TIMEZONE, DatetimeFormat, HolyTextType, SiglumType, \
    quran_df
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
    command_args = handle_args(users_df, command_args)
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
    if len(command_args_ref.args) == 0:
        return command_args_ref

    command_args = copy.deepcopy(command_args_ref)
    successes = []
    expected_args = command_args.expected_args.copy()
    for i, arg_type in enumerate(expected_args):
        if not command_args.optional[i]:
            arg = ' '.join(command_args.args[i:]) if arg_type == ArgType.TEXT_MULTISPACED else command_args.args[i]
            _, command_args = parse_arg(users_df, command_args, arg, arg_type, is_optional=False)
            continue

        if sum(successes) == len(command_args.args):
            continue

        # handle optional arg
        for arg in command_args.args:
            _, command_args = parse_arg(users_df, command_args, arg, arg_type, is_optional=True)
            if command_args.optional_errors[-1] != '':
                successes.append(False)
            else:
                successes.append(True)

    if not any(successes):
        log.info("None optional args were parsed successfully, despite there being an argument send by user.")
        command_args.errors.extend(command_args.optional_errors)
        return command_args

    log.info("All args were parsed successfully.")
    return command_args


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


def parse_arg(users_df, command_args_ref, arg_str, arg_type: ArgType, is_optional=False) -> tuple[str | int, CommandArgs]:
    command_args = copy.deepcopy(command_args_ref)
    value = None
    error = ''
    match arg_type:
        case ArgType.USER:
            command_args, error = parse_user(users_df, command_args, arg_str)
        case ArgType.PERIOD:
            command_args, error = parse_period(command_args, arg_str)
        case ArgType.POSITIVE_INT:
            value, command_args, error = parse_number(command_args, arg_str, positive_only=True)
        case ArgType.STRING | ArgType.TEXT | ArgType.TEXT_MULTISPACED:
            value, command_args, error = parse_string(command_args, arg_str)
        case _:
            command_args = command_args

    if is_optional:
        command_args.optional_errors.append(error)
    else:
        command_args.errors.append(error)

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


def parse_period(command_args, arg_str) -> [CommandArgs, str]:
    error = ''
    if arg_str == '':
        error = "Period cannot be empty."
        log.error(error)
        return command_args, error

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
        error = f"There is no such time period as {arg_str}."
        log.error(error)

    if error != '':
        command_args.period_mode = PeriodFilterMode.ERROR

    return command_args, error


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


def parse_user(users_df, command_args, arg_str) -> [CommandArgs, str]:
    if arg_str == '':
        error = "User cannot be empty."
        # command_args.errors.append(error)
        log.error(error)
        return command_args, error

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
        log.error(error)
        return command_args, error

    return command_args, ''


def parse_number(command_args, arg_str, positive_only=False) -> [int, CommandArgs, str]:
    if arg_str == '':
        return None, command_args, ''

    number, error = parse_int(arg_str, positive_only)
    if error != '':
        return None, command_args, error

    if number > command_args.number_limit:
        error = f"Given number is too big ({x_to_light_years_str(number)}), make it smaller!"
        log.error(error)
        return number, command_args, error

    command_args.number = number
    return number, command_args, ''


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


def parse_string(command_args: CommandArgs, text: str) -> [str, CommandArgs, str]:
    error = ''
    if len(text) < command_args.min_string_length:
        error = f'{command_args.label} {text} is too short, it should have at least {command_args.min_string_length} characters.'
    if len(text) > command_args.max_string_length:
        error = f'{command_args.label} {text} is too long, it should have {command_args.max_string_length} characters or less.'

    if '&' in text:  # user for 'AND' filtering
        command_args.strings = text.split('&')
    else:
        command_args.string = text
    return text, command_args, error


def display_shopping_sunday(dt):
    return dt.strftime('%d %B')


def display_holy_text_df(df, bot_state, holy_text_type, label='Filtered bible verses', show_siglum=True):
    response = f'{label}:\n\n'
    for i, row in df.iterrows():
        verse = f"[{get_siglum(row, holy_text_type, siglum_type=SiglumType.SHORT)}] {row['text']}\n\n" if show_siglum else f"{row['verse']}. {row['text']}\n"
        if len(response + verse) > 4096:
            break
        response += verse
        bot_state.set_holy_text_last_verse_id(row.name, holy_text_type)

    return response


def get_siglum(row, holy_text_type: HolyTextType, siglum_type: SiglumType) -> str:
    if holy_text_type == HolyTextType.BIBLE:
        return get_bible_siglum(row, siglum_type)
    elif holy_text_type == HolyTextType.QURAN:
        return get_quran_siglum(row, siglum_type)


def get_bible_siglum(row, siglum_type: SiglumType) -> str:
    if siglum_type == SiglumType.FULL:
        return f"{row['book']} {row['chapter']}, {row['verse']}"
    elif siglum_type == SiglumType.SHORT:
        return f"{row['abbreviation']} {row['chapter']}, {row['verse']}"


def get_quran_siglum(row, siglum_type: SiglumType):
    if siglum_type == SiglumType.FULL:
        return f"Sura {row['chapter_nr']}. {row['chapter_name']}, {row['verse']}"
    elif siglum_type == SiglumType.SHORT:
        return f"{row['chapter_nr']}:{row['verse']}"


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


def regexify_multiword_filter(words):
    base = r'^{}'
    expr = '(?=.*{})'
    return base.format(''.join(expr.format(w) for w in words))


def parse_quran_verse_arg(arg, bot_state, holy_text_type) -> [str, str]:
    arg_split = arg.split(':')
    if len(arg_split) != 2:
        return '', 'Failed to parse the --verse argument.'

    chapter_arg, verse_arg = arg_split[0].lower(), arg_split[1].lower()
    chapter_nums = quran_df.drop_duplicates('chapter_nr')['chapter_nr'].tolist()
    chapter_names = quran_df.drop_duplicates('chapter_name')['chapter_name'].tolist()

    matching_chapter_nums = [num for num in chapter_nums if str(num) == chapter_arg]
    matching_chapter_names = [chapter_name for chapter_name in chapter_names if chapter_arg in chapter_name.lower()]

    if matching_chapter_nums:
        chapter_nr = matching_chapter_nums[0]
        matching_verse_df = quran_df[(quran_df['chapter_nr'] == chapter_nr) & (quran_df['verse'] == verse_arg)]
        row = None if matching_verse_df.empty else matching_verse_df.iloc[0]
    elif matching_chapter_names:
        chapter_name = matching_chapter_names[0]
        matching_verse_df = quran_df[(quran_df['chapter_name'] == chapter_name) & (quran_df['verse'] == verse_arg)]
        row = None if matching_verse_df.empty else matching_verse_df.iloc[0]
    else:
        return '', f"Verse {chapter_arg}:{verse_arg} doesn't exist in Quran."

    if row is None:
        return '', f"Verse {chapter_arg}:{verse_arg} doesn't exist in Quran."

    bot_state.set_holy_text_last_verse_id(row.name, holy_text_type)
    response = f"[{get_siglum(row, holy_text_type, SiglumType.SHORT)}] {row['text']}"
    return response, ''
