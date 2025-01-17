import copy
import dataclasses
import random
import re
import os
import logging
import pickle
import datetime
from datetime import timezone, timedelta

from zoneinfo import ZoneInfo
import pandas as pd

from definitions import CHAT_HISTORY_PATH, USERS_PATH, METADATA_PATH, CLEANED_CHAT_HISTORY_PATH, EmojiType, PeriodFilterMode, ArgType, NamedArgType, TIMEZONE
from src.core.utils import create_dir, parse_string, parse_number, parse_int, read_df, parse_user, parse_period, parse_named_args, merge_spaced_args
from src.models.command_args import CommandArgs

log = logging.getLogger(__name__)

negative_emojis = ['👎', '😢', '😭', '🤬', '🤡', '💩', '😫', '😩', '🥶', '🤨', '🧐', '🙃', '😒', '😠', '😣', '🗿']
MATCHING_USERNAME_THRESHOLD = 5


def load_metadata():
    """Load metadata pickle file as a dict. If it doesn't exist, check if the chat data exists, to extract some metadata out."""
    if not os.path.exists(METADATA_PATH):
        chat_df = read_df(CHAT_HISTORY_PATH)
        if chat_df is None:
            return {
                'last_message_id': None,
                'last_message_utc_timestamp': None,
                'last_message_dt': None,
                'last_update': None,
                'message_count': None,
                'new_latest_data': False
            }

        chat_df = chat_df.sort_values(by='message_id').reset_index(drop=True)
        return {
            'last_message_id': chat_df['message_id'].iloc[-1],
            'last_message_utc_timestamp': int(chat_df['timestamp'].iloc[-1].replace(tzinfo=timezone.utc).astimezone(tz=None).timestamp()),
            'last_message_dt': chat_df['timestamp'].iloc[-1],
            'last_update': None,
            'message_count': len(chat_df),
            'new_latest_data': False
        }
    with open(METADATA_PATH, 'rb', buffering=0) as f:
        return pickle.load(f)


def save_metadata(metadata):
    """Dump metadata dict to pickle file."""
    dir_path = os.path.split(METADATA_PATH)[0]
    create_dir(dir_path)
    with open(METADATA_PATH, 'wb', buffering=0) as f:
        pickle.dump(metadata, f, protocol=pickle.HIGHEST_PROTOCOL)


def read_chat_history():
    df = pd.read_parquet(CLEANED_CHAT_HISTORY_PATH).sort_values(by='timestamp').reset_index(drop=True)
    print(df.tail(10))
    return df


def read_users():
    return pd.read_parquet(USERS_PATH)


def create_empty_file(path):
    log.info(f"File {path} created.")
    open(path, 'a').close()


def remove_file(path):
    try:
        os.remove(path)
        log.info(f"{path} file removed.")
    except OSError:
        log.info(f"Can't remove {path}, fie doesn't exists.")


def escape_special_characters(text):
    special_characters = r'\-.()[{}_]:+!<>=#^|~$%'
    return re.sub(f'([{re.escape(special_characters)}])', r'\\\1', text)


def contains_stopwords(s, stopwords):
    return any(word in stopwords for word in s.split())


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
    if command_args.args_with_spaces:
        command_args.args = [command_args.joined_args]

    args_num = len(command_args.args)
    expected_args_num = len(command_args.expected_args)
    if args_num > expected_args_num:
        command_args.error = f"Invalid number of arguments. Expected {command_args.expected_args}, got {command_args.args}"
        return command_args

    # Handle optional args
    command_args, success = handle_optional_args(users_df, command_args)
    if not success:
        command_args.error = '\n'.join(command_args.errors).strip()
        return command_args

    # Parse args
    for i, arg in enumerate(command_args.args):
        arg_type = command_args.handled_expected_args[i]
        command_args = parse_arg(users_df, command_args, arg, arg_type)

    command_args.error = '\n'.join(command_args.errors).strip()
    return command_args


def handle_optional_args(users_df, command_args_ref: CommandArgs):
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
            continue

        for arg in command_args.args:
            command_args = parse_arg(users_df, command_args, arg, arg_type)

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


def parse_arg(users_df, command_args_ref, arg_str, arg_type: ArgType) -> CommandArgs:
    command_args = dataclasses.replace(command_args_ref)
    match arg_type:
        case ArgType.USER:
            command_args = parse_user(users_df, command_args, arg_str)
        case ArgType.PERIOD:
            command_args = parse_period(command_args, arg_str)
        case ArgType.POSITIVE_INT:
            _, command_args = parse_number(command_args, arg_str, positive_only=True)
        case ArgType.STRING:
            _, command_args = parse_string(command_args, arg_str)
        case _:
            command_args = command_args

    return command_args


def get_today_midnight_dt():
    return datetime.datetime.now().replace(tzinfo=ZoneInfo(TIMEZONE)).replace(hour=0, minute=0, second=0, microsecond=0)


def get_past_hr_dt(hours):
    return datetime.datetime.now(datetime.timezone.utc) - timedelta(hours=hours)


def get_dt_now():
    return datetime.datetime.now(datetime.timezone.utc)


def filter_df_in_range(df: pd.DataFrame, start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
    """ Filter dataframe in range of start_h and end_h"""
    return df[(df['timestamp'] >= start_dt) & (df['timestamp'] < end_dt)]


def filter_by_time_df(df, command_args):
    today_dt = get_today_midnight_dt()
    period_mode, period_time = command_args.period_mode, command_args.period_time

    print(command_args.dt, command_args.start_dt, command_args.end_dt, command_args.parse_error)
    match period_mode:
        case PeriodFilterMode.HOUR:
            log.info(f"UTC dt {period_time} hours ago: {get_past_hr_dt(period_time)}")
            return df[df['timestamp'] >= get_past_hr_dt(period_time)]
        case PeriodFilterMode.TODAY:
            return df[df['timestamp'] >= today_dt]
        case PeriodFilterMode.YESTERDAY:
            return df[(df['timestamp'] >= today_dt - timedelta(days=1)) & (df['timestamp'] < today_dt)]
        case PeriodFilterMode.WEEK:
            return df[df['timestamp'] >= today_dt - timedelta(days=7)]
        case PeriodFilterMode.MONTH:
            return df[df['timestamp'] >= today_dt - timedelta(days=30)]
        case PeriodFilterMode.YEAR:
            return df[df['timestamp'] >= today_dt - timedelta(days=365)]
        case PeriodFilterMode.TOTAL:
            return df.copy(deep=True)
        case PeriodFilterMode.DATE:
            return df[df['timestamp'].dt.date == command_args.dt.date()]
        case PeriodFilterMode.DATE_RANGE:
            return df[(df['timestamp'] >= command_args.start_dt) & (df['timestamp'] < command_args.end_dt)]


def filter_by_shifted_time_df(df, command_args):
    period_mode, period_time = command_args.period_mode, command_args.period_time

    today_dt = get_today_midnight_dt()
    dt_now = get_dt_now()
    log.info(f"Filter by period_mode: {period_mode}, period_time: {period_time}, midnight today: {today_dt}. but it's shifted.")
    match period_mode:
        case PeriodFilterMode.HOUR:
            start_dt = dt_now - timedelta(hours=period_time * 2)
            end_dt = dt_now - timedelta(hours=period_time)
            return filter_df_in_range(df, start_dt, end_dt)
        case PeriodFilterMode.TODAY:
            return filter_df_in_range(df, today_dt - timedelta(days=1), dt_now - timedelta(days=1))
        case PeriodFilterMode.YESTERDAY:
            return filter_df_in_range(df, today_dt - timedelta(days=2), today_dt - timedelta(days=1))
        case PeriodFilterMode.WEEK:
            return filter_df_in_range(df, dt_now - timedelta(days=14), dt_now - timedelta(days=7))
        case PeriodFilterMode.MONTH:
            return filter_df_in_range(df, dt_now - timedelta(days=60), dt_now - timedelta(days=30))
        case PeriodFilterMode.YEAR:
            return filter_df_in_range(df, dt_now - timedelta(days=365 * 2), dt_now - timedelta(days=365))
        case PeriodFilterMode.TOTAL:
            return df.copy(deep=True)
        case PeriodFilterMode.DATE:
            return filter_df_in_range(df, command_args.dt - timedelta(days=1), command_args.dt)
        case PeriodFilterMode.DATE_RANGE:
            days_diff = (command_args.end_dt - command_args.start_dt).days
            return filter_df_in_range(df, command_args.start_dt - timedelta(days=days_diff), command_args.start_dt)


def filter_emojis_by_emoji_type(df, emoji_type, col='reaction_emojis'):
    if emoji_type == EmojiType.NEGATIVE:
        df[col] = df[col].apply(lambda emojis: [emoji for emoji in emojis if emoji in negative_emojis])
    return df


def filter_emoji_by_emoji_type(df, emoji_type, col='emoji'):
    if emoji_type == EmojiType.NEGATIVE:
        df = df[df[col].isin(negative_emojis)]
        # df = df[df[col] is not None]
    return df


def emoji_sentiment_to_label(emoji_type: EmojiType):
    """Convert emoji_type to a message label."""
    match emoji_type:
        case EmojiType.ALL:
            return 'Top'
        case EmojiType.NEGATIVE:
            return 'Top Sad'


def dt_to_str(dt):
    return dt.strftime('%d-%m-%Y %H:%M')


def check_bot_messages(message_ids: list, bot_id: int) -> bool:
    """Check if bot messages are present in chat history."""
    chat_df = read_df(CHAT_HISTORY_PATH)
    filtered_df = chat_df[chat_df['message_id'].isin(message_ids)]
    non_bot_messages_df = filtered_df[filtered_df['user_id'] != bot_id]
    bot_messages_df = filtered_df[filtered_df['user_id'] == bot_id]

    return non_bot_messages_df.empty and len(message_ids) == len(bot_messages_df)


def check_new_username(users_df, new_username):
    """Check during setting new username whether the new username is valid."""
    allowed_characters = "aąbcćdeęfghijklłmnńoóprsśtuwyzźż0123456789_ "

    error = ''
    forbidden_usernames = get_forbidden_usernames()
    new_username_lower = new_username.lower()
    new_username_prefix = new_username_lower[:min(MATCHING_USERNAME_THRESHOLD, len(new_username_lower))]
    usernames = users_df['final_username'].tolist()
    matching_usernames = [username for username in usernames if new_username_prefix == username.lower()[:len(new_username_prefix)]]

    if new_username in usernames:
        error = "User with this display name already exists! Choose a different one u dummy. (Wiem, że to ty kuba)"

    if matching_usernames:
        error = f"Users with similar names, like: *{'*, *'.join(matching_usernames)}* - already exist! First {MATCHING_USERNAME_THRESHOLD} letters should be unique."

    if not are_text_characters_allowed(new_username_lower, allowed_characters):
        error = "Username can only contain ASCII letters, numbers, '_' and ' '."

    if new_username_lower in forbidden_usernames:
        error = "This username is forbidden. Please choose a different one."

    if error != '':
        log.error(error)
        return False, error

    return True, ''

def are_text_characters_allowed(text, characters_filter):
    return all(c in characters_filter for c in text)

def is_alpha_numeric(text):
    return any(not c.isalnum() for c in text)


def enum_to_list(enum):
    return [member.value for member in enum]


def get_forbidden_usernames():
    return enum_to_list(PeriodFilterMode)


def generate_random_file_id():
    # return f"{datetime.datetime.now()}_{random.randint(1000, 9999)}"
    return f"{random.randint(10000000000, 100000000000)}"


def generate_random_filename(extension):
    return f"{generate_random_file_id()}.{extension}"


def username_to_user_id(users_df, username):
    return users_df[users_df['final_username'] == username].iloc[0]['user_id']

def is_list_column(series):
    return series.apply(lambda x: isinstance(x, list)).all()

def is_string_column(series):
    return series.apply(lambda x: isinstance(x, str)).all()

def validate_schema(df, schema):
    log.info(f'Validating schema: {schema.name}')
    if df is not None and not df.empty:
        schema(df)
