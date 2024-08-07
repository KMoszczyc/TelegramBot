import copy
import dataclasses
import re
import os
import logging
import pickle
import datetime
from datetime import timezone, timedelta

from zoneinfo import ZoneInfo
import pandas as pd

from definitions import CHAT_HISTORY_PATH, USERS_PATH, METADATA_PATH, CLEANED_CHAT_HISTORY_PATH, EmojiType, PeriodFilterMode, ArgType
from src.core.utils import create_dir
from src.models.command_args import CommandArgs

log = logging.getLogger(__name__)

negative_emojis = ['👎', '😢', '😭', '🤬', '🤡', '💩', '😫', '😩', '🥶', '🤨', '🧐', '🙃', '😒', '😠', '😣', '🗿']
MAX_INT = 24 * 365 * 20
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


def save_df(df, path):
    dir_path = os.path.split(path)[0]
    create_dir(dir_path)
    df.to_parquet(path)


def read_chat_history():
    df = pd.read_parquet(CLEANED_CHAT_HISTORY_PATH).sort_values(by='timestamp').reset_index(drop=True)
    print(df.tail(10))
    return df


def read_df(path):
    return pd.read_parquet(path) if os.path.exists(path) else None


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
    special_characters = r'\-.()[{}_]:+!<>'
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
    args_num = len(command_args.args)
    expected_args_num = len(command_args.expected_args)

    command_args.joined_args = ' '.join(command_args.args)
    if command_args.args_with_spaces:
        command_args.args = command_args.joined_args.split('|')

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
            if command_args.errors[i] != '':
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
        case ArgType.NUMBER:
            command_args = parse_number(command_args, arg_str)
        case ArgType.STRING:
            command_args = parse_string(command_args, arg_str)
        case _:
            command_args = command_args

    return command_args


def parse_number(command_args, arg_str) -> CommandArgs:
    if arg_str == '':
        return command_args

    number, error = parse_int(arg_str)
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


def parse_period(command_args, arg_str) -> CommandArgs:
    if arg_str == '':
        error = "Period cannot be empty."
        command_args.errors.append(error)
        log.error(error)
        return command_args

    period_mode_str = arg_str
    try:
        if 'h' in arg_str and has_numbers(arg_str):
            command_args.period_time, command_args.parse_error = parse_int(arg_str.replace('h', ''))
            period_mode_str = 'hour'
        if command_args.parse_error == '':
            command_args.period_mode = PeriodFilterMode(period_mode_str)
    except ValueError:
        error = f"There is no such time period as {arg_str}."
        command_args.errors.append(error)
        log.error(error)

    if command_args.parse_error != '':
        command_args.errors.append(command_args.parse_error)
        command_args.period_mode = PeriodFilterMode.ERROR
    else:
        command_args.errors.append('')
    return command_args


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
    elif len(user_str) >= 3 and not partially_matching_users.empty:
        command_args.user = partially_matching_users.iloc[0]['final_username']
    else:
        error = f"User {user_str} doesn't exist and cannot hurt you. Existing users are: {users_df['final_username'].tolist()}"
        command_args.errors.append(error)
        log.error(command_args.error)
        return command_args

    command_args.errors.append('')
    return command_args


def get_today_midnight_dt():
    return datetime.datetime.now().replace(tzinfo=ZoneInfo('Europe/Warsaw')).replace(hour=0, minute=0, second=0, microsecond=0)


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
    log.info(f"Filter by period_mode: {period_mode}, period_time: {period_time}, midnight today: {today_dt}")

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


def filter_emojis_by_emoji_type(df, emoji_type, col='reaction_emojis'):
    if emoji_type == EmojiType.NEGATIVE:
        df[col] = df[col].apply(lambda emojis: [emoji for emoji in emojis if emoji in negative_emojis])
    return df


def filter_emoji_by_emoji_type(df, emoji_type, col='emoji'):
    if emoji_type == EmojiType.NEGATIVE:
        df = df[df[col].isin(negative_emojis)]
        # df = df[df[col] is not None]
    return df


def extract_int(num_str):
    return parse_int(''.join(filter(str.isdigit, num_str)))


def has_numbers(num_str):
    return any(char.isdigit() for char in num_str)


def parse_int(num_str):
    error = ''
    num = None
    try:
        num = int(num_str)
        # print(num, MAX_INT)
        if num > MAX_INT:
            error = f"Kuba's dick is too big ({x_to_light_years_str(num)}), make it smaller!"
            log.error(error)
        if num < 0:
            error = "Number cannot be negative!"
            num = -1
            log.error(error)
    except ValueError:
        error = f"{num_str} is not a number."
        log.error(error)

    return num, error


def emoji_sentiment_to_label(emoji_type: EmojiType):
    """Convert emoji_type to a message label."""
    match emoji_type:
        case EmojiType.ALL:
            return 'Top'
        case EmojiType.NEGATIVE:
            return 'Top Sad'


def dt_to_str(dt):
    return dt.strftime('%d-%m-%Y %H:%M')


def x_to_light_years_str(x):
    """Kinda to last years, keep small numbers the same."""
    if x < 10000000:
        return str(x)

    ly = x / 9460730472580.8
    ly = round(ly, 6) if ly < 1 else round(ly, 2)
    return f'{ly} light years'


def check_bot_messages(message_ids: list, bot_id: int) -> bool:
    """Check if bot messages are present in chat history."""
    chat_df = read_df(CHAT_HISTORY_PATH)
    filtered_df = chat_df[chat_df['message_id'].isin(message_ids)]
    non_bot_messages_df = filtered_df[filtered_df['user_id'] != bot_id]
    bot_messages_df = filtered_df[filtered_df['user_id'] == bot_id]

    return non_bot_messages_df.empty and len(message_ids) == len(bot_messages_df)


def parse_string(command_args: CommandArgs, text: str) -> CommandArgs:
    error = ''
    if len(text) < command_args.min_string_length:
        error = f'{command_args.label} {text} is too short, it should have at least {command_args.min_string_length} characters.'
    if len(text) > command_args.max_string_length:
        error = f'{command_args.label} {text} is too long, it should have {command_args.max_string_length} characters or less.'

    command_args.errors.append(error)
    command_args.string = text
    return command_args


def check_new_username(users_df, new_username, current_username):
    """Check during setting new username whether the new username is valid."""
    error = ''
    new_username_lower = new_username.lower()
    new_username_prefix = new_username_lower[:min(MATCHING_USERNAME_THRESHOLD, len(new_username_lower))]
    usernames = users_df['final_username'].tolist()
    matching_usernames = [username for username in usernames if new_username_prefix == username.lower()[:len(new_username_prefix)]]

    if new_username in usernames:
        error = f"Username *{current_username}* not changed to *{new_username}*. User with this display name already exists! Choose a different one u dummy. (Wiem, że to ty kuba)"

    if matching_usernames:
        error = f"Username *{current_username}* not changed to *{new_username}*. Users with similar names, like: *{'*, *'.join(matching_usernames)}* - already exist! First {MATCHING_USERNAME_THRESHOLD} letters should be unique."

    if is_alpha_numeric(new_username):
        error = f"Username *{current_username}* not changed to *{new_username}*. Username can only contain letters and numbers."

    if error != '':
        log.error(error)
        return False, error

    return True, ''


def is_alpha_numeric(text):
    return any(not c.isalnum() for c in text)
