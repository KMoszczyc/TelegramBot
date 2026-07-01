import json
import locale
import logging
import os
import random
import string
import sys
import uuid
from collections import defaultdict
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import telegram
from telegram import BotCommand, Update
from telegram.ext import ContextTypes

from src.config.constants import TIMEZONE
from src.config.enums import ArgType, ErrorMessage, HolyTextType, LuckyScoreType, MessageType, PeriodFilterMode, SiglumType
from src.config.paths import (
    CHAT_AUDIO_DIR_PATH,
    CHAT_GIFS_DIR_PATH,
    CHAT_IMAGES_DIR_PATH,
    CHAT_VIDEO_NOTES_DIR_PATH,
    CHAT_VIDEOS_DIR_PATH,
    COMMANDS_PATH,
)
from src.core.arg_parser import ArgParser
from src.models.command_args import CommandArgs

log = logging.getLogger(__name__)

if sys.platform == "win32":
    locale.setlocale(locale.LC_ALL, "Polish_Poland")
else:
    locale.setlocale(locale.LC_ALL, "pl_PL.UTF-8")


def read_str_file(path):
    with open(path) as f:
        lines = f.read().splitlines()
    return lines


def get_bot_commands(path=COMMANDS_PATH):
    lines = read_str_file(path)
    commands = []
    for line in lines:
        line = line.strip()
        if not line or " - " not in line:
            continue
        cmd, desc = line.split(" - ", 1)
        commands.append(BotCommand(command=cmd.strip().lower()[:32], description=desc.strip()[:256]))
    return commands


def create_dir(path):
    if os.path.isdir(path):
        return

    os.makedirs(path)
    log.info(f"Created directory in path: {path}")


def read_df(path):
    return pd.read_parquet(path) if os.path.exists(path) else None


def save_df(df, path):
    dir_path = os.path.split(path)[0]
    create_dir(dir_path)
    df.to_parquet(path)


def preprocess_input(users_df: pd.DataFrame, command_args: CommandArgs):
    return ArgParser.preprocess_input(users_df, command_args)


def parse_args(users_df, command_args: CommandArgs) -> CommandArgs:
    return ArgParser.parse_args(users_df, command_args)


def handle_args(users_df, command_args_ref: CommandArgs):
    return ArgParser.handle_args(users_df, command_args_ref)


def merge_spaced_args(command_args: CommandArgs):
    return ArgParser.merge_spaced_args(command_args)


def filter_phrases(command_args: CommandArgs):
    return ArgParser.filter_phrases(command_args)


def is_word_in_list_of_multiple_words(word, list_of_multiple_words):
    word_lower = word.lower()
    return any(word_lower for words in list_of_multiple_words if word_lower in words.lower())


def text_filter(command_args):
    return ArgParser.text_filter(command_args)


def regex_filter(command_args):
    return ArgParser.regex_filter(command_args)


def is_inside_square_brackets(text):
    return ArgParser.is_inside_square_brackets(text)


def select_random_phrase(phrases, error_message: ErrorMessage):
    return random.choice(phrases) if phrases else error_message.value


def generate_unique_number(user_id):
    today = int(datetime.now().strftime("%Y%m%d"))
    user_id_cut = user_id % 100

    log.info(f"Generating lucky number for user [{user_id}] - {today + user_id_cut}")
    return today + user_id_cut


def are_you_lucky(user_id, with_args=False):
    today = int(datetime.now().strftime("%Y%m%d"))
    user_hash = user_id + today
    random.seed(user_hash)
    rand_value = random.random()

    if rand_value < 0.1:
        message = "Nie. 🗿" if with_args else "Dzisiaj masz wielkiego pecha. Lepiej zostań w domu i nic nie rób. (łeee jestem grzybem ;-;)"
        lucky_score_type = LuckyScoreType.VERY_UNLUCKY
    elif rand_value < 0.3:
        message = "Raczej nie." if with_args else "Dzisiaj masz lekkiego pecha. Zachowaj ostrożność."
        lucky_score_type = LuckyScoreType.UNLUCKY
    elif rand_value < 0.7:
        message = "Rabini są niezdecydowani w tej kwestii." if with_args else "Normalny dzień dla normalnego chłopa."
        lucky_score_type = LuckyScoreType.NEUTRAL
    elif rand_value < 0.9:
        message = "Raczej tak." if with_args else "Dzisiaj masz lekkie szczęście. Możesz spróbować coś zrobić, ale może się to nie powieść."
        lucky_score_type = LuckyScoreType.LUCKY
    else:
        message = (
            "Tak. 🗿" if with_args else "Dzisiaj masz ogromne szczęście! Wyjdź z domu i spróbuj zrobić coś nowego, na pewno Ci się uda!"
        )
        lucky_score_type = LuckyScoreType.VERY_LUCKY

    log.info(f"User [{user_hash}] ({rand_value}) - {message}")
    return lucky_score_type, message


def is_prime(n):
    return all(False for i in range(2, n) if n % i == 0) and n >= 2


def is_gif(message):
    return message.document and message.document.mime_type == "video/mp4" and message.gif


def is_video(message):
    return message.document and message.document.mime_type == "video/mp4" and not message.gif


def message_id_to_path(message_id, message_type: MessageType):
    match message_type:
        case MessageType.IMAGE:
            filename = f"{message_id}.jpg"
            return os.path.join(CHAT_IMAGES_DIR_PATH, filename)
        case MessageType.GIF:
            filename = f"{message_id}.mp4"
            return os.path.join(CHAT_GIFS_DIR_PATH, filename)
        case MessageType.VIDEO:
            filename = f"{message_id}.mp4"
            return os.path.join(CHAT_VIDEOS_DIR_PATH, filename)
        case MessageType.VIDEO_NOTE:
            filename = f"{message_id}.mp4"
            return os.path.join(CHAT_VIDEO_NOTES_DIR_PATH, filename)
        case MessageType.AUDIO:
            filename = f"{message_id}.ogg"
            return os.path.join(CHAT_AUDIO_DIR_PATH, filename)
    return None


def get_message_type(message):
    if message is None:
        return MessageType.NONE
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
        case MessageType.VIDEO:
            path = message_id_to_path(message.id, MessageType.VIDEO)
        case MessageType.VIDEO_NOTE:
            path = message_id_to_path(message.id, MessageType.VIDEO_NOTE)
        case MessageType.AUDIO:
            path = message_id_to_path(message.id, MessageType.AUDIO)
        case _:
            return None

    if not os.path.exists(path):
        await message.download_media(file=path)


def parse_arg(users_df, command_args_ref, arg_str, arg_type: ArgType, is_optional=False) -> tuple[str | int, CommandArgs]:
    return ArgParser.parse_arg(users_df, command_args_ref, arg_str, arg_type, is_optional)


def parse_named_args(users_df, command_args_ref: CommandArgs):
    return ArgParser.parse_named_args(users_df, command_args_ref)


def parse_named_arg(arg, command_args):
    return ArgParser.parse_named_arg(arg, command_args)


def is_aliased_named_arg(arg, shortened_available_named_args):
    return ArgParser.is_aliased_named_arg(arg, shortened_available_named_args)


def is_normal_named_arg(arg, available_named_args):
    return ArgParser.is_normal_named_arg(arg, available_named_args)


def is_named_arg(arg, commands_args):
    return ArgParser.is_named_arg(arg, commands_args)


def parse_period(command_args, arg_str):
    return ArgParser.parse_period(command_args, arg_str)


def parse_date(date_str: str):
    return ArgParser.parse_date(date_str)


def parse_date_range(date_range_str: str):
    return ArgParser.parse_date_range(date_range_str)


def parse_user(users_df, command_args, arg_str):
    return ArgParser.parse_user(users_df, command_args, arg_str)


def parse_number(command_args, arg_str, positive_only=False):
    return ArgParser.parse_number(command_args, arg_str, positive_only)


def get_error(command_args: CommandArgs) -> str:
    return ArgParser.get_error(command_args)


def parse_int(num_str, positive_only=False):
    return ArgParser.parse_int(num_str, positive_only)


def x_to_light_years_str(x):
    return ArgParser.x_to_light_years_str(x)


def parse_string(command_args: CommandArgs, text: str):
    return ArgParser.parse_string(command_args, text)


def display_shopping_sunday(dt):
    return dt.strftime("%d %B")


def display_holy_text_df(df, bot_state, holy_text_type, label="Filtered bible verses", show_siglum=True):
    response = f"{label}:\n\n"
    for _, row in df.iterrows():
        verse = (
            f"[{get_siglum(row, holy_text_type, siglum_type=SiglumType.SHORT)}] {row['text']}\n\n"
            if show_siglum
            else f"{row['verse']}. {row['text']}\n"
        )
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
    return bible_df.drop_duplicates("book")[["book", "abbreviation"]].set_index("abbreviation")


def datetime_to_ms(dt):
    return int(dt.timestamp() * 1000)


def match_substr_to_list_of_texts(substr: str, texts: list, lower_case: bool = True) -> str:
    matched_texts = [text for text in texts if substr.lower() in text.lower()] if lower_case else [text for text in texts if substr in text]
    return matched_texts[0] if matched_texts else None


def get_username(first_name, last_name):
    username = first_name if first_name is not None else ""
    if last_name is not None:
        username += f" {last_name}"
    return username.strip()


def has_numbers(num_str):
    return ArgParser.has_numbers(num_str)


def file_exists(path):
    return os.path.exists(path)


def text_to_number(text):
    numbers = [ord(character) for character in text]
    return sum(numbers)


def generate_period_headline(command_args):
    match command_args.period_mode:
        case PeriodFilterMode.HOUR:
            return f"past {command_args.period_time} hours"
        case PeriodFilterMode.SECOND:
            return f"past {command_args.period_time} seconds"
        case PeriodFilterMode.MINUTE:
            return f"past {command_args.period_time} minutes"
        case PeriodFilterMode.TODAY:
            return "today"
        case PeriodFilterMode.YESTERDAY:
            return "yesterday"
        case PeriodFilterMode.DAY:
            return f"past {command_args.period_time} days"
        case PeriodFilterMode.WEEK:
            if command_args.period_time == -1:
                return "past week"
            return f"past {command_args.period_time} weeks"
        case PeriodFilterMode.MONTH:
            if command_args.period_time == -1:
                return "past month"
            return f"past {command_args.period_time} months"
        case PeriodFilterMode.YEAR:
            if command_args.period_time == -1:
                return "past year"
            return f"past {command_args.period_time} years"
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
            return dt_now + timedelta(seconds=command_args.period_time), ""
        case PeriodFilterMode.MINUTE:
            return dt_now + timedelta(minutes=command_args.period_time), ""
        case PeriodFilterMode.HOUR:
            return dt_now + timedelta(hours=command_args.period_time), ""
        case PeriodFilterMode.DAY:
            return dt_now + timedelta(days=command_args.period_time), ""
        case PeriodFilterMode.WEEK:
            return dt_now + timedelta(weeks=command_args.period_time), ""
        case PeriodFilterMode.DATE:
            return command_args.dt, ""
        case _:
            return None, "Wrong period offset. Use one of the following: second, minute, hour, day, week, date"


async def send_response_message(context, chat_id, message_id, message):
    await context.bot.send_message(chat_id=chat_id, reply_to_message_id=message_id, text=message)


def dt_to_pretty_str(dt):
    return dt.strftime("%d-%m-%Y %H:%M:%S")


def regexify_multiword_filter(words):
    base = r"^{}"
    expr = "(?=.*{})"
    return base.format("".join(expr.format(w) for w in words))


def parse_quran_verse_arg(quran_df, arg, bot_state, holy_text_type) -> [str, str]:
    arg_split = arg.split(":")
    if len(arg_split) != 2:
        return "", "Failed to parse the --verse argument."

    chapter_arg, verse_arg = arg_split[0].lower(), arg_split[1].lower()
    chapter_nums = quran_df.drop_duplicates("chapter_nr")["chapter_nr"].tolist()
    chapter_names = quran_df.drop_duplicates("chapter_name")["chapter_name"].tolist()

    matching_chapter_nums = [num for num in chapter_nums if str(num) == chapter_arg]
    matching_chapter_names = [chapter_name for chapter_name in chapter_names if chapter_arg in chapter_name.lower()]

    if matching_chapter_nums:
        chapter_nr = matching_chapter_nums[0]
        matching_verse_df = quran_df[(quran_df["chapter_nr"] == chapter_nr) & (quran_df["verse"] == verse_arg)]
        row = None if matching_verse_df.empty else matching_verse_df.iloc[0]
    elif matching_chapter_names:
        chapter_name = matching_chapter_names[0]
        matching_verse_df = quran_df[(quran_df["chapter_name"] == chapter_name) & (quran_df["verse"] == verse_arg)]
        row = None if matching_verse_df.empty else matching_verse_df.iloc[0]
    else:
        return "", f"Verse {chapter_arg}:{verse_arg} doesn't exist in Quran."

    if row is None:
        return "", f"Verse {chapter_arg}:{verse_arg} doesn't exist in Quran."

    bot_state.set_holy_text_last_verse_id(row.name, holy_text_type)
    response = f"[{get_siglum(row, holy_text_type, SiglumType.SHORT)}] {row['text']}"
    return response, ""


def remove_punctuation(s):
    return s.translate(str.maketrans("", "", string.punctuation))


def max_str_length_in_col(series):
    """To ease display of dataframes with long strings in telegram markdown table message"""
    strings = series.tolist()

    return -1 if len(strings) == 0 else max(len(ngram) for ngram in strings)


def max_str_length_in_list(strings):
    return -1 if len(strings) == 0 else max(len(ngram) for ngram in strings)


def get_random_id():
    return str(uuid.uuid4())


def calculate_skewed_probability(value, max_value):
    """Get a probability [0, 1], "0%" of the value should happen 50% of the time, "100%" of the value should happen 0% of the time (never)."""
    return (1 - ((value / max_value) ** 0.5)) / 2 if max_value > 0 else 0


def generate_response_headline(command_args, label, text_before_str="of", text_before_user="for"):
    text = label
    text += f' {text_before_str} "{command_args.string}"' if command_args.string != "" else ""
    text += f" {text_before_user} {command_args.user}" if command_args.user is not None else " "
    text += f" ({generate_period_headline(command_args)}):"
    return text


async def send_message(update: Update, context: ContextTypes.DEFAULT_TYPE, message_type: MessageType, text: str, path: str = ""):
    log.info(f"Sending message: {text} with media type: {message_type} and media path: {path}")
    match message_type:
        case MessageType.GIF:
            await context.bot.send_animation(
                chat_id=update.effective_chat.id, animation=path, caption=text, message_thread_id=update.message.message_thread_id
            )
        case MessageType.VIDEO:
            await context.bot.send_video(
                chat_id=update.effective_chat.id, video=path, caption=text, message_thread_id=update.message.message_thread_id
            )
        case MessageType.VIDEO_NOTE:
            await context.bot.send_video_note(
                chat_id=update.effective_chat.id, video_note=path, message_thread_id=update.message.message_thread_id
            )
            if text != "":
                await context.bot.send_message(
                    chat_id=update.effective_chat.id, text=text, message_thread_id=update.message.message_thread_id
                )
        case MessageType.IMAGE:
            await context.bot.send_photo(
                chat_id=update.effective_chat.id, photo=path, caption=text, message_thread_id=update.message.message_thread_id
            )
        case MessageType.AUDIO:
            await context.bot.send_audio(
                chat_id=update.effective_chat.id, audio=path, caption=text, message_thread_id=update.message.message_thread_id
            )
        case MessageType.VOICE:
            await context.bot.send_voice(
                chat_id=update.effective_chat.id, voice=path, caption=text, message_thread_id=update.message.message_thread_id
            )
        case MessageType.TEXT:
            await context.bot.send_message(chat_id=update.effective_chat.id, text=text, message_thread_id=update.message.message_thread_id)
        case MessageType.MARKDOWN_TEXT:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=text,
                parse_mode=telegram.constants.ParseMode.MARKDOWN_V2,
                message_thread_id=update.effective_message.message_thread_id,
            )


def roll(probability):
    """Roll a probability dice and return True if the hit is successful. The range of random() is [0, 1], so.. yeah it works."""
    return random.random() < probability


def safe_json_dump(x):
    """
    Convert a value to JSON string, handling None, lists, numpy arrays, and pandas Series.
    """
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None

    # Convert numpy arrays or pandas Series to lists
    if isinstance(x, np.ndarray | pd.Series):
        x = x.tolist()

    # Ensure everything is JSON-serializable
    try:
        return json.dumps(x)
    except TypeError:
        # fallback: wrap scalar in a list
        return json.dumps([x])


def df_to_dict(df: pd.DataFrame, key_col: str, value_col: str, value_type):
    d = defaultdict(value_type)
    for key, value in zip(df[key_col], df[value_col], strict=False):
        d[key] = value
    return d
