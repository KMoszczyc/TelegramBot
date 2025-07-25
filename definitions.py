import os
import logging
from enum import Enum

import pandas as pd


def is_docker():
    def text_in_file(text, filename):
        try:
            with open(filename, encoding='utf-8') as lines:
                return any(text in line for line in lines)
        except OSError:
            return False

    cgroup = '/proc/self/cgroup'
    return os.path.exists('/.dockerenv') or text_in_file('docker', cgroup)


def read_str_file(path):
    with open(path, 'r') as f:
        lines = f.read().splitlines()
    return lines


logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
log = logging.getLogger(__name__)
RUNTIME_ENV = 'docker' if is_docker() else 'windows'
log.info(f'Runtime: {RUNTIME_ENV}')

# Constants
MAX_USERNAME_LENGTH = 20
MAX_NICKNAMES_NUM = 5
MAX_REMINDERS_DAILY_USAGE = 10
MAX_CWEL_USAGE_DAILY = 25
MAX_GET_CREDITS_DAILY = 1
LONG_MESSAGE_LIMIT = 1  # long texts spanning into multiple messages.
STOPWORD_RATIO_THRESHOLD = 0.59

TIMEZONE = 'Europe/Warsaw'

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = "/data" if RUNTIME_ENV == 'docker' else os.path.join(ROOT_DIR, 'data')
TEMP_DIR = os.path.join(DATA_DIR, 'temp')

# Chat data
METADATA_PATH = os.path.join(DATA_DIR, 'chat/metadata.pickle')
UPDATE_REQUIRED_PATH = os.path.join(DATA_DIR, 'chat/update_required.lock')
CHAT_HISTORY_PATH = os.path.join(DATA_DIR, 'chat/chat_history.parquet')
CHAT_IMAGES_DIR_PATH = os.path.join(DATA_DIR, 'chat/images')
CHAT_GIFS_DIR_PATH = os.path.join(DATA_DIR, 'chat/gifs')
CHAT_VIDEOS_DIR_PATH = os.path.join(DATA_DIR, 'chat/videos')
CHAT_AUDIO_DIR_PATH = os.path.join(DATA_DIR, 'chat/audio')
CHAT_WORD_STATS_DIR_PATH = os.path.join(DATA_DIR, 'chat/word_stats')
WORD_STATS_UPDATE_LOCK_PATH = os.path.join(CHAT_WORD_STATS_DIR_PATH, 'update.lock')

CLEANED_CHAT_HISTORY_PATH = os.path.join(DATA_DIR, 'chat/cleaned_chat_history.parquet')
REACTIONS_PATH = os.path.join(DATA_DIR, 'chat/reactions.parquet')
USERS_PATH = os.path.join(DATA_DIR, 'chat/users.parquet')
COMMANDS_USAGE_PATH = os.path.join(DATA_DIR, 'chat/commands_usage.parquet')
SCHEDULED_JOBS_PATH = os.path.join(DATA_DIR, 'chat/scheduled_jobs.pkl')
CWEL_STATS_PATH = os.path.join(DATA_DIR, 'chat/cwel_stats.parquet')
CREDITS_PATH = os.path.join(DATA_DIR, 'chat/credits.pkl')

# Miscalenous
TVP_HEADLINES_PATH = os.path.join(DATA_DIR, 'misc/paski-tvp.txt')
TVP_LATEST_HEADLINES_PATH = os.path.join(DATA_DIR, 'misc/tvp_latest_headlines.txt')
OZJASZ_PHRASES_PATH = os.path.join(DATA_DIR, 'misc/ozjasz-wypowiedzi.txt')
POLISH_STOPWORDS_PATH = os.path.join(DATA_DIR, 'misc/polish.stopwords.txt')
BARTOSIAK_PATH = os.path.join(DATA_DIR, 'misc/bartosiak.txt')
COMMANDS_PATH = os.path.join(DATA_DIR, 'misc/commands.txt')
ARGUMENTS_HELP_PATH = os.path.join(DATA_DIR, 'misc/arguments_help.txt')
BIBLE_PATH = os.path.join(DATA_DIR, 'misc/bible.parquet')
QURAN_PATH = os.path.join(DATA_DIR, 'misc/quran.parquet')
SHOPPING_SUNDAYS_PATH = os.path.join(DATA_DIR, 'misc/niedziele.txt')
EUROPEJSKAFIRMA_PATH = os.path.join(DATA_DIR, 'misc/europejskafirma.txt')
BOCZEK_PATH = os.path.join(DATA_DIR, 'misc/boczek.txt')
KIEPSCY_PATH = os.path.join(DATA_DIR, 'misc/kiepscy.parquet')
WALESA_PATH = os.path.join(DATA_DIR, 'misc/walesa.txt')

# Load text files with funny phrases
tvp_headlines = read_str_file(TVP_HEADLINES_PATH)
tvp_latest_headlines = read_str_file(TVP_LATEST_HEADLINES_PATH)
ozjasz_phrases = read_str_file(OZJASZ_PHRASES_PATH)
bartosiak_phrases = read_str_file(BARTOSIAK_PATH)
commands = read_str_file(COMMANDS_PATH)
arguments_help = read_str_file(ARGUMENTS_HELP_PATH)
bible_df = pd.read_parquet(BIBLE_PATH)
quran_df = pd.read_parquet(QURAN_PATH)
shopping_sundays = read_str_file(SHOPPING_SUNDAYS_PATH)
europejskafirma_phrases = read_str_file(EUROPEJSKAFIRMA_PATH)
boczek_phrases = read_str_file(BOCZEK_PATH)
kiepscy_df = pd.read_parquet(KIEPSCY_PATH)
walesa_phrases = read_str_file(WALESA_PATH)
polish_stopwords = read_str_file(POLISH_STOPWORDS_PATH)


class PeriodFilterMode(Enum):
    """Mode used for filtering the chat data for:
      - today (since midnight)
      - hour (1h, 3h, 6h...)
      - week - past 7 days
      - month - past 30 days
      - year
      - total
    """
    TODAY = 'today'
    SECOND = 'second'
    MINUTE = 'minute'
    HOUR = 'hour'
    DAY = 'day'
    YESTERDAY = 'yesterday'
    WEEK = 'week'
    WEEKS = 'weeks'  # for multiple weeks like 2w = 2 weeks
    MONTH = 'month'
    YEAR = 'year'
    TOTAL = 'total'
    DATE = 'date'
    DATE_RANGE = 'date_range'
    ERROR = 'error'

    @classmethod
    def _missing_(cls, value):
        return PeriodFilterMode.ERROR


class DatetimeFormat(Enum):
    """Enum for different datetime granularities."""
    DATE = "%d-%m-%Y"
    HOUR = "%d-%m-%Y:%H"
    MINUTE = "%d-%m-%Y:%H:%M"
    SECOND = "%d-%m-%Y:%H:%M:%S"


class EmojiType(Enum):
    """Enum for different reaction emoji types"""
    ALL = 'all'
    POSITIVE = 'positive'
    NEGATIVE = 'negative'


class ArgType(Enum):
    """Enum for user command argument input."""
    USER = 'user'
    PERIOD = 'period'
    TEXT = 'text'
    TEXT_MULTISPACED = 'text_multispaced'  # use only as the last argument!
    REGEX = 'regex'
    NUMBER = 'number'
    STRING = 'string'
    POSITIVE_INT = 'positive_int'
    NONE = 'none'


class MessageType(Enum):
    """Enum for message types"""
    TEXT = 'text'
    GIF = 'gif'
    VIDEO = 'video'
    VIDEO_NOTE = 'video_note'
    IMAGE = 'image'
    AUDIO = 'audio'


class NamedArgType(Enum):
    SHORT = 'short'
    NORMAL = 'normal'
    NONE = 'none'


class ChartType(Enum):
    LINE = 'line'
    BAR = 'bar'
    MIXED = 'mixed'


class HolyTextType(Enum):
    BIBLE = 'bible'
    QURAN = 'quran'


class SiglumType(Enum):
    FULL = 'full'
    SHORT = 'short'


class LuckyScoreType(Enum):
    VERY_UNLUCKY = 'very unlucky'
    UNLUCKY = 'unlucky'
    NEUTRAL = 'neutral'
    LUCKY = 'lucky'
    VERY_LUCKY = 'very lucky'

class RouletteBetType(Enum):
    RED = 'red'
    BLACK = 'black'
    GREEN = 'green'
    ODD = 'odd'
    EVEN = 'even'
    NONE = 'none'
    HIGH = 'high'
    LOW = 'low'