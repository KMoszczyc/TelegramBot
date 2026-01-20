import logging
import os
from enum import Enum

import pandas as pd

from src.models.random_event import RandomFailureEvent, RandomSuccessEvent


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
    with open(path) as f:
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
MAX_STEAL_CREDITS_DAILY = 3
LONG_MESSAGE_LIMIT = 1  # long texts spanning into multiple messages.
STOPWORD_RATIO_THRESHOLD = 0.59
MIN_QUIZ_TIME_TO_ANSWER_SECONDS = 10
CRITICAL_FAILURE_CHANCE = 0.05
CRITICAL_SUCCESS_CHANCE = 0.05
CREDIT_HISTORY_COLUMNS = ['timestamp', 'user_id', 'target_user_id', 'credit_change', 'action_type', 'bet_type', 'success']

TIMEZONE = 'Europe/Warsaw'

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = "/data" if RUNTIME_ENV == 'docker' else os.path.join(ROOT_DIR, 'data')
TEMP_DIR = os.path.join(DATA_DIR, 'temp')

# Chat data
CHAT_ETL_LOCK_PATH = os.path.join(DATA_DIR, 'chat/chat_etl.lock')
METADATA_PATH = os.path.join(DATA_DIR, 'chat/metadata.pickle')
UPDATE_REQUIRED_PATH = os.path.join(DATA_DIR, 'chat/update_required.lock')
CHAT_HISTORY_PATH = os.path.join(DATA_DIR, 'chat/chat_history.parquet')
CHAT_IMAGES_DIR_PATH = os.path.join(DATA_DIR, 'chat/images')
CHAT_GIFS_DIR_PATH = os.path.join(DATA_DIR, 'chat/gifs')
CHAT_VIDEOS_DIR_PATH = os.path.join(DATA_DIR, 'chat/videos')
CHAT_VIDEO_NOTES_DIR_PATH = os.path.join(DATA_DIR, 'chat/video_notes')
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
CREDIT_HISTORY_PATH = os.path.join(DATA_DIR, 'chat/credit_history.parquet')

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
QUIZ_DATABASE_PATH = os.path.join(DATA_DIR, 'misc/quiz_database.parquet')
POLISH_HOLIDAYS_PATH = os.path.join(DATA_DIR, 'misc/polish_holidays.csv')

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
quiz_df = pd.read_parquet(QUIZ_DATABASE_PATH)
polish_holidays_df = pd.read_csv(POLISH_HOLIDAYS_PATH, sep=';')


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
    VOICE = 'voice'
    NONE = 'none'


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
    SINGLE_NUMBER = 'single_number'


class CreditActionType(Enum):
    GET = 'get'  # get_credits()
    BET = 'bet'  # bet()
    STEAL = 'steal'  # steal()
    QUIZ = 'quiz'
    GIFT = 'gift'


STEAL_EVENTS = [
    RandomFailureEvent("A swarm of angry bees stole your credits mid-escape — half gone!", lambda amount: -amount // 2),
    RandomFailureEvent("The target turned into a dragon and breathed fire — you fled empty-handed!", lambda amount: -amount),
    RandomFailureEvent("Your shadow betrayed you and ate the credits — 75% vanished!", lambda amount: int(-amount * 0.75)),
    RandomFailureEvent("Gravity reversed, credits floated away — quarter lost!", lambda amount: -amount // 4),
    RandomFailureEvent("Time looped, you stole from yourself — total paradox loss!", lambda amount: -amount),
    RandomSuccessEvent("You accidentally robbed a bank vault — bonus jackpot!", lambda amount: amount // 2),
    RandomSuccessEvent("The target's gold statue came to life and thanked you — extra gold!", lambda amount: amount),
    RandomSuccessEvent("You found a portal to a credit dimension — double reality!", lambda amount: amount * 2),
    RandomSuccessEvent("Aliens beamed down and upgraded your haul — triple cosmic bonus!", lambda amount: amount * 3),
    RandomSuccessEvent("Your evil twin helped — lucky twin credits!", lambda amount: amount // 3),
]

BET_EVENTS = [
    RandomFailureEvent("The roulette wheel turned into a black hole — sucked double losses!", lambda amount: -amount * 2),
    RandomFailureEvent("Your bet offended the casino ghost — table haunted you away!", lambda amount: -amount),
    RandomFailureEvent("The dealer was a vampire — drained your credits twice!", lambda amount: -amount * 2),
    RandomFailureEvent("Bad luck gremlin possessed you — lost all plus gremlin fee!", lambda amount: -amount - 50),
    RandomFailureEvent("The ball became sentient and rebelled — total anarchy wipeout!", lambda amount: -amount),
    RandomSuccessEvent("Lady Luck was actually a wizard — enchanted double winnings!", lambda amount: amount),
    RandomSuccessEvent("You summoned the jackpot demon — triple infernal payout!", lambda amount: amount * 3),
    RandomSuccessEvent("Stars formed a winning constellation — triple celestial bonus!", lambda amount: amount * 3),
    RandomSuccessEvent("Epic win? Nah, legendary streak — 5x mega bonus!", lambda amount: amount * 5),
    RandomSuccessEvent("Casino turned into a fairy tale — unbelievable 10x magic!", lambda amount: amount * 10),
]

QUIZ_EVENTS = [
    RandomFailureEvent("Distracted by a dancing squirrel — penalty for cuteness overload!", lambda: -10),
    RandomFailureEvent("Your brain turned into jelly — extra wobbly penalty!", lambda: -20),
    RandomFailureEvent("Wrong answer summoned a quiz troll — big oops penalty!", lambda: -30),
    RandomFailureEvent("Memory erased by time travelers — lost in history!", lambda: -15),
    RandomFailureEvent("Quiz apocalypse — severe cosmic fail penalty!", lambda: -50),
    RandomSuccessEvent("Your answer echoed through dimensions — bonus echo credits!", lambda: 15),
    RandomSuccessEvent("Quiz gods blessed you — divine knowledge payout!", lambda: 25),
    RandomSuccessEvent("Time froze for your brilliance — frozen time bonus!", lambda: 50),
    RandomSuccessEvent("You broke the quiz matrix — system crash payout!", lambda: 30),
    RandomSuccessEvent("Genius level: Expert — ultimate expert bonus!", lambda: 100),
]
