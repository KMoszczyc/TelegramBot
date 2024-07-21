import os
import logging
from enum import Enum


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
log.info(f'Are we running in docker? {is_docker()}')

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = "/data" if is_docker() else os.path.join(ROOT_DIR, 'data')

# Chat data
METADATA_PATH = os.path.join(DATA_DIR, 'chat/metadata.pickle')
UPDATE_REQUIRED_PATH = os.path.join(DATA_DIR, 'chat/update_required.lock')
CHAT_HISTORY_PATH = os.path.join(DATA_DIR, 'chat/chat_history.parquet')
CHAT_IMAGES_DIR_PATH = os.path.join(DATA_DIR, 'chat/images')
CHAT_GIFS_DIR_PATH = os.path.join(DATA_DIR, 'chat/gifs')
CHAT_VIDEOS_DIR_PATH = os.path.join(DATA_DIR, 'chat/videos')
CHAT_AUDIO_DIR_PATH = os.path.join(DATA_DIR, 'chat/audio')

# CHAT_HISTORY_PATH = os.path.join(ROOT_DIR, 'data/chat/test_chat_history.parquet')
CLEANED_CHAT_HISTORY_PATH = os.path.join(DATA_DIR, 'chat/cleaned_chat_history.parquet')
REACTIONS_PATH = os.path.join(DATA_DIR, 'chat/reactions.parquet')
USERS_PATH = os.path.join(DATA_DIR, 'chat/users.parquet')

# Miscalenous
TVP_HEADLINES_PATH = os.path.join(DATA_DIR, 'misc/paski-tvp.txt')
TVP_LATEST_HEADLINES_PATH = os.path.join(DATA_DIR, 'misc/tvp_latest_headlines.txt')
OZJASZ_PHRASES_PATH = os.path.join(DATA_DIR, 'misc/ozjasz-wypowiedzi.txt')
POLISH_STOPWORDS_PATH = os.path.join(DATA_DIR, 'misc/polish.stopwords.txt')
BARTOSIAK_PATH = os.path.join(DATA_DIR, 'misc/bartosiak.txt')

# Load text files with funny phrases
tvp_headlines = read_str_file(TVP_HEADLINES_PATH)
tvp_latest_headlines = read_str_file(TVP_LATEST_HEADLINES_PATH)
ozjasz_phrases = read_str_file(OZJASZ_PHRASES_PATH)
bartosiak_phrases = read_str_file(BARTOSIAK_PATH)


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
    HOUR = 'hour'
    YESTERDAY = 'yesterday'
    WEEK = 'week'
    MONTH = 'month'
    YEAR = 'year'
    TOTAL = 'total'
    ERROR = 'error'


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
    REGEX = 'regex'
    NUMBER = 'number'


class MessageType(Enum):
    """Enum for message types"""
    TEXT = 'text'
    GIF = 'gif'
    VIDEO = 'video'
    VIDEO_NOTE = 'video_note'
    IMAGE = 'image'
    AUDIO = 'audio'
