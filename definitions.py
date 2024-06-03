import os
import logging
from enum import Enum

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = "/data"  # location of a docker mounted volume, shared between etc and bot containers. Specified in docker-compose.yml
# DATA_DIR = os.path.join(ROOT_DIR, 'data')

# Chat data
METADATA_PATH = os.path.join(DATA_DIR, 'chat/metadata.pickle')
UPDATE_REQUIRED_PATH = os.path.join(DATA_DIR, 'chat/update_required.lock')
CHAT_HISTORY_PATH = os.path.join(DATA_DIR, 'chat/chat_history.parquet')
CHAT_IMAGES_DIR_PATH = os.path.join(DATA_DIR, 'chat/images')

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

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)


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

class EmojiType(Enum):
    """Enum for different reaction emoji types"""
    ALL = 'all'
    POSITIVE = 'positive'
    NEGATIVE = 'negative'