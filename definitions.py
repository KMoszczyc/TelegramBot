import os
import logging
from enum import Enum

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

# Chat data
METADATA_PATH = os.path.join(ROOT_DIR, 'data/chat/metadata.pickle')
CHAT_HISTORY_PATH = os.path.join(ROOT_DIR, 'data/chat/chat_history.parquet')
CHAT_IMAGES_DIR_PATH = os.path.join(ROOT_DIR, 'data/chat/images')

# CHAT_HISTORY_PATH = os.path.join(ROOT_DIR, 'data/chat/test_chat_history.parquet')
CLEANED_CHAT_HISTORY_PATH = os.path.join(ROOT_DIR, 'data/chat/cleaned_chat_history.parquet')
REACTIONS_PATH = os.path.join(ROOT_DIR, 'data/chat/reactions.parquet')
USERS_PATH = os.path.join(ROOT_DIR, 'data/chat/users.parquet')

# Miscalenous
TVP_HEADLINES_PATH = os.path.join(ROOT_DIR, 'data/misc/paski-tvp.txt')
TVP_LATEST_HEADLINES_PATH = os.path.join(ROOT_DIR, 'data/misc/tvp_latest_headlines.txt')
OZJASZ_PHRASES_PATH = os.path.join(ROOT_DIR, 'data/misc/ozjasz-wypowiedzi.txt')
POLISH_STOPWORDS_PATH = os.path.join(ROOT_DIR, 'data/misc/polish.stopwords.txt')
BARTOSIAK_PATH = os.path.join(ROOT_DIR, 'data/misc/bartosiak.txt')

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)


class PeriodFilterMode(Enum):
    """Mode used for filtering the chat data for:
      - today (since midnight)
      - week - past 7 days
      - month - past 30 days
      - year
      - total
    """
    TODAY = 'today'
    YESTERDAY = 'yesterday'
    WEEK = 'week'
    MONTH = 'month'
    YEAR = 'year'
    TOTAL = 'total'
