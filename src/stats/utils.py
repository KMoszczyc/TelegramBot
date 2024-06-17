import re
import os
import logging
import pickle
from datetime import timezone

import pandas as pd

from definitions import CHAT_HISTORY_PATH, USERS_PATH, METADATA_PATH, CLEANED_CHAT_HISTORY_PATH
from src.core.utils import create_dir

log = logging.getLogger(__name__)


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
    """Dump self.metadata dict to pickle file."""
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
    special_characters = r'\-.()[{}_]:'
    return re.sub(f'([{re.escape(special_characters)}])', r'\\\1', text)


def contains_stopwords(s, stopwords):
    return any(word in stopwords for word in s.split())
