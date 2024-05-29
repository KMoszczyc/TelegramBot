import pandas as pd
from src.core.client_api_handler import ClientAPIHandler
from definitions import CHAT_HISTORY_PATH, USERS_PATH, METADATA_PATH, CLEANED_CHAT_HISTORY_PATH
from src.core.utils import create_dir
import os
import logging
import traceback
import pickle
from datetime import datetime, timezone
import time


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
    os.system('sync')
    time.sleep(2)

    fd = os.open('/path/to/metadata.pickle', os.O_RDONLY)
    os.fsync(fd)
    with open(fd, 'rb') as file:
        metadata = pickle.load(file)
    os.close(fd)
    return metadata

    # with open('metadata.pickle', 'rb') as f: metadata = pickle.load(f)


def save_metadata(metadata):
    """Dump self.metadata dict to pickle file."""
    dir_path = os.path.split(METADATA_PATH)[0]
    create_dir(dir_path)
    with open(METADATA_PATH, 'wb') as f:
        pickle.dump(metadata, f, protocol=pickle.HIGHEST_PROTOCOL)
        f.flush()
        os.fsync(f.fileno())
    os.system('sync')


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
