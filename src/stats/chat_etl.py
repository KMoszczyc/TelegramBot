import logging
import shutil
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
import os

import pandas as pd
from dotenv import load_dotenv

from definitions import CHAT_HISTORY_PATH, USERS_PATH, CLEANED_CHAT_HISTORY_PATH, REACTIONS_PATH, UPDATE_REQUIRED_PATH, TEMP_DIR
import src.stats.utils as stats_utils
import src.core.utils as core_utils

load_dotenv()
BOT_ID = os.getenv('BOT_ID')

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', 1000)
pd.options.mode.chained_assignment = None

log = logging.getLogger(__name__)
excluded_user_ids = [6455867316, 6455867316, 1660481027, 1626698260, 1653222205, 1626673718, 2103796402]
BOT_MESSAGE_RETENION_IN_MINUTES = 5


class ChatETL:
    """Core chat downloader and data processor."""

    def __init__(self, client_api_handler):
        self.client_api_handler = client_api_handler
        self.metadata = stats_utils.load_metadata()

    def update(self, days: int):
        log.info(f"Running chat ETL for the past: {days} days")

        # ETL
        self.download_chat_history(days)
        self.extract_users()
        self.clean_chat_history()
        self.generate_reactions_df()

        # Cleanup
        self.delete_bot_messages()
        self.cleanup_temp_dir()

    def download_chat_history(self, days):
        self.metadata = stats_utils.load_metadata()
        latest_messages, message_types = self.client_api_handler.get_chat_history(days)

        columns = ['message_id', 'timestamp', 'user_id', 'first_name', 'last_name', 'username', 'text', 'reaction_emojis', 'reaction_user_ids', 'message_type']
        data = []

        malformed_count = 0
        message_ids_for_reaction_api_update = [message.id for message in latest_messages if self.count_reactions(message) > 3]
        message_reactions = self.client_api_handler.get_reactions(message_ids_for_reaction_api_update) if message_ids_for_reaction_api_update else []
        log.info(f'Additional {len(message_ids_for_reaction_api_update)} messages pulled with more detailed reactions.')

        for message, message_type in zip(latest_messages, message_types):
            reaction_emojis, reaction_user_ids = [], []
            if message is None or message.sender is None:
                continue
            success = True

            if message.reactions is not None and message.reactions.recent_reactions is not None:
                reaction_emojis, reaction_user_ids, malformed_count, success = self.parse_reactions(message, message.reactions.recent_reactions, malformed_count, success)
                reactions_count = self.count_reactions(message)
                if reactions_count > 3 and message_reactions:
                    reaction_emojis, reaction_user_ids, malformed_count, success = self.parse_reactions(message, message_reactions[message.id].reactions, malformed_count, success)

            if not success:
                continue
            single_message_data = [message.id, message.date, message.sender_id, message.sender.first_name, message.sender.last_name, message.sender.username,
                                   message.text,
                                   reaction_emojis,
                                   reaction_user_ids,
                                   message_type.value]
            data.append(single_message_data)

        old_chat_df = core_utils.read_df(CHAT_HISTORY_PATH)
        latest_chat_df = pd.DataFrame(data, columns=columns)

        log.info(f'{len(latest_chat_df)} messages pulled since {datetime.now(tz=ZoneInfo('Europe/Warsaw')) - timedelta(days=days)} with {malformed_count} malformed records.')
        if old_chat_df is not None and not latest_chat_df.empty:
            merged_chat_df = pd.concat([old_chat_df, latest_chat_df], ignore_index=True).drop_duplicates(subset='message_id', keep='last').reset_index(drop=True)
        elif old_chat_df is not None:
            merged_chat_df = old_chat_df
        elif not latest_chat_df.empty:
            merged_chat_df = latest_chat_df
        else:
            log.info('Failed to update the chat history.')
            return

        new_msg_count = len(merged_chat_df) - len(old_chat_df) if old_chat_df is not None else len(merged_chat_df)
        log.info(f'New {new_msg_count} messages since {self.metadata['last_message_dt'].tz_convert('Europe/Warsaw')} with {malformed_count} malformed records.')
        merged_chat_df = merged_chat_df.sort_values(by='timestamp').reset_index(drop=True)

        print(merged_chat_df.tail(1))
        self.metadata['last_message_id'] = merged_chat_df['message_id'].iloc[-1]
        self.metadata['last_message_utc_timestamp'] = int(merged_chat_df['timestamp'].iloc[-1].replace(tzinfo=timezone.utc).astimezone(tz=None).timestamp())
        self.metadata['1_day_offset_utc_timestamp'] = int(
            (merged_chat_df['timestamp'].iloc[-1].replace(tzinfo=timezone.utc).astimezone(tz=None) - timedelta(days=1)).timestamp())
        self.metadata['last_message_dt'] = merged_chat_df['timestamp'].iloc[-1]
        self.metadata['last_update'] = datetime.now(tz=timezone.utc)
        self.metadata['message_count'] = len(merged_chat_df)
        self.metadata['new_latest_data'] = True

        stats_utils.create_empty_file(UPDATE_REQUIRED_PATH)
        stats_utils.save_metadata(self.metadata)
        core_utils.save_df(merged_chat_df, CHAT_HISTORY_PATH)

    def parse_reactions(self, msg, message_reactions, malformed_count, success):
        reaction_emojis, reaction_user_ids = [], []

        for reaction in message_reactions:
            try:
                reaction_emojis.append(reaction.reaction.emoticon)
                reaction_user_ids.append(reaction.peer_id.user_id)
            except AttributeError:
                success = False
                malformed_count += 1
                log.error(f'Issue with reading message reaction emojis/user_id: {msg}.')

        return reaction_emojis, reaction_user_ids, malformed_count, success

    def count_reactions(self, message):
        return sum(reaction_count.count for reaction_count in message.reactions.results) if message.reactions is not None else 0

    def clean_chat_history(self):
        log.info('Cleaning chat history...')

        chat_df = core_utils.read_df(CHAT_HISTORY_PATH)
        users_df = stats_utils.read_users()
        filtered_df = chat_df[~chat_df['user_id'].isin(excluded_user_ids)]
        cleaned_df = filtered_df.drop(['first_name', 'last_name', 'username'], axis=1)
        cleaned_df = cleaned_df.merge(users_df, on='user_id')
        cleaned_df = cleaned_df[['message_id', 'timestamp', 'user_id', 'final_username', 'text', 'reaction_emojis', 'reaction_user_ids', 'message_type']]
        cleaned_df['timestamp'] = cleaned_df['timestamp'].dt.tz_convert('Europe/Warsaw')
        cleaned_df['reaction_user_ids'] = cleaned_df['reaction_user_ids'].tolist()

        log.info(f'Cleaned chat history df, from: {len(chat_df)} to: {len(cleaned_df)}')
        core_utils.save_df(cleaned_df, CLEANED_CHAT_HISTORY_PATH)

    def extract_users(self):
        """Extract users from the chat history"""

        if os.path.exists(USERS_PATH):
            log.info(f'Users already extracted, {USERS_PATH} exists.')
            return
        log.info('Extracting users...')

        chat_df = core_utils.read_df(CHAT_HISTORY_PATH)
        unique_chat_df = chat_df.drop_duplicates('user_id')
        users_df = unique_chat_df[['user_id', 'first_name', 'last_name', 'username']]
        filtered_users_df = users_df[~users_df['user_id'].isin(excluded_user_ids)]
        filtered_users_df['final_username'] = filtered_users_df.apply(self.create_final_username, axis=1)
        filtered_users_df['nicknames'] = [[] for _ in range(len(filtered_users_df))]
        filtered_users_df = filtered_users_df.set_index('user_id')

        core_utils.save_df(filtered_users_df, USERS_PATH)

    def create_final_username(self, row):
        final_username = row['username']
        if final_username is None:
            final_username = f"{row['first_name']} {row['last_name']}" if row['last_name'] is not None else row['first_name']
        return final_username

    def generate_reactions_df(self):
        """Include all reactions and fill the missing user_ids with None"""
        log.info('Generating reactions df...')

        chat_df = core_utils.read_df(CLEANED_CHAT_HISTORY_PATH)
        users_df = core_utils.read_df(USERS_PATH)

        chat_df['len_reactions'] = chat_df['reaction_emojis'].apply(lambda x: len(x))
        chat_df['len_reaction_users'] = chat_df['reaction_user_ids'].apply(lambda x: len(x))
        clean_df = chat_df[chat_df['len_reactions'] > 0]
        reactions_df = clean_df.explode(['reaction_emojis', 'reaction_user_ids'])

        reactions_df = reactions_df.merge(users_df, left_on='reaction_user_ids', right_on='user_id', how='left')
        reactions_df = reactions_df[['message_id', 'timestamp', 'final_username_x', 'final_username_y', 'text', 'reaction_emojis']]
        reactions_df.columns = ['message_id', 'timestamp', 'reacted_to_username', 'reacting_username', 'text', 'emoji']

        core_utils.save_df(reactions_df, REACTIONS_PATH)

    def delete_bot_messages(self):
        """Be carefull here, you could delete someone's messages forever if you are not sure about the bot_id!"""
        chat_df = core_utils.read_df(CHAT_HISTORY_PATH)
        if chat_df is None:
            log.info('No chat history, no bot messages to delete.')
            return

        filter_dt = datetime.now(timezone.utc) - timedelta(minutes=BOT_MESSAGE_RETENION_IN_MINUTES)

        bot_messages_df = chat_df[chat_df['user_id'] == int(BOT_ID)]
        old_bot_messages_df = bot_messages_df[bot_messages_df['timestamp'] < filter_dt]
        not_liked_old_bot_messages_df = old_bot_messages_df[old_bot_messages_df['reaction_emojis'].apply(lambda x: len(x) == 0)]

        log.info(f"Deleting {len(old_bot_messages_df)} bot messages older than {BOT_MESSAGE_RETENION_IN_MINUTES} minutes and without reactions.")
        message_ids = not_liked_old_bot_messages_df['message_id'].tolist()

        self.client_api_handler.delete_messages(message_ids)

    def cleanup_temp_dir(self):
        stats_utils.create_dir(TEMP_DIR)
        files_num = len(os.listdir(TEMP_DIR))
        if os.path.exists(TEMP_DIR):
            log.info(f'Removing {files_num} files from temp dir...')
            shutil.rmtree(TEMP_DIR)
