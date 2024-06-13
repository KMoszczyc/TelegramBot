import logging
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

from definitions import CHAT_HISTORY_PATH, USERS_PATH, CLEANED_CHAT_HISTORY_PATH, REACTIONS_PATH, UPDATE_REQUIRED_PATH
import src.stats.utils as stats_utils

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', 1000)
pd.options.mode.chained_assignment = None

log = logging.getLogger(__name__)
excluded_user_ids = [6455867316, 6455867316, 1660481027, 1626698260, 1653222205, 1626673718, 2103796402]


class ChatETL:
    """Core chat downloader and data processor."""

    def __init__(self, client_api_handler):
        self.client_api_handler = client_api_handler
        self.metadata = stats_utils.load_metadata()

    def update(self, days: int):
        log.info(f"Running chat update for the past: {days} days")

        self.download_chat_history(days)
        self.extract_users()
        self.clean_chat_history()
        self.generate_reactions_df()

    def download_chat_history(self, days):
        self.metadata = stats_utils.load_metadata()
        # latest_messages = self.client_api_handler.get_chat_history(self.metadata['last_message_utc_timestamp'])
        latest_messages = self.client_api_handler.get_chat_history(days)
        # self.client_api_handler.get_reactions(270441)

        columns = ['message_id', 'timestamp', 'user_id', 'first_name', 'last_name', 'username', 'text', 'reaction_emojis', 'reaction_user_ids', 'photo']
        data = []

        malformed_count = 0
        message_ids_for_reaction_api_update = [message.id for message in latest_messages if self.count_reactions(message) > 3]
        message_reactions = self.client_api_handler.get_reactions(message_ids_for_reaction_api_update) if message_ids_for_reaction_api_update else []
        log.info(f'Additional {len(message_ids_for_reaction_api_update)} messages pulled with more detailed reactions.')

        for message in latest_messages:
            reaction_emojis, reaction_user_ids = [], []
            if message is None or message.sender is None:
                continue
            success = True

            if message.reactions is not None and message.reactions.recent_reactions is not None:
                reaction_emojis, reaction_user_ids, malformed_count, success = self.parse_reactions(message, message.reactions.recent_reactions,
                                                                                                    malformed_count, success)
                reactions_count = self.count_reactions(message)
                if reactions_count > 3 and message_reactions:
                    reaction_emojis, reaction_user_ids, malformed_count, success = self.parse_reactions(message, message_reactions[message.id].reactions,
                                                                                                        malformed_count, success)
                    # print(message.date, reaction_emojis, reaction_user_ids)

            if not success:
                continue
            is_photo = bool(message.photo)
            single_message_data = [message.id, message.date, message.sender_id, message.sender.first_name, message.sender.last_name, message.sender.username,
                                   message.text,
                                   reaction_emojis,
                                   reaction_user_ids,
                                   is_photo]
            data.append(single_message_data)

        old_chat_df = stats_utils.read_df(CHAT_HISTORY_PATH)
        latest_chat_df = pd.DataFrame(data, columns=columns)

        log.info(
            f'{len(latest_chat_df)} messages pulled since {datetime.now(tz=ZoneInfo('Europe/Warsaw')) - timedelta(days=days)} with {malformed_count} malformed records.')
        # print(latest_chat_df.head(5))
        if old_chat_df is not None and not latest_chat_df.empty:
            # merged_chat_df = pd.concat([old_chat_df, latest_chat_df]).drop_duplicates(subset='message_id').reset_index(drop=True)
            merged_chat_df = pd.concat([old_chat_df, latest_chat_df], ignore_index=True).drop_duplicates(subset='message_id', keep='last').reset_index(
                drop=True)
        elif old_chat_df is not None:
            merged_chat_df = old_chat_df
        elif not latest_chat_df.empty:
            merged_chat_df = latest_chat_df
        else:
            return

        new_msg_count = len(merged_chat_df) - len(old_chat_df) if old_chat_df else len(merged_chat_df)
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
        stats_utils.save_df(merged_chat_df, CHAT_HISTORY_PATH)

    def parse_reactions(self, msg, message_reactions, malformed_count, success):
        reaction_emojis, reaction_user_ids = [], []

        # print(message_reactions)
        for reaction in message_reactions:
            try:
                reaction_emojis.append(reaction.reaction.emoticon)
                reaction_user_ids.append(reaction.peer_id.user_id)
            except AttributeError:
                success = False
                malformed_count += 1
                log.error(f'Issue with reading message reaction emojis/user_id: {msg}.')

        # for reaction_count in message.reactions.results:
        #     count = reaction_count.count
        #     emoji = reaction_count.reaction.emoticon
        #     all_emojis.extend([emoji] * count)

        return reaction_emojis, reaction_user_ids, malformed_count, success

    def count_reactions(self, message):
        return sum(reaction_count.count for reaction_count in message.reactions.results) if message.reactions is not None else 0

    def clean_chat_history(self):
        chat_df = stats_utils.read_df(CHAT_HISTORY_PATH)
        users_df = stats_utils.read_users()
        filtered_df = chat_df[~chat_df['user_id'].isin(excluded_user_ids)]
        cleaned_df = filtered_df.drop(['first_name', 'last_name', 'username'], axis=1)
        cleaned_df = cleaned_df.merge(users_df, on='user_id')
        cleaned_df = cleaned_df[['message_id', 'timestamp', 'user_id', 'final_username', 'text', 'reaction_emojis', 'reaction_user_ids', 'photo']]
        cleaned_df['timestamp'] = cleaned_df['timestamp'].dt.tz_convert('Europe/Warsaw')
        cleaned_df['reaction_user_ids'] = cleaned_df['reaction_user_ids'].tolist()

        # print(users_df)
        # print(cleaned_df.head(5))

        log.info(f'Cleaned chat history df, from: {len(chat_df)} to: {len(cleaned_df)}')
        stats_utils.save_df(cleaned_df, CLEANED_CHAT_HISTORY_PATH)

    def extract_users(self):
        chat_df = stats_utils.read_df(CHAT_HISTORY_PATH)
        unique_chat_df = chat_df.drop_duplicates('user_id')
        users_df = unique_chat_df[['user_id', 'first_name', 'last_name', 'username']]
        filtered_users_df = users_df[~users_df['user_id'].isin(excluded_user_ids)]
        filtered_users_df['final_username'] = filtered_users_df.apply(self.create_final_username, axis=1)

        stats_utils.save_df(filtered_users_df, USERS_PATH)

    def create_final_username(self, row):
        final_username = row['username']
        if final_username is None:
            final_username = f"{row['first_name']} {row['last_name']}" if row['last_name'] is not None else row['first_name']
        return final_username

    def generate_reactions_df(self):
        """Include all reactions and fill the missing user_ids with None"""
        chat_df = stats_utils.read_df(CLEANED_CHAT_HISTORY_PATH)
        users_df = stats_utils.read_df(USERS_PATH)

        chat_df['len_reactions'] = chat_df['reaction_emojis'].apply(lambda x: len(x))
        chat_df['len_reaction_users'] = chat_df['reaction_user_ids'].apply(lambda x: len(x))
        clean_df = chat_df[chat_df['len_reactions'] > 0]
        reactions_df = clean_df.explode(['reaction_emojis', 'reaction_user_ids'])

        reactions_df = reactions_df.merge(users_df, left_on='reaction_user_ids', right_on='user_id', how='left')
        reactions_df = reactions_df[['message_id', 'timestamp', 'final_username_x', 'final_username_y', 'text', 'reaction_emojis']]
        reactions_df.columns = ['message_id', 'timestamp', 'reacted_to_username', 'reacting_username', 'text', 'emoji']

        stats_utils.save_df(reactions_df, REACTIONS_PATH)
