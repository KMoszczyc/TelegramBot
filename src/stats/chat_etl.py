import logging
import os
import shutil
import time
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
from dotenv import load_dotenv

import src.core.utils as core_utils
import src.stats.utils as stats_utils
from definitions import (
    CHAT_HISTORY_PATH,
    CLEANED_CHAT_HISTORY_PATH,
    COMMANDS_USAGE_PATH,
    TEMP_DIR,
    TIMEZONE,
    USERS_PATH,
    DBSaveMode,
    MessageType,
    Table,
)
from src.core.client_api_handler import ClientAPIHandler
from src.models.db.db import DB
from src.models.schemas import chat_history_schema, cleaned_chat_history_schema, commands_usage_schema, reactions_schema, users_schema
from src.stats.ocr import OCR

load_dotenv()
BOT_ID = os.getenv("BOT_ID")

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)
pd.set_option("display.width", 1000)
pd.options.mode.chained_assignment = None

log = logging.getLogger(__name__)
excluded_user_ids = [6455867316, 6455867316, 1660481027, 1626698260, 1653222205, 1626673718, 2103796402]
BOT_MESSAGE_RETENION_IN_MINUTES = 5


class ChatETL:
    """Core chat downloader and data processor."""

    def __init__(self):
        self.client_api_handler = ClientAPIHandler()
        self.db = DB()

    def update(self, days: int, bulk_ocr=False):
        log.info(f"Running chat ETL for the past: {days} days")

        # ETL
        self.download_chat_history(days)
        self.extract_users()
        self.clean_chat_history()
        self.generate_reactions_df()

        if bulk_ocr:
            self.perform_bulk_ocr()

        # Validate
        self.validate_data()

        # Cleanup
        # self.delete_bot_messages() # with the introduction of topici, this is not needed
        self.cleanup_temp_dir()

    def download_chat_history(self, days):
        # old_chat_df = core_utils.read_df(CHAT_HISTORY_PATH)
        latest_messages, message_types = self.client_api_handler.get_chat_history(days)

        columns = [
            "message_id",
            "timestamp",
            "user_id",
            "first_name",
            "last_name",
            "username",
            "text",
            "image_text",
            "reaction_emojis",
            "reaction_user_ids",
            "message_type",
        ]
        data = []

        malformed_count = 0
        ocr_count = 0
        message_ids_for_reaction_api_update = [message.id for message in latest_messages if self.count_reactions(message) > 3]
        message_reactions = (
            self.client_api_handler.get_reactions(message_ids_for_reaction_api_update) if message_ids_for_reaction_api_update else []
        )
        log.info(f"Additional {len(message_ids_for_reaction_api_update)} messages pulled with more detailed reactions.")

        for message, message_type in zip(latest_messages, message_types, strict=False):
            reaction_emojis, reaction_user_ids = [], []
            if message is None or message.sender is None:
                continue
            success = True

            if message.reactions is not None and message.reactions.recent_reactions is not None:
                reaction_emojis, reaction_user_ids, malformed_count, success = self.parse_reactions(
                    message, message.reactions.recent_reactions, malformed_count, success
                )
                reactions_count = self.count_reactions(message)
                if reactions_count > 3 and message_reactions:
                    reaction_emojis, reaction_user_ids, malformed_count, success = self.parse_reactions(
                        message, message_reactions[message.id].reactions, malformed_count, success
                    )

            if not success:
                continue

            image_text = ""
            # if message_type == MessageType.IMAGE and (old_chat_df is None or old_chat_df[old_chat_df['message_id'] == message.id].empty):
            #     path = core_utils.message_id_to_path(message.id, MessageType.IMAGE)
            #     image_text = OCR.extract_text_from_image(path)
            #     ocr_count += 1

            single_message_data = [
                int(message.id),
                message.date,
                int(message.sender_id),
                message.sender.first_name,
                message.sender.last_name,
                message.sender.username,
                message.text,
                image_text,
                reaction_emojis,
                reaction_user_ids,
                message_type.value,
            ]
            data.append(single_message_data)

        latest_chat_df = pd.DataFrame(data, columns=columns)
        data_pull_start_dt = (datetime.now(tz=ZoneInfo(TIMEZONE)) - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
        latest_chat_df["timestamp"] = (
            pd.to_datetime(latest_chat_df["timestamp"], utc=True).dt.tz_convert(TIMEZONE).astype(f"datetime64[ns, {TIMEZONE}]")
        )
        log.info(
            f"Since {data_pull_start_dt}: {len(latest_chat_df)} messages were pulled with {malformed_count} malformed records and {ocr_count} ocr performed on images."
        )

        latest_chat_df = latest_chat_df.sort_values(by="timestamp").reset_index(drop=True)
        stats_utils.validate_schema(latest_chat_df, chat_history_schema)

        # stats_utils.create_empty_file(UPDATE_REQUIRED_PATH)
        # core_utils.save_df(merged_chat_df, CHAT_HISTORY_PATH)
        self.db.save_dataframe(latest_chat_df, Table.CHAT_HISTORY, mode=DBSaveMode.APPEND)
        return latest_chat_df

    def perform_bulk_ocr(self):
        # chat_df = core_utils.read_df(CHAT_HISTORY_PATH)
        chat_df = self.db.read_df_from_db(Table.CHAT_HISTORY)
        if chat_df.empty or chat_df is None:
            log.info("No chat history df, no ocr performed.")
            return

        image_chat_df = chat_df[chat_df["message_type"] == MessageType.IMAGE.value]
        if image_chat_df.empty:
            log.info("No images in the chat history, no ocr performed.")

        ocr_count = 0
        missing_images = 0
        log.info(f"Performing bulk ocr on {len(image_chat_df)} images out of {len(chat_df)} messages total.")
        start_time = time.time()
        for i, row in image_chat_df.iterrows():
            path = core_utils.message_id_to_path(row.message_id, MessageType.IMAGE)
            if not os.path.exists(path):
                missing_images += 1
                chat_df.at[i, "image_text"] = ""
                continue
            image_text = OCR.extract_text_from_image(path)
            chat_df.at[i, "image_text"] = image_text
            ocr_count += 1
        end_time = time.time()

        ocr_text_detected_df = image_chat_df[image_chat_df["image_text"] != ""]
        log.info(
            f"OCR detected text in {len(ocr_text_detected_df)} images out of {ocr_count} images total, {missing_images} images were missing. It took {round(end_time - start_time, 2)} seconds."
        )
        # core_utils.save_df(chat_df, CHAT_HISTORY_PATH)
        self.db.save_dataframe(chat_df, Table.CHAT_HISTORY, mode=DBSaveMode.APPEND)

    def parse_reactions(self, msg, message_reactions, malformed_count, success):
        reaction_emojis, reaction_user_ids = [], []

        for reaction in message_reactions:
            try:
                reaction_emojis.append(reaction.reaction.emoticon)
                reaction_user_ids.append(reaction.peer_id.user_id)
            except AttributeError:
                success = False
                malformed_count += 1
                log.error(f"Issue with reading message reaction emojis/user_id: {msg}.")

        return reaction_emojis, reaction_user_ids, malformed_count, success

    def count_reactions(self, message):
        return sum(reaction_count.count for reaction_count in message.reactions.results) if message.reactions is not None else 0

    def clean_chat_history(self):
        log.info("Cleaning chat history...")

        chat_df = core_utils.read_df(CHAT_HISTORY_PATH)
        users_df = stats_utils.read_users()
        filtered_df = chat_df[~chat_df["user_id"].isin(excluded_user_ids)]
        cleaned_df = filtered_df.drop(["first_name", "last_name", "username"], axis=1)
        cleaned_df = cleaned_df.merge(users_df, on="user_id")
        cleaned_df = cleaned_df[
            [
                "message_id",
                "timestamp",
                "user_id",
                "final_username",
                "text",
                "image_text",
                "reaction_emojis",
                "reaction_user_ids",
                "message_type",
            ]
        ]
        cleaned_df["timestamp"] = cleaned_df["timestamp"].dt.tz_convert(TIMEZONE)
        cleaned_df["reaction_user_ids"] = cleaned_df["reaction_user_ids"].tolist()
        stats_utils.validate_schema(cleaned_df, cleaned_chat_history_schema)

        log.info(f"Cleaned chat history df, from: {len(chat_df)} to: {len(cleaned_df)}")
        # core_utils.save_df(cleaned_df, CLEANED_CHAT_HISTORY_PATH)
        self.db.save_dataframe(cleaned_df, Table.CLEANED_CHAT_HISTORY, mode=DBSaveMode.APPEND)

    def extract_users(self):
        """Extract users from the chat history"""

        if os.path.exists(USERS_PATH):
            log.info(f"Users already extracted, {USERS_PATH} exists.")
            users_df = core_utils.read_df(USERS_PATH)
            stats_utils.validate_schema(users_df, users_schema)
            return

        log.info("Extracting users...")

        chat_df = core_utils.read_df(CHAT_HISTORY_PATH)
        unique_chat_df = chat_df.drop_duplicates("user_id")
        users_df = unique_chat_df[["user_id", "first_name", "last_name", "username"]]
        filtered_users_df = users_df[~users_df["user_id"].isin(excluded_user_ids)]
        filtered_users_df["final_username"] = filtered_users_df.apply(self.create_final_username, axis=1)
        filtered_users_df["nicknames"] = [[] for _ in range(len(filtered_users_df))]
        filtered_users_df = filtered_users_df.set_index("user_id")

        stats_utils.validate_schema(filtered_users_df, users_schema)
        # core_utils.save_df(filtered_users_df, USERS_PATH)
        self.db.save_dataframe(filtered_users_df, Table.USERS, mode=DBSaveMode.APPEND)

    def create_final_username(self, row):
        final_username = row["username"]
        if final_username is None:
            final_username = f"{row['first_name']} {row['last_name']}" if row["last_name"] is not None else row["first_name"]
        return final_username

    def generate_reactions_df(self):
        """Include all reactions and fill the missing user_ids with None"""
        log.info("Generating reactions df...")

        chat_df = core_utils.read_df(CLEANED_CHAT_HISTORY_PATH)
        users_df = core_utils.read_df(USERS_PATH)

        chat_df["len_reactions"] = chat_df["reaction_emojis"].apply(lambda x: len(x))
        chat_df["len_reaction_users"] = chat_df["reaction_user_ids"].apply(lambda x: len(x))
        clean_df = chat_df[chat_df["len_reactions"] > 0]
        reactions_df = clean_df.explode(["reaction_emojis", "reaction_user_ids"])

        reactions_df = reactions_df.merge(users_df, left_on="reaction_user_ids", right_on="user_id", how="left")
        reactions_df = reactions_df[["message_id", "timestamp", "final_username_x", "final_username_y", "text", "reaction_emojis"]]
        reactions_df.columns = ["message_id", "timestamp", "reacted_to_username", "reacting_username", "text", "emoji"]
        reactions_df = reactions_df.dropna(subset=["message_id", "timestamp", "reacted_to_username", "reacting_username", "emoji"])

        stats_utils.validate_schema(reactions_df, reactions_schema)
        # core_utils.save_df(reactions_df, REACTIONS_PATH)
        self.db.save_dataframe(reactions_df, Table.REACTIONS, mode=DBSaveMode.APPEND)

    def delete_bot_messages(self):
        """Be carefull here, you could delete someone's messages forever if you are not sure about the bot_id!"""
        chat_df = core_utils.read_df(CHAT_HISTORY_PATH)
        if chat_df is None:
            log.info("No chat history, no bot messages to delete.")
            return

        filter_dt = datetime.now(UTC) - timedelta(minutes=BOT_MESSAGE_RETENION_IN_MINUTES)

        bot_messages_df = chat_df[chat_df["user_id"] == int(BOT_ID)]
        old_bot_messages_df = bot_messages_df[bot_messages_df["timestamp"] < filter_dt]
        not_liked_old_bot_messages_df = old_bot_messages_df[old_bot_messages_df["reaction_emojis"].apply(lambda x: len(x) == 0)]

        log.info(
            f"Deleting {len(old_bot_messages_df)} bot messages older than {BOT_MESSAGE_RETENION_IN_MINUTES} minutes and without reactions."
        )
        message_ids = not_liked_old_bot_messages_df["message_id"].tolist()

        self.client_api_handler.delete_messages(message_ids)

    def cleanup_temp_dir(self):
        core_utils.create_dir(TEMP_DIR)
        files_num = len(os.listdir(TEMP_DIR))
        if os.path.exists(TEMP_DIR):
            log.info(f"Removing {files_num} files from temp dir...")
            shutil.rmtree(TEMP_DIR)

    def move_video_notes(self):
        chat_df = core_utils.read_df(CLEANED_CHAT_HISTORY_PATH)
        if chat_df is None:
            log.info("No chat history, no video notes to move.")
            return

        video_notes_df = chat_df[chat_df["message_type"] == MessageType.VIDEO_NOTE.value]
        ids = video_notes_df["message_id"].tolist()
        for message_id in ids:
            src_path = core_utils.message_id_to_path(message_id, MessageType.VIDEO)
            dst_path = core_utils.message_id_to_path(message_id, MessageType.VIDEO_NOTE)
            shutil.move(src_path, dst_path)

    def validate_data(self):
        commands_usage_df = core_utils.read_df(COMMANDS_USAGE_PATH)
        stats_utils.validate_schema(commands_usage_df, commands_usage_schema)
