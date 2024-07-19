import os
import logging
from datetime import datetime, timezone, timedelta

from telethon import TelegramClient, functions
from telethon.sessions import StringSession
from dotenv import load_dotenv

from definitions import CHAT_IMAGES_DIR_PATH, CHAT_VIDEOS_DIR_PATH, CHAT_GIFS_DIR_PATH, MessageType
import src.core.utils as core_utils
import src.stats.utils as stats_utils

load_dotenv()
log = logging.getLogger(__name__)
log.info(os.environ)

API_ID = int(os.getenv('API_ID'))
API_HASH = os.getenv('API_HASH')
CHAT_ID = int(os.getenv('CHAT_ID'))
TEST_CHAT_ID = int(os.getenv('TEST_CHAT_ID'))
SESSION = os.getenv('SESSION')
BOT_ID = int(os.getenv('BOT_ID'))


class ClientAPIHandler:
    def __init__(self):
        log.info("Initialize telethon client API")
        # self.client = TelegramClient('ozjasz_sessionv2', api_id=API_ID, api_hash=API_HASH)
        self.client = TelegramClient(StringSession(SESSION), api_id=API_ID, api_hash=API_HASH)

        # session_str = StringSession.save(self.client.session)
        # print(session_str)
        core_utils.create_dir(CHAT_IMAGES_DIR_PATH)

    def get_chat_history(self, days: int = 1) -> object:
        """
        days - number of past days of chat messages that will get updated
        :rtype: object
        """

        async def helper():
            chat_history = []
            message_types = []
            count = 0
            offset_dt = datetime.now(tz=timezone.utc) - timedelta(days=days)
            offset_timestamp = offset_dt.timestamp()
            async with self.client:
                async for msg in self.client.iter_messages(CHAT_ID, offset_date=offset_timestamp, reverse=True):
                    # for msg in self.client.iter_messages(CHAT_ID, offset_date=date, reverse=True):
                    if msg is None:
                        break
                    if count % 10000 == 0:
                        if hasattr(msg.sender, 'first_name') and hasattr(msg.sender, 'last_name') and hasattr(msg.sender, 'username'):
                            log.info(f"{msg.date}, {msg.id}, ':', {msg.sender.first_name}, {msg.sender.last_name}, {msg.sender.username}, {msg.sender_id}, ':', {msg.text}")
                        else:
                            log.info(f"{msg.id}, {msg.text}")

                    message_type = core_utils.get_message_type(msg)
                    await core_utils.download_media(msg, message_type)

                    message_types.append(message_type)
                    chat_history.append(msg)
                    count += 1
            return chat_history, message_types

        with self.client:
            chat_history = self.client.loop.run_until_complete(helper())
            return chat_history

    def get_reactions(self, message_ids: list) -> dict:
        """Get all reactions from given message_ids. Used when a message has over 3 reactions, as recent reactions in the chat_history have only 3 last reactions.
        :param message_ids: a list of message ids
        :return:
        """

        async def helper():
            async with self.client:
                message_reactions = {}
                for message_id in message_ids:
                    message_reactions[message_id] = await self.client(functions.messages.GetMessageReactionsListRequest(peer=CHAT_ID, id=message_id, limit=100))
                return message_reactions

        with self.client:
            return self.client.loop.run_until_complete(helper())

    def get_chat_users(self):
        with self.client:
            # channel = self.client(ResolveUsernameRequest('channel_name'))
            for _user in self.client.iter_participants(CHAT_ID):
                print(_user)

    def delete_messages(self, message_ids: list):
        """Be carefull here, you could delete someone's messages forever if you are not sure about the bot_id!"""

        if not stats_utils.check_bot_messages(message_ids, BOT_ID):
            log.info("Not all messages set for deletion belong to a bot, deletion stopped!")
            return
        log.info("All message ids correspond to bot messages. Proceeding with deletion.")

        async def helper():
            async with self.client:
                await self.client.delete_messages(entity=CHAT_ID, message_ids=message_ids)

        with self.client:
            return self.client.loop.run_until_complete(helper())
