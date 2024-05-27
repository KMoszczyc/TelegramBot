import logging
from datetime import datetime, timezone, timedelta
from telethon import TelegramClient
from telethon.sessions import StringSession
import os
from dotenv import load_dotenv

load_dotenv()
log = logging.getLogger(__name__)

API_ID = int(os.getenv('API_ID'))
API_HASH = os.getenv('API_HASH')
CHAT_ID = int(os.getenv('CHAT_ID'))
TEST_CHAT_ID = int(os.getenv('TEST_CHAT_ID'))
SESSION = os.getenv('SESSION')


class ClientAPIHandler:
    def __init__(self):
        log.info("Initialize telethon client API")
        # self.client = TelegramClient('ozjasz_sessionv2', api_id=API_ID, api_hash=API_HASH)
        self.client = TelegramClient(StringSession(SESSION), api_id=API_ID, api_hash=API_HASH)

        # session_str = StringSession.save(self.client.session)
        # print(session_str)

    def get_chat_history(self, days: int = 1) -> object:
        """
        days - number of past days of chat messages that will get updated
        :rtype: object
        """
        chat_history = []
        count = 0
        offset_dt = datetime.now(tz=timezone.utc) - timedelta(days=days)
        offset_timestamp = offset_dt.timestamp()
        print(offset_dt, offset_timestamp)
        with self.client:
            for msg in self.client.iter_messages(CHAT_ID, offset_date=offset_timestamp, reverse=True):
                # for msg in self.client.iter_messages(CHAT_ID, offset_date=date, reverse=True):
                if msg is None:
                    break

                if count % 10000 == 0:
                    if hasattr(msg.sender, 'first_name') and hasattr(msg.sender, 'last_name') and hasattr(msg.sender, 'username'):
                        print(msg.date, msg.id, ':', msg.sender.first_name, msg.sender.last_name, msg.sender.username, msg.sender_id, ':', msg.text)
                    else:
                        print(msg.id, msg.text)

                chat_history.append(msg)
                count += 1

        return chat_history

    def get_chat_users(self):
        with self.client:
            # channel = self.client(ResolveUsernameRequest('channel_name'))
            for _user in self.client.iter_participants(CHAT_ID):
                print(_user)
