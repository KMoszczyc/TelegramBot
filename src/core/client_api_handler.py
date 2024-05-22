import pandas as pd
from telethon import TelegramClient, events, sync
import os
from dotenv import load_dotenv

load_dotenv()

API_ID = int(os.getenv('API_ID'))
API_HASH = os.getenv('API_HASH')
CHAT_ID = int(os.getenv('CHAT_ID'))
TEST_CHAT_ID = int(os.getenv('TEST_CHAT_ID'))


class ClientAPIHandler:
    def __init__(self):
        self.client = TelegramClient('ozjasz_session', api_id=API_ID, api_hash=API_HASH)

    def get_chat_history(self, last_timestamp=None):
        chat_history = []
        count = 0
        print('last_timestamp:', last_timestamp)
        with self.client:
            # 10 is the limit on how many messages to fetch. Remove or change for more.
            for msg in self.client.iter_messages(CHAT_ID, offset_date=last_timestamp, reverse=True):
                if msg is None:
                    break

                if count % 10000 == 0:
                    if hasattr(msg.sender, 'first_name') and hasattr(msg.sender, 'last_name') and hasattr(msg.sender, 'username'):
                        print(msg.date, msg.id,  ':', msg.sender.first_name, msg.sender.last_name, msg.sender.username, msg.sender_id, ':', msg.text)
                    else:
                        print(msg.id, msg.text)

                chat_history.append(msg)
                count += 1

        return chat_history


