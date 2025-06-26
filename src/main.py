import os.path

from definitions import CHAT_HISTORY_PATH
from src.core.client_api_handler import ClientAPIHandler
from src.core.ozjasz_bot import OzjaszBot

from src.stats.chat_etl import ChatETL

if __name__ == '__main__':
    # Before running the bot, update the data first.
    api_handler = ClientAPIHandler()
    chat_stats = ChatETL(api_handler, bulk_word_stats=True, bulk_ocr=False)
    if os.path.exists(CHAT_HISTORY_PATH):
        chat_stats.update(1)
    else:
        chat_stats.update(3000)

    OzjaszBot()
