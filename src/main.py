import os.path

from definitions import CHAT_HISTORY_PATH
from src.core.client_api_handler import ClientAPIHandler
from src.core.ozjasz_bot import OzjaszBot

from src.stats.chat_etl import ChatETL

if __name__ == '__main__':
    OzjaszBot()
