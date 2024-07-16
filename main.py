from src.core.client_api_handler import ClientAPIHandler
from src.core.ozjasz_bot import OzjaszBot

from src.stats.chat_etl import ChatETL

if __name__ == '__main__':
    # Before running the bot, update the data from the past 24h first.
    api_handler = ClientAPIHandler()
    chat_stats = ChatETL(api_handler)
    chat_stats.update(1)

    OzjaszBot()
