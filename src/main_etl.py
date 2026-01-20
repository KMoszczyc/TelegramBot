import argparse

from src.core.client_api_handler import ClientAPIHandler
from src.stats.chat_etl import ChatETL

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Download and preprocess telegram chat messages and images.')
    parser.add_argument('--days', default=7,
                        help='Specify the number of past days of chat messages that should be updated.')
    args = parser.parse_args()

    api_handler = ClientAPIHandler()
    chat_stats = ChatETL(api_handler)
    chat_stats.update(int(args.days), bulk_ocr=False)
