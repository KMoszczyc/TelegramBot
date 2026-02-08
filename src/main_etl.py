import argparse

from src.stats.chat_etl import ChatETL

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download and preprocess telegram chat messages and images.")
    parser.add_argument("--days", default=7, help="Specify the number of past days of chat messages that should be updated.")
    args = parser.parse_args()

    chat_stats = ChatETL()
    chat_stats.update(int(args.days), bulk_ocr=False)
