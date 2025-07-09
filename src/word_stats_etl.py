import argparse

from src.stats.word_stats import WordStats

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Download and preprocess telegram chat messages and images.')
    parser.add_argument('--days', default=1,
                        help='Specify the number of past days of chat messages that should be updated.')
    args = parser.parse_args()

    word_stats = WordStats()
    word_stats.full_update(days=int(args.days))
