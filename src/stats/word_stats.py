import logging
import os

from nltk import ngrams
import pandas as pd

from definitions import CLEANED_CHAT_HISTORY_PATH, STOPWORD_RATIO_THRESHOLD, polish_stopwords, CHAT_WORD_STATS_DIR_PATH

import src.stats.utils as stats_utils
import src.core.utils as core_utils
from src.models.command_args import CommandArgs

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', 1000)
pd.options.mode.chained_assignment = None

log = logging.getLogger(__name__)


class WordStats:
    def __init__(self):
        self.ngram_dfs = {}
        self.ngram_range = [1, 2, 3, 4, 5]
        self.load_ngrams()

    def load_ngrams(self):
        if not os.path.exists(CHAT_WORD_STATS_DIR_PATH):
            log.info("Chat word stats directory not found.")
            return

        for n in self.ngram_range:
            path = self.get_ngram_path(n)
            if not os.path.exists(path):
                continue

            self.ngram_dfs[n] = pd.read_parquet(self.get_ngram_path(n))

    def do_all_ngram_parquets_exist(self):
        for n in self.ngram_range:
            path = self.get_ngram_path(n)
            if not os.path.exists(path):
                return False

    def full_update(self):
        log.info(f"Do all ngram parquets exist: {self.do_all_ngram_parquets_exist()}")
        if os.path.exists(CHAT_WORD_STATS_DIR_PATH) and self.do_all_ngram_parquets_exist():
            log.info("All word stats ngram parquets exist, no need to run full update")
            return

        log.info("Ngram word stats parquets not found, running full word stats update.")
        chat_df = stats_utils.read_df(CLEANED_CHAT_HISTORY_PATH)
        self.update_ngrams(chat_df, full_update=True)

    def clean_chat_messages(self, chat_df):
        filtered_chat_df = chat_df[chat_df['text'] != ''].dropna()
        filtered_chat_df = filtered_chat_df[~filtered_chat_df['text'].str.startswith('/')]  # remove user commands
        filtered_chat_df = filtered_chat_df[~filtered_chat_df['text'].str.contains("https")]  # remove rows with links
        filtered_chat_df['text'] = filtered_chat_df['text'].str.replace(r"\(.*\)", "", regex=True)  # remove text inside braces/brackets
        filtered_chat_df['text'] = filtered_chat_df['text'].apply(core_utils.remove_punctuation)  # remove special characters
        filtered_chat_df['text'] = filtered_chat_df['text'].str.lower()

        return filtered_chat_df

    def update_ngrams(self, df, full_update=False):
        df_raw = self.clean_chat_messages(df)
        ngram_range = [1, 2, 3, 4, 5]
        for n in ngram_range:
            if (not full_update) and (not os.path.exists(self.get_ngram_path(n))):
                log.info(f'{self.get_ngram_path(n)} does not exist, skipping {n}-gram update')
                continue

            df = df_raw.copy(deep=True)
            df['ngrams'] = df['text'].str.split().apply(lambda x: list(map(' '.join, ngrams(x, n=n))))
            df = df[df['ngrams'].str.len() > 0]  # remove empty ngram rows
            latest_ngram_df = df.explode('ngrams')
            latest_ngram_df = latest_ngram_df[latest_ngram_df.apply(lambda row: self.stopword_filter(row['ngrams'], n), axis=1)]
            latest_ngram_df['ngram_id'] = latest_ngram_df.groupby('message_id').cumcount() + 1

            self.update_ngram(n, latest_ngram_df)
            self.save_ngram(n)

    def update_ngram(self, n, latest_df):
        if self.ngram_dfs.get(n) is None:
            self.ngram_dfs[n] = latest_df
            log.info(f"Init ngram ({n}, {n}) stats with {len(latest_df)} rows")
            return

        old_ngram_df = self.ngram_dfs.get(n)
        merged_ngram_df = pd.concat([old_ngram_df, latest_df], ignore_index=True).drop_duplicates(subset=['message_id', 'ngram_id'], keep='last').reset_index(drop=True)
        self.ngram_dfs[n] = merged_ngram_df

        log.info(f"Updated ngram ({n}, {n}) stats with {len(merged_ngram_df) - len(old_ngram_df)} rows, now its {len(merged_ngram_df)}")


    def save_ngram(self, n):
        if not os.path.exists(CHAT_WORD_STATS_DIR_PATH):
            core_utils.create_dir(CHAT_WORD_STATS_DIR_PATH)

        # for ngram, df in self.ngram_dfs.items():
        self.ngram_dfs[n].to_parquet(self.get_ngram_path(n))

    def count_ngrams(self, df):
        return df['ngrams'].value_counts()

    def stopword_filter(self, text, n):
        if self.is_nan(text):
            return True

        if n == 1:  # for a single word we dont want it to be a stopword 100%
            return not stats_utils.contains_stopwords(text, polish_stopwords)

        return (not stats_utils.is_ngram_contaminated_by_stopwords(text, STOPWORD_RATIO_THRESHOLD, polish_stopwords)) and stats_utils.is_ngram_valid(text)

    def is_nan(self, text):
        return text != text

    def get_ngram_path(self, n):
        filename = f'ngram_{n}.parquet'
        return os.path.join(CHAT_WORD_STATS_DIR_PATH, filename)

    def filter_ngrams(self, command_args: CommandArgs):
        """Filter ngrams by time and user"""

        fitlered_ngram_dfs = {}
        for n, df in self.ngram_dfs.items():
            fitlered_ngram_dfs[n] = stats_utils.filter_by_time_df(df, command_args)

            log.info(f"n: {n}, {df.columns}")
            if command_args.user is not None:
                fitlered_ngram_dfs[n] = df[df['final_username'] == command_args.user]

        return fitlered_ngram_dfs

# chat_df = stats_utils.read_df(CLEANED_CHAT_HISTORY_PATH)
# ws = WordStats()
# for i, df in ws.ngram_dfs.items():
#     print(df.head(5))
#
# print(ws.count_ngrams(ws.ngram_dfs[1])[:10])
#
# ws2 = WordStats()
# ws2.update_ngrams(chat_df)
# for i, df in ws2.ngram_dfs.items():
#     print(df.head(5))
# print(ws2.count_ngrams(ws2.ngram_dfs[1])[:10])
