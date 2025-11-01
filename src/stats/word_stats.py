import logging
import os

from nltk import ngrams
import pandas as pd

from definitions import CLEANED_CHAT_HISTORY_PATH, STOPWORD_RATIO_THRESHOLD, polish_stopwords, CHAT_WORD_STATS_DIR_PATH, WORD_STATS_UPDATE_LOCK_PATH, PeriodFilterMode

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
        return True

    def full_update(self, days=None):
        log.info(f"Do all ngram parquets exist: {self.do_all_ngram_parquets_exist()}")
        if os.path.exists(CHAT_WORD_STATS_DIR_PATH) and self.do_all_ngram_parquets_exist() and days is None:
            log.info("All word stats ngram parquets exist, no need to run full update")
            return

        if not os.path.exists(CLEANED_CHAT_HISTORY_PATH):
            log.error(f"Cleaned chat history not found at {CLEANED_CHAT_HISTORY_PATH}")
            return

        log.info("Ngram word stats parquets not found, running full word stats update.")
        chat_df = stats_utils.read_df(CLEANED_CHAT_HISTORY_PATH)

        if days:
            chat_df = stats_utils.filter_by_time_df(chat_df, CommandArgs(period_mode=PeriodFilterMode.DAY, period_time=days))
            log.info(f"Updating only last {days} days for word stats, {len(chat_df)} rows")

        if len(chat_df) == 0:
            log.info("Chat history is empty, skipping word stats update")
            return

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
        if self.is_word_stats_update_locked():
            log.info("Word stats update is locked, skipping update")
            return

        df_raw = self.clean_chat_messages(df)
        ngram_range = [1, 2, 3, 4, 5]
        self.create_lock_file()
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

        self.remove_lock_file()

    def update_ngram(self, n, latest_df):
        columns = ['timestamp', 'final_username', 'message_id', 'ngram_id', 'ngrams']  # try to optimize memory
        latest_df = latest_df[columns]
        if self.ngram_dfs.get(n) is None:
            self.ngram_dfs[n] = latest_df
            log.info(f"Init ngram-{n} stats with {len(latest_df)} rows")
            return

        old_ngram_df = self.ngram_dfs.get(n)
        merged_ngram_df = pd.concat([old_ngram_df, latest_df], ignore_index=True).drop_duplicates(subset=['message_id', 'ngram_id'], keep='last').reset_index(drop=True)
        self.ngram_dfs[n] = merged_ngram_df

        log.info(f"Updated ngram-{n} stats with {len(merged_ngram_df) - len(old_ngram_df)} rows, now its {len(merged_ngram_df)}")

    def save_ngram(self, n):
        if not os.path.exists(CHAT_WORD_STATS_DIR_PATH):
            core_utils.create_dir(CHAT_WORD_STATS_DIR_PATH)

        # for ngram, df in self.ngram_dfs.items():
        self.ngram_dfs[n].to_parquet(self.get_ngram_path(n))

    def wordstats_cmd_handler(self, filtered_ngram_dfs, command_args, text_filter):
        n = command_args.named_args['ngram'] if 'ngram' in command_args.named_args else None
        exact_match = 'exact_match' in command_args.named_args
        groupby_user = 'user' in command_args.named_args
        if text_filter is not None and exact_match:
            filter_ngram = len(text_filter.split())
            ngram_df = filtered_ngram_dfs[filter_ngram]
            merged_df = ngram_df[ngram_df['ngrams'].str.lower().str.fullmatch(text_filter)]
            groupby_cols = ['final_username', 'ngrams']
        elif text_filter is not None:  # partial match
            dfs = [df[df['ngrams'].str.lower().str.contains(text_filter)] for df in filtered_ngram_dfs.values()] if n is None else [
                filtered_ngram_dfs[n][filtered_ngram_dfs[n]['ngrams'].str.contains(text_filter)]]
            merged_df = pd.concat(dfs)
            groupby_cols = ['final_username', 'ngrams'] if groupby_user else ['ngrams']
        elif groupby_user is not None:
            merged_df = pd.concat(filtered_ngram_dfs)
            groupby_cols = ['final_username', 'ngrams']
        else:
            merged_df = pd.concat(filtered_ngram_dfs)
            groupby_cols = ['ngrams']

        ngram_counts_df = merged_df.groupby(groupby_cols).size().reset_index(name="counts").sort_values(by='counts', ascending=False)
        return self.display_ngram_counts(command_args, ngram_counts_df, groupby_cols)

    def decode_diacritic_accents(self, dfs):
        for n, df in dfs.items():
            df['ngrams'] = df['ngrams'].apply(stats_utils.remove_diactric_accents)
            dfs[n] = df
        return dfs

    def display_ngram_counts(self, command_args, df, groupby_cols):
        user_col = 'final_username' if 'final_username' in groupby_cols else None
        ngram_col = 'ngrams' if 'ngrams' in groupby_cols else None
        count_col = 'counts'

        text = core_utils.generate_response_headline(command_args, label='``` Word stats')
        max_len_username = core_utils.max_str_length_in_col(df[user_col]) if user_col is not None else -1
        max_len_ngram = core_utils.max_str_length_in_col(df['ngrams'].head(10)) if ngram_col is not None else -1

        for i, (index, row) in enumerate(df.head(10).iterrows()):
            if user_col and ngram_col and count_col:
                text += f"\n{i + 1}.".ljust(4) + f" {row[user_col]}:".ljust(max_len_username + 3) + f"{row[ngram_col]}".ljust(max_len_ngram + 3) + f"{row[count_col]}"
            elif user_col and count_col:
                text += f"\n{i + 1}.".ljust(4) + f" {row[user_col]}:".ljust(max_len_username + 3) + f"{row[count_col]}"
            elif ngram_col and count_col:
                text += f"\n{i + 1}.".ljust(4) + f" {row[ngram_col]}:".ljust(max_len_ngram + 3) + f"{row[count_col]}"

        text += "```"
        return stats_utils.escape_special_characters(text)

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

    def filter_ngrams(self, command_args: CommandArgs, text_filter=None):
        """Filter ngrams by time and user"""

        fitlered_ngram_dfs = {}
        for n, df in self.ngram_dfs.items():
            if command_args.user is not None:
                df = df[df['final_username'] == command_args.user]
            df = stats_utils.filter_by_time_df(df, command_args)
            if text_filter is not None:
                df = df[df['ngrams'].str.contains(text_filter)]
            fitlered_ngram_dfs[n] = df

        return fitlered_ngram_dfs

    def create_lock_file(self):
        if not os.path.exists(WORD_STATS_UPDATE_LOCK_PATH):
            core_utils.create_dir(CHAT_WORD_STATS_DIR_PATH)
            open(WORD_STATS_UPDATE_LOCK_PATH, 'a').close()
            log.info(f"Word stats update lock at {WORD_STATS_UPDATE_LOCK_PATH} created.")

    def remove_lock_file(self):
        if os.path.exists(WORD_STATS_UPDATE_LOCK_PATH):
            os.remove(WORD_STATS_UPDATE_LOCK_PATH)
            log.info(f"Word stats update lock at {WORD_STATS_UPDATE_LOCK_PATH} removed.")

    def is_word_stats_update_locked(self):
        return os.path.exists(WORD_STATS_UPDATE_LOCK_PATH)
