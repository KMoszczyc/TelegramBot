import logging
from datetime import datetime, timezone, timedelta

# import matplotlib.pyplot as plt
# import matplotlib.ticker as mtick
# from sklearn.feature_extraction.text import CountVectorizer
import numpy as np
import pandas as pd

from definitions import CHAT_HISTORY_PATH, USERS_PATH, CLEANED_CHAT_HISTORY_PATH, POLISH_STOPWORDS_PATH, REACTIONS_PATH
import src.stats.utils as stats_utils
import src.core.utils as core_utils

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', 1000)

log = logging.getLogger(__name__)
excluded_user_ids = [6455867316, 6455867316, 1660481027, 1626698260, 1653222205, 1626673718, 2103796402]


class ChatETL:
    """Core chat downloader and data processor."""

    def __init__(self, client_api_handler):
        self.client_api_handler = client_api_handler
        self.metadata = stats_utils.load_metadata()

    def update(self, days: int):
        log.info(f"Running chat update for the past: {days} days")

        self.download_chat_history(days)
        self.extract_users()
        self.clean_chat_history()
        self.generate_reactions_df()

    def download_chat_history(self, days):
        self.metadata = stats_utils.load_metadata()
        # latest_messages = self.client_api_handler.get_chat_history(self.metadata['last_message_utc_timestamp'])
        latest_messages = self.client_api_handler.get_chat_history(days)

        columns = ['message_id', 'timestamp', 'user_id', 'first_name', 'last_name', 'username', 'text', 'all_emojis', 'reaction_emojis', 'reaction_user_ids']
        data = []

        malformed_count = 0
        for message in latest_messages:
            all_emojis, reaction_emojis, reaction_user_ids = [], [], []
            if message is None or message.sender is None:
                continue
            success = True

            if message.reactions is not None and message.reactions.recent_reactions is not None:
                for reaction in message.reactions.recent_reactions:
                    try:
                        reaction_emojis.append(reaction.reaction.emoticon)
                        reaction_user_ids.append(reaction.peer_id.user_id)
                    except AttributeError:
                        success = False
                        malformed_count += 1
                        log.error(f'Issue with reading message reaction emojis/user_id: {message}.')
                for reaction_count in message.reactions.results:
                    count = reaction_count.count
                    emoji = reaction_count.reaction.emoticon
                    all_emojis.extend([emoji] * count)

            if not success:
                continue

            single_message_data = [message.id, message.date, message.sender_id, message.sender.first_name, message.sender.last_name, message.sender.username,
                                   message.text,
                                   all_emojis,
                                   reaction_emojis,
                                   reaction_user_ids]
            data.append(single_message_data)

        old_chat_df = stats_utils.read_df(CHAT_HISTORY_PATH)
        latest_chat_df = pd.DataFrame(data, columns=columns)

        log.info(f'New {len(latest_chat_df)} messages since {datetime.now(tz=timezone.utc) - timedelta(days=days)} with {malformed_count} malformed records.')
        # print(latest_chat_df.head(5))
        if old_chat_df is not None and not latest_chat_df.empty:
            # merged_chat_df = pd.concat([old_chat_df, latest_chat_df]).drop_duplicates(subset='message_id').reset_index(drop=True)
            merged_chat_df = pd.concat([old_chat_df, latest_chat_df], ignore_index=True).drop_duplicates(subset='message_id', keep='last').reset_index(drop=True)
        elif old_chat_df is not None:
            merged_chat_df = old_chat_df
        elif not latest_chat_df.empty:
            merged_chat_df = latest_chat_df
        else:
            return

        merged_chat_df = merged_chat_df.sort_values(by='timestamp').reset_index(drop=True)

        print('merged_chat_df', merged_chat_df.tail(5))
        self.metadata['last_message_id'] = merged_chat_df['message_id'].iloc[-1]
        self.metadata['last_message_utc_timestamp'] = int(merged_chat_df['timestamp'].iloc[-1].replace(tzinfo=timezone.utc).astimezone(tz=None).timestamp())
        self.metadata['1_day_offset_utc_timestamp'] = int((merged_chat_df['timestamp'].iloc[-1].replace(tzinfo=timezone.utc).astimezone(tz=None) - timedelta(days=1)).timestamp())
        self.metadata['last_message_dt'] = merged_chat_df['timestamp'].iloc[-1]
        self.metadata['last_update'] = datetime.now(tz=timezone.utc)
        self.metadata['message_count'] = len(merged_chat_df)
        self.metadata['new_latest_data'] = True

        stats_utils.save_metadata(self.metadata)
        stats_utils.save_df(merged_chat_df, CHAT_HISTORY_PATH)

    def clean_chat_history(self):
        chat_df = stats_utils.read_df(CHAT_HISTORY_PATH)
        users_df = stats_utils.read_users()
        filtered_df = chat_df[~chat_df['user_id'].isin(excluded_user_ids)]
        cleaned_df = filtered_df.drop(['first_name', 'last_name', 'username'], axis=1)
        cleaned_df = cleaned_df.merge(users_df, on='user_id')
        cleaned_df = cleaned_df[['message_id', 'timestamp', 'user_id', 'final_username', 'text', 'all_emojis', 'reaction_emojis', 'reaction_user_ids']]
        cleaned_df['timestamp'] = cleaned_df['timestamp'].dt.tz_convert('Europe/Warsaw')

        # print(users_df)
        # print(cleaned_df.head(5))

        log.info(f'Cleaned chat history df, from: {len(chat_df)} to: {len(cleaned_df)}')
        stats_utils.save_df(cleaned_df, CLEANED_CHAT_HISTORY_PATH)

    def extract_users(self):
        chat_df = stats_utils.read_df(CHAT_HISTORY_PATH)
        unique_chat_df = chat_df.drop_duplicates('user_id')
        users_df = unique_chat_df[['user_id', 'first_name', 'last_name', 'username']]
        filtered_users_df = users_df[~users_df['user_id'].isin(excluded_user_ids)]
        filtered_users_df['final_username'] = filtered_users_df.apply(self.create_final_username, axis=1)

        stats_utils.save_df(filtered_users_df, USERS_PATH)

    def create_final_username(self, row):
        final_username = row['username']
        if final_username is None:
            final_username = f"{row['first_name']} {row['last_name']}" if row['last_name'] is not None else row['first_name']
        return final_username

    def generate_reactions_df(self):
        chat_df = stats_utils.read_df(CLEANED_CHAT_HISTORY_PATH)
        users_df = stats_utils.read_df(USERS_PATH)

        chat_df['len_reactions'] = chat_df['reaction_emojis'].apply(lambda x: len(x))
        chat_df['len_reaction_users'] = chat_df['reaction_user_ids'].apply(lambda x: len(x))

        malformed_df = chat_df[chat_df['len_reactions'] != chat_df['len_reaction_users']]
        clean_df = chat_df[(chat_df['len_reactions'] == chat_df['len_reaction_users']) & (chat_df['len_reactions'] > 0)]

        reactions_df = clean_df.explode(['reaction_emojis', 'reaction_user_ids'])
        reactions_df = reactions_df.merge(users_df, left_on='reaction_user_ids', right_on='user_id')
        reactions_df = reactions_df[['message_id', 'timestamp', 'final_username_x', 'final_username_y', 'text', 'reaction_emojis']]
        reactions_df.columns = ['message_id', 'timestamp', 'reacted_to_username', 'reacting_username', 'text', 'reaction_emoji']
        stats_utils.save_df(reactions_df, REACTIONS_PATH)

        # print(reactions_df.groupby('reacting_username')['reaction_emoji'].value_counts())
        # reactions_by_username_count_df = reactions_df.groupby('reacting_username')['reaction_emoji'].value_counts().unstack('reaction_emoji', fill_value=0).T.sort_values('Kamil', ascending=False)
        # print(tabulate(reactions_by_username_count_df, headers='keys', tablefmt='psql'))

        # grouped = reactions_df.groupby(['reacting_username', 'reaction_emoji'], sort=True).agg(count=('reaction_emoji', 'count'))
        # given_reactions_by_username_count_df = grouped.sort_values(['reacting_username', 'count'], ascending=False).groupby('reacting_username').head(5)
        # print(tabulate(given_reactions_by_username_count_df.reset_index(), headers='keys', tablefmt='psql', showindex=False))
        #
        # grouped = reactions_df.groupby(['reacted_to_username', 'reaction_emoji'], sort=True).agg(count=('reaction_emoji', 'count'))
        # received_reactions_by_username_count_df = grouped.sort_values(['reacted_to_username', 'count'], ascending=False).groupby('reacted_to_username').head(5)
        # print(tabulate(received_reactions_by_username_count_df.reset_index(), headers='keys', tablefmt='psql', showindex=False))

        # print('html:', given_reactions_by_username_count_df.to_html())
        # self.given_reactions_by_username_count_html = given_reactions_by_username_count_df.to_html()
        # self.received_reactions_by_username_count_html = received_reactions_by_username_count_df.to_html()

    # def generate_chat_plots(self):
    #     chat_df = stats_utils.read_df(CLEANED_CHAT_HISTORY_PATH)
    #     chat_df['timestamp'] = chat_df['timestamp'].dt.tz_convert('Europe/Warsaw')
    #     chat_df['date'] = chat_df['timestamp'].dt.date
    #     chat_df['year'] = chat_df['timestamp'].dt.year
    #     chat_df['month'] = chat_df['timestamp'].dt.month_name()
    #     chat_df['day_name'] = chat_df['timestamp'].dt.day_name()
    #     chat_df['day'] = chat_df['timestamp'].dt.day
    #     chat_df['hour'] = chat_df['timestamp'].dt.hour
    #     chat_df['minute'] = (chat_df['timestamp'].dt.floor('15Min', ambiguous=True)).dt.minute
    #
    #     months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
    #     days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    #     # messages_by_month = chat_df.groupby('month').size().reindex(months, axis=0)
    #     # messages_by_day = chat_df.groupby('day_name').size().reindex(days, axis=0)
    #     # messages_by_hour = chat_df.groupby('hour').size()
    #
    #     messages_by_month = chat_df.groupby(['year', 'month']).size().groupby('month').mean().reindex(months, axis=0)
    #     messages_by_day = chat_df.groupby(['date', 'day_name']).size().groupby('day_name').mean().reindex(days, axis=0)
    #     messages_by_hour = chat_df.groupby(['date', 'hour']).size().groupby('hour').mean()
    #
    #     self.generate_chat_plots_per_person(chat_df)
    #     # ax = messages_by_month.plot(x='month', y='messages')
    #     # ax.tick_params(axis='x', labelrotation=45)
    #     # plt.xticks([0,1,2,3,4,5,6,7,8,9,10,11], months)
    #     # plt.tight_layout()
    #     # plt.xlabel("month")
    #     # plt.ylabel("messages")
    #     # plt.show()
    #     #
    #     # messages_by_day.plot(x='day', y='messages')
    #     # plt.xlabel("day")
    #     # plt.ylabel("messages")
    #     # plt.show()
    #     #
    #     # messages_by_hour.plot(x='hour', y='messages')
    #     # plt.xticks(list(range(0, 24)))
    #     # plt.xlabel("hour")
    #     # plt.ylabel("messages")
    #     # plt.show()
    #
    # def generate_chat_plots_per_person(self, chat_df):
    #
    #     usernames = chat_df['final_username'].unique().tolist()
    #     months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
    #     days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    #     name = "tab20"
    #     cmap = plt.colormaps[name]  # type: matplotlib.colors.ListedColormap
    #     colors = cmap.colors  # type: list
    #
    #     fig, ax = plt.subplots()
    #     ax.set_prop_cycle(color=colors)
    #
    #     for username in usernames:
    #         chat_user_df = chat_df[chat_df['final_username'] == username]
    #         messages_by_month = chat_user_df.groupby(['month']).size().reindex(months, axis=0)
    #         month_sum = messages_by_month.sum()
    #         messages_by_month_relative = messages_by_month / month_sum * 100
    #         messages_by_month_relative.plot(x='month', y='messages', label=username)
    #
    #     ax.tick_params(axis='x', labelrotation=45)
    #     plt.xticks([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11], months)
    #     plt.locator_params(axis='y', nbins=18)
    #     ax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))
    #
    #     plt.tight_layout()
    #     plt.xlabel("month")
    #     plt.ylabel("messages")
    #     plt.legend()
    #     plt.show()
    #
    #     fig, ax = plt.subplots()
    #     ax.set_prop_cycle(color=colors)
    #
    #     for username in usernames:
    #         chat_user_df = chat_df[chat_df['final_username'] == username]
    #         messages_by_day = chat_user_df.groupby(['day_name']).size().reindex(days, axis=0)
    #         day_sum = messages_by_day.sum()
    #         messages_by_day_relative = messages_by_day / day_sum * 100
    #         messages_by_day_relative.plot(x='month', y='messages', label=username)
    #     plt.xlabel("day")
    #     plt.ylabel("messages")
    #     plt.locator_params(axis='y', nbins=18)
    #     ax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))
    #
    #     plt.legend()
    #     plt.show()
    #
    #     fig, ax = plt.subplots()
    #     ax.set_prop_cycle(color=colors)
    #
    #     for username in usernames:
    #         chat_user_df = chat_df[chat_df['final_username'] == username]
    #         messages_by_hour = chat_user_df.groupby(['hour']).size()
    #         hour_sum = messages_by_hour.sum()
    #         messages_by_hour_relative = messages_by_hour / hour_sum * 100
    #
    #         messages_by_hour_relative.plot(x='month', y='messages', label=username)
    #     plt.xticks(list(range(0, 24)))
    #     ax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))
    #     plt.xlabel("hour")
    #     plt.ylabel("messages")
    #     plt.locator_params(axis='y', nbins=20)
    #
    #     plt.legend()
    #     plt.show()

    # def generate_word_stats(self):
    #     chat_df = stats_utils.read_df(CLEANED_CHAT_HISTORY_PATH)
    #     polish_stopwords = core_utils.read_str_file(POLISH_STOPWORDS_PATH)
    #     filtered_chat_df = chat_df[chat_df['text'] != ''].dropna()
    #     filtered_chat_df['text'] = filtered_chat_df['text'].str.replace(r"https:\/\/.*", "", regex=True)
    #     filtered_chat_df['text'] = filtered_chat_df['text'].str.replace(r"\(.*\)", "", regex=True)  # remove text inside braces/brackets
    #     filtered_chat_df['text'] = filtered_chat_df['text'].str.replace('[^\w\s]', '')  # remove special characters
    #
    #     print(len(chat_df), len(filtered_chat_df))
    #     print(filtered_chat_df.head(10))
    #     print(filtered_chat_df.tail(10))
    #
    #     print(filtered_chat_df.info())
    #
    #     cv = CountVectorizer(ngram_range=(7, 7))
    #     cv_fit = cv.fit_transform(filtered_chat_df['text'])
    #     word_list = cv.get_feature_names_out()
    #
    #     # Added [0] here to get a 1d-array for iteration by the zip function.
    #     counts = np.asarray(cv_fit.sum(axis=0))[0]
    #     word_counts = dict(zip(word_list, counts))
    #     sorted_word_counts = sorted(word_counts.items(), key=lambda x: x[1], reverse=True)
    #
    #     # cleaned_word_counts = [(word, count) for word, count in sorted_word_counts if word not in polish_stopwords] # single words
    #     # cleaned_word_counts = [(words, count) for words, count in sorted_word_counts if not contains_stopwords(words, polish_stopwords)] # bigrams/trigrams
    #
    #     for word, count in sorted_word_counts[:200]:
    #         print(f'{word:60}- {count}')
    #
    #     print(filtered_chat_df[filtered_chat_df['text'].str.contains('dobrą opinię jeżeli chodzi it')].head(10))

    # async def given_reactions_counts(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
    #     # self.generate_reactions_df()
    #     print('html', self.given_reactions_by_username_count_html)
    #     await context.bot.send_message(chat_id=update.effective_chat.id, text=self.given_reactions_by_username_count_html, parse_mode='html')


def contains_stopwords(s, stopwords):
    return any(word in stopwords for word in s.split())

