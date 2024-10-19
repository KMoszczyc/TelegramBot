# def generate_chat_plots(self):
#     chat_df = stats_utils.read_df(CLEANED_CHAT_HISTORY_PATH)
#     chat_df['timestamp'] = chat_df['timestamp'].dt.tz_convert(TIMEZONE)
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