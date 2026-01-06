import os
import random
from datetime import timedelta
from zoneinfo import ZoneInfo
import pandas as pd
from dotenv import load_dotenv
import telegram
from sympy.physics.units import minute

from definitions import polish_holidays_df, TIMEZONE, USERS_PATH, CLEANED_CHAT_HISTORY_PATH
import src.core.utils as core_utils
import src.stats.utils as stats_utils

pd.options.mode.chained_assignment = None
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', 1000)

load_dotenv()
BOT_ID = int(os.getenv('BOT_ID'))
CHAT_ID = int(os.getenv('CHAT_ID'))
THREAD_ID = int(os.getenv('BOT_CHAT_THREAD_ID'))


class Holidays:
    def __init__(self, job_queue, credits_obj):
        self.job_queue = job_queue
        self.users_df = core_utils.read_df(USERS_PATH)
        self.credits = credits_obj
        self.users_df = core_utils.read_df(USERS_PATH)
        self.chat_df = core_utils.read_df(CLEANED_CHAT_HISTORY_PATH)

        self.run()

    def run(self):
        df = self.preprocess_holidays()
        for index, row in df.iterrows():
            date, holiday_name, holiday_info = row['date'], row['holiday_name'], row['message']
            self.job_queue.run_once(callback=lambda context: self.gift_credits_to_all_users(context, holiday_name, holiday_info), when=date)

    async def gift_credits_to_all_users(self, context, holiday_name, holiday_info):
        amount = random.randint(500, 3000)
        self.credits.give_credits_to_all(amount)
        message = f"Dziś *{holiday_name}*, w nagrodę wszyscy otrzymują *{amount} kredytów!*\n\n{holiday_info}"
        message = stats_utils.escape_special_characters(message)
        await context.bot.send_message(chat_id=CHAT_ID, text=message, message_thread_id=THREAD_ID, parse_mode=telegram.constants.ParseMode.MARKDOWN_V2)

    def preprocess_holidays(self):
        current_dt = core_utils.get_dt_now()
        raw_holidays_df = polish_holidays_df.copy(deep=True)
        raw_holidays_df['date'] = pd.to_datetime(raw_holidays_df['date'], format='%d-%m-%Y', utc=True).dt.tz_convert(tz=ZoneInfo(TIMEZONE))

        years_num = 2  # for how many years should we load holiday jobs - 2 seem enough (current and the next), as Ozjasz is frequently updated, I guess more is unnecessary and could put some overhead?
        years = [current_dt.year + i for i in range(years_num)]
        dfs = []
        for year in years:
            df = raw_holidays_df.copy(deep=True)
            df['date'] = df['date'].apply(lambda x: x.replace(year=year, hour=22, minute=13))
            dfs.append(df)

        merged_df = pd.concat(dfs, ignore_index=True)
        merged_df = merged_df[merged_df['date'] >= current_dt]

        return merged_df
