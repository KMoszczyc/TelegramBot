from telegram import Update
from telegram.ext import ContextTypes
from enum import Enum
import logging
import datetime
from datetime import timezone, timedelta
from zoneinfo import ZoneInfo

from definitions import CHAT_HISTORY_PATH, USERS_PATH, METADATA_PATH, CLEANED_CHAT_HISTORY_PATH, POLISH_STOPWORDS_PATH, REACTIONS_PATH, PeriodFilterMode
import src.stats.utils as stats_utils

log = logging.getLogger(__name__)


class ChatCommands:
    def __init__(self):
        self.chat_df = stats_utils.read_df(CLEANED_CHAT_HISTORY_PATH)
        self.reactions_df = stats_utils.read_df(REACTIONS_PATH)
        self.users_df = stats_utils.read_df(USERS_PATH)

    async def summary(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        chat_df, reactions_df, mode = self.preprocess_input(context.args)

        print(chat_df.head(5))
        print(chat_df.tail(5))
        print(len(chat_df))

        # print(self.chat_df.tail(5))
        text = f"""Chat summary ({mode.value}):
        - {len(chat_df)} messages, {len(reactions_df)} reactions in total
                """

        await context.bot.send_message(chat_id=update.effective_chat.id, text=text)

    async def top_messages_by_reactions(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Top 5 messages from selected time period by number of reactions"""
        chat_df, reactions_df, mode = self.preprocess_input(context.args)
        chat_df = chat_df[chat_df['text'] != '']
        # chat_df['all_emojis'] = chat_df['all_emojis'].fillna([], inplace=True)
        chat_df['reactions_num'] = chat_df['all_emojis'].apply(lambda x: len(x))
        chat_df = chat_df.sort_values('reactions_num', ascending=False)

        text = f"Top Cinco messages ({mode.value}):"
        for i, (index, row) in enumerate(chat_df.head(5).iterrows()):
            text += f"\n{i + 1}. {row['final_username']}: {row['text']} [{''.join(row['all_emojis'])}]"

        await context.bot.send_message(chat_id=update.effective_chat.id, text=text)

    def parse_mode(self, args):
        try:
            return PeriodFilterMode(args[0]) if len(args) == 1 else PeriodFilterMode.TOTAL
        except ValueError as e:
            log.error(f"There is no such command mode as {args}. {e}")
            return PeriodFilterMode.TOTAL

    def get_today_midnight_dt(self):
        return datetime.datetime.now().replace(tzinfo=ZoneInfo('Europe/Warsaw')).replace(hour=0, minute=0, second=0, microsecond=0)

    def filter_df(self, df, mode):
        today_dt = self.get_today_midnight_dt()
        print('mode', mode, today_dt)
        match mode:
            case PeriodFilterMode.TODAY:
                return df[df['timestamp'] >= today_dt]
            case PeriodFilterMode.YESTERDAY:
                return df[(df['timestamp'] >= today_dt - datetime.timedelta(days=1)) & (df['timestamp'] < today_dt)]
            case PeriodFilterMode.WEEK:
                return df[df['timestamp'] >= today_dt - datetime.timedelta(days=7)]
            case PeriodFilterMode.MONTH:
                return df[df['timestamp'] >= today_dt - datetime.timedelta(days=30)]
            case PeriodFilterMode.YEAR:
                return df[df['timestamp'] >= today_dt - datetime.timedelta(days=365)]
            case PeriodFilterMode.TOTAL:
                return df.copy(deep=True)

    def preprocess_input(self, args):
        mode = self.parse_mode(args)
        chat_df = self.filter_df(self.chat_df, mode)
        reactions_df = self.filter_df(self.reactions_df, mode)
        return chat_df, reactions_df, mode

    def parse_int(self, num_str):
        try:
            return int(num_str)
        except ValueError:
            log.error(f"{num_str} is not a number. {e}")
            return None
