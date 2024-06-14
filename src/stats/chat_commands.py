import os.path
import logging
import datetime

from telegram import Update
from telegram.ext import ContextTypes
import telegram
from zoneinfo import ZoneInfo
import pandas as pd

from definitions import USERS_PATH, CLEANED_CHAT_HISTORY_PATH, REACTIONS_PATH, PeriodFilterMode, CHAT_IMAGES_DIR_PATH, UPDATE_REQUIRED_PATH, EmojiType
import src.stats.utils as stats_utils

pd.options.mode.chained_assignment = None
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', 1000)
log = logging.getLogger(__name__)

negative_emojis = ['👎', '😢', '😭', '🤬', '🤡', '💩', '😫', '😩', '🥶', '🤨', '🧐', '🙃', '😒', '😠', '😣', '🗿']
MAX_INT = 24 * 365 * 20


class ChatCommands:
    def __init__(self):
        self.users_df = stats_utils.read_df(USERS_PATH)
        self.chat_df = stats_utils.read_df(CLEANED_CHAT_HISTORY_PATH)
        self.reactions_df = stats_utils.read_df(REACTIONS_PATH)

    def update(self):
        """If chat data was updated recentely, reload it."""
        if not os.path.isfile(UPDATE_REQUIRED_PATH):
            log.info(f"Update not required, {UPDATE_REQUIRED_PATH} doesn't exist.")
            return

        log.info('Reloading chat data due to the recent update.')
        self.chat_df = stats_utils.read_df(CLEANED_CHAT_HISTORY_PATH)
        self.reactions_df = stats_utils.read_df(REACTIONS_PATH)
        stats_utils.remove_file(UPDATE_REQUIRED_PATH)

    async def summary(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        chat_df, reactions_df, period_mode, mode_time, user, error = self.preprocess_input(context.args)

        sad_reactions_df = self.filter_emoji_by_emoji_type(reactions_df, EmojiType.NEGATIVE, 'emoji')
        text_only_chat_df = chat_df[chat_df['text'] != '']

        images_num = len(chat_df[chat_df['photo']])
        reactions_received_counts = reactions_df.groupby('reacted_to_username').size().reset_index(name='count').sort_values('count', ascending=False)
        reactions_given_counts = reactions_df.groupby('reacting_username').size().reset_index(name='count').sort_values('count', ascending=False)
        sad_reactions_received_counts = sad_reactions_df.groupby('reacted_to_username').size().reset_index(name='count').sort_values('count', ascending=False)
        sad_reactions_given_counts = sad_reactions_df.groupby('reacting_username').size().reset_index(name='count').sort_values('count', ascending=False)

        message_counts = chat_df.groupby('final_username').size().reset_index(name='count').sort_values('count', ascending=False)

        text = "*Chat summary*"
        text += f"({period_mode.value}):" if mode_time == -1 else f" (past {mode_time}h):"
        text += f"\n- *Total*: *{len(chat_df)}* messages, *{len(reactions_df)}* reactions and *{images_num}* images"
        text += "\n- *Top spammer*: " + ", ".join([f"{row['final_username']}: *{row['count']}*" for _, row in message_counts.head(3).iterrows()])
        text += "\n- *Most liked*: " + ", ".join([f"{row['reacted_to_username']}: *{row['count']}*" for _, row in reactions_received_counts.head(3).iterrows()])
        text += "\n- *Most liking*: " + ", ".join([f"{row['reacting_username']}: *{row['count']}*" for _, row in reactions_given_counts.head(3).iterrows()])
        text += "\n- *Most disliked*: " + ", ".join([f"{row['reacted_to_username']}: *{row['count']}*" for _, row in sad_reactions_received_counts.head(3).iterrows()])
        text += "\n- *Most disliking*: " + ", ".join([f"{row['reacting_username']}: *{row['count']}*" for _, row in sad_reactions_given_counts.head(3).iterrows()])
        text += "\n- *Top message*: " + ", ".join([f"{row['final_username']} [{self.dt_to_str(row['timestamp'])}]: {row['text']} [{''.join(row['reaction_emojis'])}]" for _, row in text_only_chat_df.head(1).iterrows()])


        text = stats_utils.escape_special_characters(text)
        await context.bot.send_message(chat_id=update.effective_chat.id, text=text, parse_mode=telegram.constants.ParseMode.MARKDOWN_V2)

    async def messages_by_reactions(self, update: Update, context: ContextTypes.DEFAULT_TYPE, emoji_type: EmojiType = EmojiType.ALL):
        """Top or worst 5 messages from selected time period by number of reactions"""
        chat_df, reactions_df, period_mode, mode_time, user, error = self.preprocess_input(context.args, emoji_type)
        if error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=error)
            return

        chat_df = chat_df[chat_df['text'] != '']
        label = self.emoji_sentiment_to_label(emoji_type)

        text = f"{label} Cinco messages"
        text += f" by {user}" if user is not None else " "
        text += f"({period_mode.value}):" if mode_time == -1 else f" (past {mode_time}h):"

        for i, (index, row) in enumerate(chat_df.head(5).iterrows()):
            if row['reactions_num'] == 0:
                break
            text += f"\n{i + 1}. {row['final_username']}" if user is None else f"\n{i + 1}."
            text += f" [{self.dt_to_str(row['timestamp'])}]:"
            text += f" {row['text']} [{''.join(row['reaction_emojis'])}]"

        await context.bot.send_message(chat_id=update.effective_chat.id, text=text)

    async def memes_by_reactions(self, update: Update, context: ContextTypes.DEFAULT_TYPE, emoji_type: EmojiType = EmojiType.ALL):
        """Top or sad 5 memes (images) from selected time period by number of reactions"""
        chat_df, reactions_df, period_mode, mode_time, user, error = self.preprocess_input(context.args, emoji_type)
        if error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=error)
            return

        label = self.emoji_sentiment_to_label(emoji_type)
        text = f"{label} Cinco memes"
        text += ' ' if user is None else f" by {user}"
        text += f"({period_mode.value}):" if mode_time == -1 else f" (past {mode_time}h):"

        await context.bot.send_message(chat_id=update.effective_chat.id, text=text)

        chat_df = chat_df[chat_df['photo'] == True]

        for i, (index, row) in enumerate(chat_df.head(5).iterrows()):
            if row['reactions_num'] == 0:
                break
            img_path = os.path.join(CHAT_IMAGES_DIR_PATH, f'{str(row['message_id'])}.jpg')
            text = f"\n{i + 1}. {row['final_username']}" if user is None else f"\n{i + 1}."
            text += f" [{self.dt_to_str(row['timestamp'])}]:"
            text += f" {row['text']} [{''.join(row['reaction_emojis'])}]"

            # text = f"\n{i + 1}. {row['final_username']}: {row['text']} [{''.join(row['reaction_emojis'])}]"
            await context.bot.send_photo(chat_id=update.effective_chat.id, photo=img_path, caption=text)

    def parse_args(self, args: list[str]) -> (PeriodFilterMode, str, str):
        period_mode = PeriodFilterMode.TOTAL
        mode_time = -1
        parse_error, mode_error = '', ''

        try:
            period_mode_str = args[0] if args else 'total'
            if args and 'h' in args[0] and self.has_numbers(args[0]):
                mode_time, parse_error = self.parse_int(args[0].replace('h', ''))
                period_mode_str = 'hour'

            period_mode = PeriodFilterMode(period_mode_str)
        except ValueError:
            mode_error = f"There is no such time period as {args[0]}."
            log.error(mode_error)

        user, user_error = self.parse_user(args)
        error = mode_error + user_error + parse_error

        return period_mode, mode_time, user, error

    def parse_user(self, args: list[str]) -> (str, str):
        if len(args) <= 1:
            return None, ''

        user = None
        error = ''
        user_str = ' '.join(args[1:]).replace('@', '')

        exact_matching_users = self.users_df[self.users_df['final_username'].str.lower() == user_str.lower()]
        partially_matching_users = self.users_df[self.users_df['final_username'].str.contains(user_str, case=False)]

        if not exact_matching_users.empty:
            user = exact_matching_users.iloc[0]['final_username']
        elif len(args[1]) >= 3 and not partially_matching_users.empty:
            user = partially_matching_users.iloc[0]['final_username']
        else:
            error = f"User {args[1]} doesn't exist and cannot hurt you. Existing users are: {self.users_df['final_username'].tolist()}"
            log.error(error)
        return user, error

    def get_today_midnight_dt(self):
        return datetime.datetime.now().replace(tzinfo=ZoneInfo('Europe/Warsaw')).replace(hour=0, minute=0, second=0, microsecond=0)

    def get_past_hr_dt(self, hours):
        return datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=hours)

    def filter_by_time_df(self, df, period_mode, mode_time):
        today_dt = self.get_today_midnight_dt()
        log.info(f"Filter by period_mode: {period_mode}, mode_time: {mode_time}, midnight today: {today_dt}")
        match period_mode:
            case PeriodFilterMode.HOUR:
                log.info(f"UTC dt {mode_time} hours ago: {self.get_past_hr_dt(mode_time)}")
                return df[df['timestamp'] >= self.get_past_hr_dt(mode_time)]
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

    def preprocess_input(self, args, emoji_type: EmojiType = EmojiType.ALL):
        self.update()

        mode, mode_time, user, error = self.parse_args(args)
        if error != '':
            return self.chat_df, self.reactions_df, mode, mode_time, user, error

        filtered_chat_df = self.filter_by_time_df(self.chat_df, mode, mode_time)
        filtered_reactions_df = self.filter_by_time_df(self.reactions_df, mode, mode_time)

        filtered_chat_df = self.filter_emojis_by_emoji_type(filtered_chat_df, emoji_type, 'reaction_emojis')

        filtered_chat_df['reactions_num'] = filtered_chat_df['reaction_emojis'].apply(lambda x: len(x))
        filtered_chat_df = filtered_chat_df.sort_values(['reactions_num', 'timestamp'], ascending=[False, True])
        filtered_chat_df['timestamp'] = filtered_chat_df['timestamp'].dt.tz_convert('Europe/Warsaw')

        if user is not None:
            filtered_chat_df = filtered_chat_df[filtered_chat_df['final_username'] == user]

        log.info('Last message:')
        print(filtered_chat_df.tail(1))

        return filtered_chat_df, filtered_reactions_df, mode, mode_time, user, error

    def filter_emojis_by_emoji_type(self, df, emoji_type, col='reaction_emojis'):
        if emoji_type == EmojiType.NEGATIVE:
            df[col] = df[col].apply(lambda emojis: [emoji for emoji in emojis if emoji in negative_emojis])
        return df

    def filter_emoji_by_emoji_type(self, df, emoji_type, col='emoji'):
        if emoji_type == EmojiType.NEGATIVE:
            df = df[df[col].isin(negative_emojis)]
            # df = df[df[col] is not None]
        return df

    def extract_int(self, num_str):
        return self.parse_int(''.join(filter(str.isdigit, num_str)))

    def has_numbers(self, num_str):
        return any(char.isdigit() for char in num_str)

    def parse_int(self, num_str):
        error = ''
        num = None
        try:
            num = int(num_str)
            print(num, MAX_INT)
            if num > MAX_INT:
                error = f"Kuba's dick is too big ({self.x_to_light_years(num)} light years), make it smaller!"
                log.error(error)
        except ValueError:
            error = f"{num_str} is not a number."
            log.error(error)
        return num, error

    def emoji_sentiment_to_label(self, emoji_type: EmojiType):
        """Convert emoji_type to a message label."""
        match emoji_type:
            case EmojiType.ALL:
                return 'Top'
            case EmojiType.NEGATIVE:
                return 'Top Sad'

    def dt_to_str(self, dt):
        return dt.strftime('%d-%m-%Y %H:%M')

    def x_to_light_years(self, x):
        ly = x / 9460730472580.8
        ly = round(ly, 6) if ly < 1 else round(ly, 2)
        return ly
