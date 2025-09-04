import os.path
import logging

from matplotlib import pyplot as plt
from telegram import Update
from telegram.ext import ContextTypes
import telegram
import pandas as pd

from definitions import USERS_PATH, CLEANED_CHAT_HISTORY_PATH, REACTIONS_PATH, UPDATE_REQUIRED_PATH, EmojiType, ArgType, MessageType, MAX_USERNAME_LENGTH, TEMP_DIR, TIMEZONE, PeriodFilterMode, \
    ChartType, MAX_CWEL_USAGE_DAILY, CHAT_VIDEO_NOTES_DIR_PATH
import src.stats.utils as stats_utils
import src.core.utils as core_utils
from src.core.client_api_handler import BOT_ID
from src.core.command_logger import CommandLogger
from src.core.job_persistance import JobPersistance
from src.models.bot_state import BotState
from src.models.command_args import CommandArgs
import src.stats.charts
from src.models.youtube_download import YoutubeDownload
from src.stats import charts
from src.stats.word_stats import WordStats

pd.options.mode.chained_assignment = None
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', 1000)
log = logging.getLogger(__name__)

negative_emojis = ['👎', '😢', '😭', '🤬', '🤡', '💩', '😫', '😩', '🥶', '🤨', '🧐', '🙃', '😒', '😠', '😣', '🗿']
MAX_INT = 24 * 365 * 20
MAX_NICKNAMES_NUM = 5


class ChatCommands:
    def __init__(self, command_logger: CommandLogger, job_persistance: JobPersistance, bot_state: BotState):
        self.users_df = stats_utils.read_df(USERS_PATH)
        self.users_map = stats_utils.get_users_map(self.users_df)
        self.chat_df = stats_utils.read_df(CLEANED_CHAT_HISTORY_PATH)
        self.reactions_df = stats_utils.read_df(REACTIONS_PATH)
        self.command_logger = command_logger
        self.bot_state = bot_state
        self.job_persistance = job_persistance
        self.cwel_stats_df = stats_utils.init_cwel_stats()
        self.word_stats = WordStats()
        self.ytdl = YoutubeDownload()

    def update(self):
        """If chat data was updated recentely, reload it."""
        if not os.path.isfile(UPDATE_REQUIRED_PATH):
            log.info(f"Update not required, {UPDATE_REQUIRED_PATH} doesn't exist.")
            return

        log.info('Reloading chat data due to the recent update.')
        self.chat_df = stats_utils.read_df(CLEANED_CHAT_HISTORY_PATH)
        self.reactions_df = stats_utils.read_df(REACTIONS_PATH)
        self.users_df = stats_utils.read_df(USERS_PATH)
        self.word_stats = WordStats()

        stats_utils.remove_file(UPDATE_REQUIRED_PATH)

    def preprocess_input(self, command_args, emoji_type: EmojiType = EmojiType.ALL):
        self.update()

        command_args = core_utils.parse_args(self.users_df, command_args)
        if command_args.error != '':
            return self.chat_df, self.reactions_df, command_args

        filtered_chat_df = stats_utils.filter_by_time_df(self.chat_df, command_args)
        filtered_reactions_df = stats_utils.filter_by_time_df(self.reactions_df, command_args)

        filtered_chat_df = stats_utils.filter_emojis_by_emoji_type(filtered_chat_df, emoji_type, 'reaction_emojis')

        filtered_chat_df['reactions_num'] = filtered_chat_df['reaction_emojis'].apply(lambda x: len(x))
        filtered_chat_df = filtered_chat_df.sort_values(['reactions_num', 'timestamp'], ascending=[False, True])
        filtered_chat_df['timestamp'] = filtered_chat_df['timestamp'].dt.tz_convert(TIMEZONE)

        if command_args.user is not None:
            filtered_chat_df = filtered_chat_df[filtered_chat_df['final_username'] == command_args.user]

        log.info('Last message:')
        print(filtered_chat_df.tail(1))

        return filtered_chat_df, filtered_reactions_df, command_args

    async def cmd_summary(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(args=context.args, expected_args=[ArgType.USER, ArgType.PERIOD], optional=[True, True], available_named_args={'num': ArgType.POSITIVE_INT})
        chat_df, reactions_df, command_args = self.preprocess_input(command_args, EmojiType.ALL)
        display_count = command_args.named_args['num'] if 'num' in command_args.named_args else 3

        shifted_chat_df = stats_utils.filter_by_shifted_time_df(self.chat_df, command_args)
        shifted_reactions_df = stats_utils.filter_by_shifted_time_df(self.reactions_df, command_args)
        if command_args.error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error, message_thread_id=update.message.message_thread_id)
            return

        sad_reactions_df = stats_utils.filter_emoji_by_emoji_type(reactions_df, EmojiType.NEGATIVE, 'emoji')
        text_only_chat_df = chat_df[chat_df['text'] != '']

        self.calculate_monologue_index_metric_periodized(chat_df)

        # Calculate message and reaction count
        images_num = len(chat_df[chat_df['message_type'] == 'image'])
        reactions_received_counts = reactions_df.groupby('reacted_to_username').size().reset_index(name='count').sort_values('count', ascending=False)
        reactions_given_counts = reactions_df.groupby('reacting_username').size().reset_index(name='count').sort_values('count', ascending=False)
        sad_reactions_received_counts = sad_reactions_df.groupby('reacted_to_username').size().reset_index(name='count').sort_values('count', ascending=False)
        sad_reactions_given_counts = sad_reactions_df.groupby('reacting_username').size().reset_index(name='count').sort_values('count', ascending=False)

        chat_df['word_count'] = chat_df['text'].apply(lambda x: len(str(x).split()))
        chat_df['word_length'] = chat_df['text'].apply(lambda x: stats_utils.text_to_word_length_sum(x))

        user_stats = chat_df.groupby('final_username').agg(
            word_count=('word_count', 'sum'),
            word_length=('word_length', 'sum'),
            message_count=('text', 'size')
        ).reset_index()

        # Ratios
        fun_metric = self.calculate_fun_metric(chat_df, reactions_df)
        wholesome_metric = self.calculate_wholesome_metric(reactions_df)
        user_stats['monologue_ratio'] = (user_stats['word_count'] / user_stats['message_count']).round(2)
        user_stats['avg_word_length'] = (user_stats['word_length'] / user_stats['word_count']).round(2)

        # Calculate message and reaction count changes
        message_count_change = 0 if shifted_chat_df.empty else round((len(chat_df) - len(shifted_chat_df)) / len(shifted_chat_df) * 100, 1)
        reaction_count_change = 0 if shifted_reactions_df.empty else round((len(reactions_df) - len(shifted_reactions_df)) / len(shifted_reactions_df) * 100, 1)
        message_count_change_text = f'+{message_count_change}%' if message_count_change > 0 else f'{message_count_change}%'
        reaction_count_change_text = f'+{reaction_count_change}%' if reaction_count_change > 0 else f'{reaction_count_change}%'

        # Create summary
        text = "*Chat summary*"
        text += f"({command_args.period_mode.value}):" if command_args.period_time == -1 else f" (past {command_args.period_time}h):"
        # text += f"\n- *Total*: *{len(chat_df)} ({message_count_change_text})* messages, *{len(reactions_df)} ({reaction_count_change_text})* reactions and *{images_num}* images"
        # text += "\n- *Top spammer*: " + ", ".join([f"{row['final_username']}: *{row['message_count']}*" for _, row in user_stats.sort_values('message_count', ascending=False).head(3).iterrows()])
        # text += "\n- *Word count*: " + ", ".join([f"{row['final_username']}: *{row['word_count']}*" for _, row in user_stats.sort_values('word_count', ascending=False).head(3).iterrows()])
        # text += "\n- *Monologue index*: " + ", ".join([f"{row['final_username']}: *{row['monologue_ratio']}*" for _, row in user_stats.sort_values('monologue_ratio', ascending=False).head(3).iterrows()])
        # text += "\n- *Fun meter*: " + ", ".join([f"{row['final_username']}: *{row['ratio']}*" for _, row in fun_metric.head(3).iterrows()])
        # text += "\n- *Wholesome meter*: " + ", ".join([f"{row['reacting_username']}: *{row['ratio']}*" for _, row in wholesome_metric.head(3).iterrows()])
        # text += "\n- *Unwholesome meter*: " + ", ".join([f"{row['reacting_username']}: *{row['ratio']}*" for _, row in wholesome_metric.sort_values('ratio', ascending=True).head(3).iterrows()])
        # text += "\n- *Most liked*: " + ", ".join([f"{row['reacted_to_username']}: *{row['count']}*" for _, row in reactions_received_counts.head(3).iterrows()])
        # text += "\n- *Most liking*: " + ", ".join([f"{row['reacting_username']}: *{row['count']}*" for _, row in reactions_given_counts.head(3).iterrows()])
        # text += "\n- *Most disliked*: " + ", ".join([f"{row['reacted_to_username']}: *{row['count']}*" for _, row in sad_reactions_received_counts.head(3).iterrows()])
        # text += "\n- *Most disliking*: " + ", ".join([f"{row['reacting_username']}: *{row['count']}*" for _, row in sad_reactions_given_counts.head(3).iterrows()])
        # text += "\n- *Top message*: " + ", ".join( [f"{row['final_username']} [{stats_utils.dt_to_str(row['timestamp'])}]: {row['text']} [{''.join(row['reaction_emojis'])}]" for _, row in text_only_chat_df.head(1).iterrows()])
        #
        # text = stats_utils.escape_special_characters(text)
        # await context.bot.send_message(chat_id=update.effective_chat.id, text=text, parse_mode=telegram.constants.ParseMode.MARKDOWN_V2, message_thread_id=update.message.message_thread_id)

        rows = [
            ['<b>Top spammer</b>', *[f"{row['final_username']}: <b>{row['message_count']}</b>" for _, row in user_stats.sort_values('message_count', ascending=False).head(display_count).iterrows()]],
            ['<b>Word count</b>', *[f"{row['final_username']}: <b>{row['word_count']}</b>" for _, row in user_stats.sort_values('word_count', ascending=False).head(display_count).iterrows()]],
            ['<b>Monologue index</b>',
             *[f"{row['final_username']}: <b>{row['monologue_ratio']}</b>" for _, row in user_stats.sort_values('monologue_ratio', ascending=False).head(display_count).iterrows()]],
            ['<b>Elaborateness</b>',
             *[f"{row['final_username']}: <b>{row['avg_word_length']}</b>" for _, row in user_stats.sort_values('avg_word_length', ascending=False).head(display_count).iterrows()]],
            ['<b>Fun</b>', *[f"{row['final_username']}: <b>{row['ratio']}</b>" for _, row in fun_metric.head(display_count).iterrows()]],
            ['<b>Wholesome</b>', *[f"{row['reacting_username']}: <b>{row['ratio']}</b>" for _, row in wholesome_metric.head(display_count).iterrows()]],
            ['<b>Unwholesome</b>', *[f"{row['reacting_username']}: <b>{row['ratio']}</b>" for _, row in wholesome_metric.sort_values('ratio', ascending=True).head(display_count).iterrows()]],
            ['<b>Most liked</b>', *[f"{row['reacted_to_username']}: <b>{row['count']}</b>" for _, row in reactions_received_counts.head(display_count).iterrows()]],
            ['<b>Most liking</b>', *[f"{row['reacting_username']}: <b>{row['count']}</b>" for _, row in reactions_given_counts.head(display_count).iterrows()]],
            ['<b>Most disliked</b>', *[f"{row['reacted_to_username']}: <b>{row['count']}</b>" for _, row in sad_reactions_received_counts.head(display_count).iterrows()]],
            ['<b>Most disliking</b>', *[f"{row['reacting_username']}: <b>{row['count']}</b>" for _, row in sad_reactions_given_counts.head(display_count).iterrows()]],
        ]
        footnotes = [
            f"Total: {len(chat_df)} ({message_count_change_text}) messages, {len(reactions_df)} ({reaction_count_change_text}) reactions and {images_num} images",
            "Top message: " + ", ".join(
                [f"{row['final_username']} [{stats_utils.dt_to_str(row['timestamp'])}]: {row['text']} [{''.join(row['reaction_emojis'])}]" for _, row in text_only_chat_df.head(1).iterrows()])
        ]
        send_msg = '\n'.join(footnotes)

        # Adjust the col display count to the longest row
        longest_row_count = max(len(row) for row in rows) - 1  # -1 because 1st column is a header for metric name
        col_count = min(longest_row_count, display_count)
        columns = [f'<b>{core_utils.generate_period_headline(command_args)}</b>', *[f"<b>TOP{i + 1}</b>" for i in range(col_count)]]

        summary_df = pd.DataFrame(rows, columns=columns)
        path = charts.create_table_plotly(summary_df, command_args=command_args, columns=columns)

        current_message_type = MessageType.IMAGE
        await self.send_message(update, context, current_message_type, path, text=send_msg)

    async def cmd_messages_by_reactions(self, update: Update, context: ContextTypes.DEFAULT_TYPE, emoji_type: EmojiType = EmojiType.ALL):
        """Top or worst 5 messages from selected time period by number of reactions"""
        command_args = CommandArgs(args=context.args, expected_args=[ArgType.USER, ArgType.PERIOD], optional=[True, True], available_named_args={'text': ArgType.STRING}, max_string_length=50)
        chat_df, reactions_df, command_args = self.preprocess_input(command_args, emoji_type)
        if command_args.error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error, message_thread_id=update.message.message_thread_id)
            return

        chat_df = chat_df[(chat_df['text'] != '') & (chat_df['text'].notna())]

        if 'text' in command_args.named_args:
            filter_phrase = command_args.named_args['text'].lower()
            chat_df = chat_df[chat_df['text'].str.lower().str.contains(filter_phrase)]

        label = stats_utils.emoji_sentiment_to_label(emoji_type)
        text = self.generate_response_headline(command_args, label=f"{label} Cinco messages")

        for i, (index, row) in enumerate(chat_df.head(5).iterrows()):
            if row['reactions_num'] == 0:
                break
            text += f"\n{i + 1}. {row['final_username']}" if command_args.user is None else f"\n{i + 1}."
            text += f" [{stats_utils.dt_to_str(row['timestamp'])}]:"
            text += f" {row['text']} [{''.join(row['reaction_emojis'])}]"

        await context.bot.send_message(chat_id=update.effective_chat.id, text=text)

    async def cmd_media_by_reactions(self, update: Update, context: ContextTypes.DEFAULT_TYPE, message_type: MessageType, emoji_type: EmojiType = EmojiType.ALL):
        """Top or sad 5 media (images, videos, video notes, audio, gifs) from selected time period by number of reactions. Videos and video notes are merged into one."""
        command_args = CommandArgs(args=context.args, expected_args=[ArgType.USER, ArgType.PERIOD], optional=[True, True], available_named_args={'text': ArgType.STRING}, max_string_length=50)
        chat_df, reactions_df, command_args = self.preprocess_input(command_args, emoji_type)
        if command_args.error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error, message_thread_id=update.message.message_thread_id)
            return

        label = stats_utils.emoji_sentiment_to_label(emoji_type)
        text = self.generate_response_headline(command_args, label=f"{label} Cinco {message_type.value}")

        await context.bot.send_message(chat_id=update.effective_chat.id, text=text)

        if message_type == MessageType.VIDEO:
            chat_df = chat_df[chat_df['message_type'].isin([MessageType.VIDEO.value, MessageType.VIDEO_NOTE.value])]
        else:
            chat_df = chat_df[chat_df['message_type'] == message_type.value]

        if 'text' in command_args.named_args and message_type == MessageType.IMAGE:
            filter_text_lower = command_args.named_args['text'].lower()
            chat_df = chat_df[chat_df['image_text'].str.lower().str.contains(filter_text_lower)]

        chat_df = chat_df.sort_values(['reactions_num', 'timestamp'], ascending=[False, True])

        for i, (index, row) in enumerate(chat_df.head(5).iterrows()):
            text = f"\n{i + 1}. {row['final_username']}" if command_args.user is None else f"\n{i + 1}."
            text += f" [{stats_utils.dt_to_str(row['timestamp'])}]:"
            text += f" {row['text']} [{''.join(row['reaction_emojis'])}]"

            current_message_type = MessageType(row['message_type'])
            path = core_utils.message_id_to_path(str(row['message_id']), current_message_type)
            await self.send_message(update, context, current_message_type, path, text)

    async def send_message(self, update, context, message_type: MessageType, path, text):
        log.info(f'Sending message: {text} with media type: {message_type} and media path: {path}')
        match message_type:
            case MessageType.GIF:
                await context.bot.send_animation(chat_id=update.effective_chat.id, animation=path, caption=text, message_thread_id=update.message.message_thread_id)
            case MessageType.VIDEO:
                await context.bot.send_video(chat_id=update.effective_chat.id, video=path, caption=text, message_thread_id=update.message.message_thread_id)
            case MessageType.VIDEO_NOTE:
                await context.bot.send_video_note(chat_id=update.effective_chat.id, video_note=path, message_thread_id=update.message.message_thread_id)
                if text != '':
                    await context.bot.send_message(chat_id=update.effective_chat.id, text=text, message_thread_id=update.message.message_thread_id)
            case MessageType.IMAGE:
                await context.bot.send_photo(chat_id=update.effective_chat.id, photo=path, caption=text, message_thread_id=update.message.message_thread_id)
            case MessageType.AUDIO:
                await context.bot.send_audio(chat_id=update.effective_chat.id, audio=path, caption=text, message_thread_id=update.message.message_thread_id)
            case MessageType.VOICE:
                await context.bot.send_voice(chat_id=update.effective_chat.id, voice=path, caption=text, message_thread_id=update.message.message_thread_id)

    async def cmd_last_messages(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Display last n messages from chat history"""
        command_args = CommandArgs(args=context.args, expected_args=[ArgType.USER, ArgType.POSITIVE_INT], max_number=100)
        chat_df, reactions_df, command_args = self.preprocess_input(command_args, EmojiType.ALL)
        chat_df = chat_df.sort_values(by='timestamp', ascending=False)
        if command_args.error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error, message_thread_id=update.message.message_thread_id)
            return

        text = f"Last {command_args.number} messages"
        text += f" by {command_args.user}" if command_args.user is not None else ":"

        for i, (index, row) in enumerate(chat_df.head(command_args.number).iterrows()):
            text += f"\n{i + 1}. {row['final_username']}" if command_args.user is None else f"\n{i + 1}."
            text += f" [{stats_utils.dt_to_str(row['timestamp'])}]:"
            text += f" {row['text']} [{''.join(row['reaction_emojis'])}]"

        if len(text) > 4096:
            text = "Too much text to display. Lower the number of messages."

        await context.bot.send_message(chat_id=update.effective_chat.id, text=text, message_thread_id=update.message.message_thread_id)

    async def cmd_display_users(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Display all users in chat"""

        text = "All ye who dost partake in this discourse:"
        for index, row in self.users_df.iterrows():
            nicknames = ', '.join(row['nicknames'])
            text += f"\n- *{row['final_username']}*: [{nicknames}]"

        text = stats_utils.escape_special_characters(text)
        await context.bot.send_message(chat_id=update.effective_chat.id, text=text, parse_mode=telegram.constants.ParseMode.MARKDOWN_V2, message_thread_id=update.message.message_thread_id)

    async def cmd_add_nickname(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Set username for all users in chat"""
        command_args = CommandArgs(args=context.args, expected_args=[ArgType.STRING], is_text_arg=True, min_string_length=3, max_string_length=20, label='Nickname')
        command_args = core_utils.parse_args(self.users_df, command_args)
        if command_args.error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error, message_thread_id=update.message.message_thread_id)
            return

        user_id = update.effective_user.id
        current_nicknames = self.users_df.at[user_id, 'nicknames']
        current_username = self.users_df.at[user_id, 'final_username']
        new_nickname = command_args.string

        if len(current_nicknames) >= MAX_NICKNAMES_NUM:
            error = f'Nickname *{new_nickname}* not added for *{current_username}*. Nicknames limit is {MAX_NICKNAMES_NUM}.'
            error = stats_utils.escape_special_characters(error)
            await context.bot.send_message(chat_id=update.effective_chat.id, text=error, message_thread_id=update.message.message_thread_id, parse_mode=telegram.constants.ParseMode.MARKDOWN_V2)
            return

        self.users_df.at[user_id, 'nicknames'] = [new_nickname] if len(current_nicknames) == 0 else current_nicknames + [new_nickname]
        core_utils.save_df(self.users_df, USERS_PATH)

        current_nicknames = self.users_df.at[user_id, 'nicknames']
        text = f'Nickname *{new_nickname}* added for *{current_username}*. Resulting in the following nicknames: *{", ".join(current_nicknames)}*. It will get updated in a few minutes.'
        text = stats_utils.escape_special_characters(text)
        await context.bot.send_message(chat_id=update.effective_chat.id, text=text, parse_mode=telegram.constants.ParseMode.MARKDOWN_V2, message_thread_id=update.message.message_thread_id)

    async def cmd_set_username(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Set username for all users in chat"""
        command_args = CommandArgs(args=context.args, expected_args=[ArgType.STRING], is_text_arg=True, min_string_length=3, max_string_length=MAX_USERNAME_LENGTH, label='Username')
        command_args = core_utils.parse_args(self.users_df, command_args)
        if command_args.error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error, message_thread_id=update.message.message_thread_id)
            return

        text = "Due to missuse of *set_username* command through various means, it has been *indefinitely disabled*, until decided otherwise by the Ozjasz Team. You can unlock this feature after subscribing to Ozjasz premium, for *5$ monthly*. \n\nRegards, *Ozjasz Team*."
        escaped_text = stats_utils.escape_special_characters(text)
        await context.bot.send_message(chat_id=update.effective_chat.id, text=escaped_text, parse_mode=telegram.constants.ParseMode.MARKDOWN_V2, message_thread_id=update.message.message_thread_id)
        return

        # user_id = update.effective_user.id
        # current_username = self.users_df.at[user_id, 'final_username']
        # new_username = command_args.string
        # is_valid, error = stats_utils.check_new_username(self.users_df, new_username)
        #
        # if not is_valid:
        #     error = stats_utils.escape_special_characters(error)
        #     await context.bot.send_message(chat_id=update.effective_chat.id, text=error, message_thread_id=update.message.message_thread_id, parse_mode=telegram.constants.ParseMode.MARKDOWN_V2, message_thread_id=update.message.message_thread_id)
        #     return
        #
        # self.users_df.at[user_id, 'final_username'] = new_username
        # core_utils.save_df(self.users_df, USERS_PATH)
        # text = f'Username changed from: *{current_username}* to *{new_username}*. It will get updated in a few minutes.'
        # text = stats_utils.escape_special_characters(text)
        # await context.bot.send_message(chat_id=update.effective_chat.id, text=text, parse_mode=telegram.constants.ParseMode.MARKDOWN_V2, message_thread_id=update.message.message_thread_id)

    async def cmd_fun(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(args=context.args, expected_args=[ArgType.PERIOD])
        command_args = core_utils.parse_args(self.users_df, command_args)
        chat_df, reactions_df, command_args = self.preprocess_input(command_args, EmojiType.ALL)
        if command_args.error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error, message_thread_id=update.message.message_thread_id)
            return

        fun_ratios = self.calculate_fun_metric(chat_df, reactions_df)
        text = self.generate_response_headline(command_args, label='Funmeter')

        for i, (index, row) in enumerate(fun_ratios.iterrows()):
            text += f"\n{i + 1}.".ljust(4) + f" {row['final_username']}:".ljust(MAX_USERNAME_LENGTH + 5) + f"{row['ratio']}" if command_args.user is None else f"\n{i + 1}."

        text += "```"
        text = stats_utils.escape_special_characters(text)
        await context.bot.send_message(chat_id=update.effective_chat.id, text=text, parse_mode=telegram.constants.ParseMode.MARKDOWN_V2, message_thread_id=update.message.message_thread_id)

    async def cmd_wholesome(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(args=context.args, expected_args=[ArgType.PERIOD])
        command_args = core_utils.parse_args(self.users_df, command_args)
        chat_df, reactions_df, command_args = self.preprocess_input(command_args, EmojiType.ALL)
        if command_args.error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error, message_thread_id=update.message.message_thread_id)
            return

        wholesome_ratios = self.calculate_wholesome_metric(reactions_df)

        text = self.generate_response_headline(command_args, label='``` Wholesome meter')

        for i, (index, row) in enumerate(wholesome_ratios.iterrows()):
            text += f"\n{i + 1}.".ljust(4) + f" {row['reacting_username']}:".ljust(MAX_USERNAME_LENGTH + 5) + f"{row['ratio']}" if command_args.user is None else f"\n{i + 1}."

        text += "```"
        text = stats_utils.escape_special_characters(text)
        await context.bot.send_message(chat_id=update.effective_chat.id, text=text, parse_mode=telegram.constants.ParseMode.MARKDOWN_V2, message_thread_id=update.message.message_thread_id)

    async def cmd_funchart(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(args=context.args, expected_args=[ArgType.USER, ArgType.PERIOD], optional=[True, True], available_named_args={'acc': ArgType.NONE})
        chat_df, reactions_df, command_args = self.preprocess_input(command_args, EmojiType.ALL)
        if command_args.error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error, message_thread_id=update.message.message_thread_id)
            return

        text = self.generate_response_headline(command_args, label='Funmeter chart')

        users = [command_args.user]
        if command_args.user is None:
            users = self.users_df['final_username'].unique()

        fun_ratios = self.calculate_fun_metric_periodized(chat_df, reactions_df, frequency='D')
        path = charts.generate_plot(fun_ratios, users, 'final_username', 'period', 'ratio', text, x_label='time', y_label='funratio daily')

        current_message_type = MessageType.IMAGE
        await self.send_message(update, context, current_message_type, path, text)

    async def cmd_spamchart(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(args=context.args, expected_args=[ArgType.USER, ArgType.PERIOD], optional=[True, True])
        chat_df, reactions_df, command_args = self.preprocess_input(command_args, EmojiType.ALL)
        if command_args.error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error, message_thread_id=update.message.message_thread_id)
            return

        text = self.generate_response_headline(command_args, label='Spamchart')

        users = [command_args.user]
        if command_args.user is None:
            users = self.users_df['final_username'].unique()

        chat_df['period'] = chat_df['timestamp'].dt.to_period('D')
        message_counts = chat_df.groupby(['period', 'final_username']).size().unstack(fill_value=0).stack().reset_index(name='message_count')
        path = charts.generate_plot(message_counts, users, 'final_username', 'period', 'message_count', text, x_label='time', y_label='messages daily')

        current_message_type = MessageType.IMAGE
        await self.send_message(update, context, current_message_type, path, text)

    async def cmd_monologuechart(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(args=context.args, expected_args=[ArgType.USER, ArgType.PERIOD], optional=[True, True], available_named_args={'acc': ArgType.NONE})
        chat_df, reactions_df, command_args = self.preprocess_input(command_args, EmojiType.ALL)
        if command_args.error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error, message_thread_id=update.message.message_thread_id)
            return

        label = 'Monologue index chart accumulated' if 'acc' in command_args.named_args else 'Monologue index chart daily'
        text = self.generate_response_headline(command_args, label=label)

        users = [command_args.user]
        if command_args.user is None:
            users = self.users_df['final_username'].unique()
        metric_col = 'monologue_index_acc' if 'acc' in command_args.named_args else 'monologue_index_periodized'

        # First calculate the metrics and only then filter by time (accumulated metrics need the entire chat history)
        total_monologue_stats_df = self.calculate_monologue_index_metric_periodized(self.chat_df, frequency='D')
        filtered_monologue_stats_df = stats_utils.filter_by_time_df(total_monologue_stats_df, command_args, time_column='period')
        filtered_monologue_stats_df['period'] = filtered_monologue_stats_df['period'].dt.to_period('D')
        path = charts.generate_plot(filtered_monologue_stats_df, users, 'final_username', 'period', metric_col, text, x_label='time', y_label='monologue index', chart_type=ChartType.LINE)

        current_message_type = MessageType.IMAGE
        await self.send_message(update, context, current_message_type, path, text)

    async def cmd_likechart(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(args=context.args, expected_args=[ArgType.USER, ArgType.PERIOD], optional=[True, True])

        chat_df, reactions_df, command_args = self.preprocess_input(command_args, EmojiType.ALL)
        if command_args.error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error, message_thread_id=update.message.message_thread_id)
            return

        text = self.generate_response_headline(command_args, label='Likechart')

        users = [command_args.user]
        if command_args.user is None:
            users = self.users_df['final_username'].unique()

        reactions_df['period'] = reactions_df['timestamp'].dt.to_period('D')
        reaction_counts = reactions_df.groupby(['period', 'reacted_to_username']).size().unstack(fill_value=0).stack().reset_index(name='reaction_count')
        path = charts.generate_plot(reaction_counts, users, 'reacted_to_username', 'period', 'reaction_count', text, x_label='time', y_label='likes received daily')

        current_message_type = MessageType.IMAGE
        await self.send_message(update, context, current_message_type, path, text)

    async def cmd_command_usage(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(args=context.args, expected_args=[ArgType.USER, ArgType.PERIOD], optional=[True, True])
        command_args = core_utils.parse_args(self.users_df, command_args)

        if command_args.error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error, message_thread_id=update.message.message_thread_id)
            return

        command_usage_df = self.command_logger.preprocess_data(self.users_df, command_args)
        command_counts_df = command_usage_df.groupby('command_name').size().reset_index(name='count').sort_values('count', ascending=False)

        text = self.generate_response_headline(command_args, label='``` Command usage')
        for index, row in command_counts_df.iterrows():
            text += f"\n {row['command_name']}:".ljust(20) + f"{row['count']}"

        text += "```"
        text = stats_utils.escape_special_characters(text)
        await context.bot.send_message(chat_id=update.effective_chat.id, text=text, parse_mode=telegram.constants.ParseMode.MARKDOWN_V2, message_thread_id=update.message.message_thread_id)

    async def cmd_command_usage_chart(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(args=context.args, available_named_args={'user': ArgType.USER, 'period': ArgType.PERIOD, 'command': ArgType.STRING})
        command_args = core_utils.parse_args(self.users_df, command_args)

        command = command_args.named_args['command'] if 'command' in command_args.named_args else ''
        if command != '' and command not in self.command_logger.get_commands():
            command_args.error += f'Command "{command}" does not exist.'

        if command_args.error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error, message_thread_id=update.message.message_thread_id)
            return

        text = self.generate_response_headline(command_args, label='Command usage chart')
        command_usage_df = self.command_logger.preprocess_data(self.users_df, command_args)

        commands = command_usage_df['command_name'].unique() if command == '' else command
        users = self.users_df['final_username'].unique()
        command_usage_df['period'] = command_usage_df['timestamp'].dt.to_period('D')

        grouping_col = 'username' if command != '' else 'command_name'
        selected_for_grouping = users if command != '' else commands

        command_usage_counts = command_usage_df.groupby(['period', grouping_col]).size().unstack(fill_value=0).stack().reset_index(name='command_count')
        path = charts.generate_plot(command_usage_counts, selected_for_grouping, grouping_col, 'period', 'command_count', text, x_label='time', y_label='command usage daily')

        current_message_type = MessageType.IMAGE
        await self.send_message(update, context, current_message_type, path, text)

    async def cmd_relationship_graph(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(args=context.args, expected_args=[ArgType.USER, ArgType.PERIOD], optional=[True, True])

        chat_df, reactions_df, command_args = self.preprocess_input(command_args, EmojiType.ALL)
        if command_args.error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error, message_thread_id=update.message.message_thread_id)
            return

        if reactions_df.empty:
            await context.bot.send_message(chat_id=update.effective_chat.id, text='No data from that period, sorry :(', message_thread_id=update.message.message_thread_id)
            return

        text = self.generate_response_headline(command_args, label='Relationship Graph')
        path = charts.create_relationship_graph(reactions_df)
        current_message_type = MessageType.IMAGE
        await self.send_message(update, context, current_message_type, path, text)

    async def cmd_remind(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(args=context.args, expected_args=[ArgType.PERIOD, ArgType.USER, ArgType.TEXT_MULTISPACED], optional=[False, False, False], min_string_length=1,
                                   max_string_length=1000)
        command_args = core_utils.parse_args(self.users_df, command_args)
        if command_args.error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error, message_thread_id=update.message.message_thread_id)
            return

        dt, dt_error = core_utils.period_offset_to_dt(command_args)
        message_id, message_id_error = stats_utils.get_last_message_id_of_a_user(self.chat_df, command_args.user_id)
        error = dt_error + message_id_error
        if error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=error, message_thread_id=update.message.message_thread_id)
            return

        self.job_persistance.save_job(job_queue=context.job_queue, dt=dt, func=core_utils.send_response_message, args=[update.effective_chat.id, message_id, command_args.string])
        response = f"{command_args.user} is gonna get pinged at {core_utils.dt_to_pretty_str(dt)}."
        await context.bot.send_message(chat_id=update.effective_chat.id, text=response)

    async def cmd_cwel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(args=context.args, expected_args=[ArgType.POSITIVE_INT], optional=[True], max_number=MAX_CWEL_USAGE_DAILY)
        command_args = core_utils.parse_args(self.users_df, command_args)
        if command_args.error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error, message_thread_id=update.message.message_thread_id)
            return

        error = 'You have to reply to a message to cwel someone.' if not update.message.reply_to_message else ''
        error += 'You cannot cwel Ozjasz. Only Ozjasz can cwel you.' if update.message.reply_to_message and update.message.reply_to_message.from_user.id == BOT_ID else error
        if error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=error, message_thread_id=update.message.message_thread_id)
            return

        source_message = update.message.reply_to_message
        receiver_username = self.users_map[source_message.from_user.id]
        message_id = source_message.message_id
        giver_id = update.message.from_user.id
        giver_username = self.users_map[giver_id]
        if receiver_username == giver_username:
            await context.bot.send_message(chat_id=update.effective_chat.id, text='You cannot cwel yourself.')
            return
        # cwel_value = command_args.number if command_args.number is not None else 1
        # success, error = self.bot_state.update_cwel_usage_map(giver_id, cwel_value)
        # if not success:
        #     await context.bot.send_message(chat_id=update.effective_chat.id, text=error, message_thread_id=update.message.message_thread_id)
        #     return

        self.cwel_stats_df = stats_utils.append_cwel_stats(self.cwel_stats_df, source_message.date, receiver_username, giver_username, message_id, 1)
        cwel_count = self.cwel_stats_df[self.cwel_stats_df['receiver_username'] == receiver_username]['value'].sum()
        processed_cwel_stats_df = self.cwel_stats_df.groupby('receiver_username')['value'].sum().sort_values(ascending=False).reset_index()
        cwel_place = processed_cwel_stats_df[processed_cwel_stats_df['receiver_username'] == receiver_username].index[0] + 1
        cwels_left = self.bot_state.get_cwels_left(giver_id)

        # time_str = f"{cwel_value} times " if cwel_value > 1 else ""
        response = f"*{giver_username}* cwel'd *{receiver_username}*, now *{receiver_username}* is cwel *#{cwel_place}*, lvl *{cwel_count}*"
        response = stats_utils.escape_special_characters(response)
        await context.bot.send_message(chat_id=update.effective_chat.id, text=response, parse_mode=telegram.constants.ParseMode.MARKDOWN_V2, message_thread_id=update.message.message_thread_id)

    async def cmd_topcwel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(args=context.args, expected_args=[ArgType.PERIOD])
        command_args = core_utils.parse_args(self.users_df, command_args)
        if command_args.error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error, message_thread_id=update.message.message_thread_id)
            return

        filtered_df = stats_utils.filter_by_time_df(self.cwel_stats_df, command_args)
        processed_cwel_stats_df = filtered_df.groupby('receiver_username')['value'].sum().sort_values(ascending=False).reset_index()
        text = self.generate_response_headline(command_args, label='``` Top Cwel')

        for i, (index, row) in enumerate(processed_cwel_stats_df.iterrows()):
            text += f"\n{i + 1}.".ljust(4) + f" {row['receiver_username']}:".ljust(MAX_USERNAME_LENGTH + 5) + f"{row['value']}" if command_args.user is None else f"\n{i + 1}."

        text += "```"
        text = stats_utils.escape_special_characters(text)
        await context.bot.send_message(chat_id=update.effective_chat.id, text=text, parse_mode=telegram.constants.ParseMode.MARKDOWN_V2, message_thread_id=update.message.message_thread_id)

    async def cmd_wordstats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(args=context.args, expected_args=[ArgType.USER, ArgType.PERIOD], optional=[True, True], available_named_args={'ngram': ArgType.POSITIVE_INT, 'text': ArgType.TEXT},
                                   min_number=1, max_number=6, max_string_length=1000)
        command_args = core_utils.parse_args(self.users_df, command_args)
        filtered_ngrams_df = self.word_stats.filter_ngrams(command_args)

        if command_args.error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error, message_thread_id=update.message.message_thread_id)
            return
        if 'text' in command_args.named_args:  # for a specific phrase like "
            filter_phrase = command_args.named_args['text'].lower()
            filter_ngram = len(filter_phrase.split())
            if filter_ngram not in self.word_stats.ngram_range:
                await context.bot.send_message(chat_id=update.effective_chat.id,
                                               text=f'Text must be within the ngram range of {self.word_stats.ngram_range} and "{filter_phrase}" is {filter_ngram}-gram.', message_thread_id=update.message.message_thread_id)
                return
            ngram_df = filtered_ngrams_df[filter_ngram]
            filter_ngram_df = ngram_df[ngram_df['ngrams'].str.lower().str.fullmatch(filter_phrase)]
            ngram_counts_df = filter_ngram_df.groupby(['final_username', 'ngrams']).size().reset_index(name="counts").sort_values(by='counts', ascending=False)
            text = self.generate_response_headline(command_args, label=f'``` Word stats')
            max_len_ngram = core_utils.max_str_length_in_col(ngram_counts_df['final_username'])
            for i, (index, row) in enumerate(ngram_counts_df.iterrows()):
                text += f"\n{i + 1}.".ljust(4) + f" {row['final_username']}:".ljust(max_len_ngram + 5) + f"{row['counts']}"
        elif 'ngram' in command_args.named_args:  # for a specific ngram value like (3, 3) etc
            n = command_args.named_args['ngram']
            ngram_df = filtered_ngrams_df[command_args.named_args['ngram']]
            ngram_counts = self.word_stats.count_ngrams(ngram_df)[:10]
            text = self.generate_response_headline(command_args, label=f'``` Word stats {n}-gram')
            max_len_ngram = core_utils.max_str_length_in_col(ngram_counts.index)
            for i, (ngram_text, ngram_count) in enumerate(ngram_counts.items()):
                text += f"\n{i + 1}.".ljust(4) + f" {ngram_text}:".ljust(max_len_ngram + 5) + f"{ngram_count}"
        else:  # for all ngram values <1, 2.. 5>
            text = self.generate_response_headline(command_args, label='``` Word stats')
            top_counts, top_ngram_texts, ngram_nums = [], [], []
            for ngram_num, ngram_df in filtered_ngrams_df.items():
                counts_df = self.word_stats.count_ngrams(ngram_df)
                top_counts.append(counts_df.iloc[0])
                top_ngram_texts.append(counts_df.index[0])
                ngram_nums.append(ngram_num)

            max_len_ngram = max(len(ngram) for ngram in top_ngram_texts)
            for ngram_num, top_ngram_text, top_count in zip(ngram_nums, top_ngram_texts, top_counts):
                text += f"\n[{ngram_num}] - ".ljust(4) + f" {top_ngram_text}:".ljust(max_len_ngram + 5) + f"{top_count}"

        text += "```"
        text = stats_utils.escape_special_characters(text)
        await context.bot.send_message(chat_id=update.effective_chat.id, text=text, parse_mode=telegram.constants.ParseMode.MARKDOWN_V2, message_thread_id=update.message.message_thread_id)

    def generate_response_headline(self, command_args, label):
        text = label
        text += f' of "{command_args.string}"' if command_args.string != '' else ''
        text += f" for {command_args.user}" if command_args.user is not None else " "
        text += f" ({core_utils.generate_period_headline(command_args)}):"
        return text

    def calculate_fun_metric(self, chat_df, reactions_df):
        reactions_received_counts = reactions_df.groupby('reacted_to_username').size().reset_index(name='reaction_count')
        message_counts = chat_df.groupby('final_username').size().reset_index(name='message_count')
        message_counts = message_counts[message_counts['message_count'] > 0]

        merged_df = reactions_received_counts.merge(message_counts, left_on='reacted_to_username', right_on='final_username', how='inner').fillna(0)
        merged_df['ratio'] = (merged_df['reaction_count'] / merged_df['message_count']).round(2)
        fun_ratios = merged_df[['final_username', 'ratio']].copy().sort_values('ratio', ascending=False)

        return fun_ratios

    def calculate_monologue_index_metric_periodized(self, chat_df, frequency='D'):
        chat_df['period'] = chat_df['timestamp'].dt.to_period(frequency)
        chat_df = chat_df.sort_values('timestamp')

        chat_df['word_count'] = chat_df['text'].apply(lambda x: len(str(x).split()))
        user_stats = chat_df.groupby(['period', 'final_username']).agg(
            word_count=('word_count', 'sum'),
            message_count=('text', 'size')
        ).reset_index()
        user_stats['word_count_acc'] = user_stats.groupby('final_username')['word_count'].cumsum()
        user_stats['message_count_acc'] = user_stats.groupby('final_username')['message_count'].cumsum()

        user_stats['monologue_index_periodized'] = (user_stats['word_count'] / user_stats['message_count']).round(2)
        user_stats['monologue_index_acc'] = (user_stats['word_count_acc'] / user_stats['message_count_acc']).round(2)

        user_stats['period'] = user_stats['period'].dt.to_timestamp()
        user_stats['temp_period'] = user_stats['period']
        user_stats.index = user_stats['temp_period']
        user_stats['period'] = pd.to_datetime(user_stats['period']).dt.tz_localize(TIMEZONE)
        user_stats = user_stats.reset_index(drop=True)

        return user_stats

    def calculate_wholesome_metric(self, reactions_df):
        reactions_received_counts = reactions_df.groupby('reacted_to_username').size().reset_index(name='reactions_received_count')
        reactions_given_counts = reactions_df.groupby('reacting_username').size().reset_index(name='reactions_given_count')
        reactions_received_counts = reactions_received_counts[reactions_received_counts['reactions_received_count'] > 0]

        merged_df = reactions_received_counts.merge(reactions_given_counts, left_on='reacted_to_username', right_on='reacting_username', how='inner').fillna(0)
        merged_df['ratio'] = (merged_df['reactions_given_count'] / merged_df['reactions_received_count']).round(2)
        wholesome_ratios = merged_df[['reacting_username', 'ratio']].copy().sort_values('ratio', ascending=False)

        return wholesome_ratios

    def calculate_fun_metric_periodized(self, chat_df, reactions_df, frequency='D'):
        chat_df['period'] = chat_df['timestamp'].dt.to_period(frequency)
        reactions_df['period'] = reactions_df['timestamp'].dt.to_period(frequency)

        message_counts = chat_df.groupby(['period', 'final_username']).size().reset_index(name='message_count')
        reaction_counts = reactions_df.groupby(['period', 'reacted_to_username']).size().reset_index(name='reaction_count')

        merged_df = pd.merge(message_counts, reaction_counts,
                             left_on=['period', 'final_username'],
                             right_on=['period', 'reacted_to_username'],
                             how='inner').fillna(0)

        merged_df['ratio'] = (merged_df['reaction_count'] / merged_df['message_count']).round(2)
        result_df = merged_df[['period', 'final_username', 'ratio']].sort_values(['period', 'ratio'], ascending=[True, False])

        return result_df

    async def cmd_play(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(args=context.args, expected_args=[ArgType.STRING], available_named_args={'full': ArgType.NONE}, optional=[False], max_string_length=1000)
        command_args = core_utils.parse_args(self.users_df, command_args)
        if command_args.error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error, message_thread_id=update.message.message_thread_id)
            return

        audio_path, error = self.ytdl.download(command_args.string)
        if error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=error, message_thread_id=update.message.message_thread_id)
            return

        if 'full' in command_args.named_args:
            await self.send_message(update, context, MessageType.VOICE, audio_path, '')
            return

        reply_message_id = update.message.reply_to_message.message_id
        reply_message_type = self.get_reply_message_type(reply_message_id)
        if reply_message_type is not None and reply_message_type in [MessageType.VIDEO, MessageType.VIDEO_NOTE, MessageType.GIF]:
            video_path = core_utils.message_id_to_path(reply_message_id, reply_message_type)
            message_type = MessageType.VIDEO if reply_message_type in [MessageType.VIDEO, MessageType.GIF] else MessageType.VIDEO_NOTE
        else:
            video_path = stats_utils.get_random_media_path(CHAT_VIDEO_NOTES_DIR_PATH)
            message_type = MessageType.VIDEO_NOTE
        output_path = self.ytdl.swap_video_audio(video_path, audio_path)

        await self.send_message(update, context, message_type, output_path, '')

    def get_reply_message_type(self, reply_message_id):
        reply_message = self.chat_df[self.chat_df['message_id'] == reply_message_id]
        if reply_message.empty:
            return None
        return MessageType(reply_message.iloc[0]['message_type'])
