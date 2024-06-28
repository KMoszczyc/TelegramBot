import os.path
import logging

from telegram import Update
from telegram.ext import ContextTypes
import telegram
import pandas as pd

from definitions import USERS_PATH, CLEANED_CHAT_HISTORY_PATH, REACTIONS_PATH, CHAT_IMAGES_DIR_PATH, UPDATE_REQUIRED_PATH, EmojiType, ArgType
import src.stats.utils as utils
from src.stats.models.command_args import CommandArgs

pd.options.mode.chained_assignment = None
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', 1000)
log = logging.getLogger(__name__)

negative_emojis = ['👎', '😢', '😭', '🤬', '🤡', '💩', '😫', '😩', '🥶', '🤨', '🧐', '🙃', '😒', '😠', '😣', '🗿']
MAX_INT = 24 * 365 * 20


class ChatCommands:
    def __init__(self):
        self.users_df = utils.read_df(USERS_PATH)
        self.chat_df = utils.read_df(CLEANED_CHAT_HISTORY_PATH)
        self.reactions_df = utils.read_df(REACTIONS_PATH)

    def update(self):
        """If chat data was updated recentely, reload it."""
        if not os.path.isfile(UPDATE_REQUIRED_PATH):
            log.info(f"Update not required, {UPDATE_REQUIRED_PATH} doesn't exist.")
            return

        log.info('Reloading chat data due to the recent update.')
        self.chat_df = utils.read_df(CLEANED_CHAT_HISTORY_PATH)
        self.reactions_df = utils.read_df(REACTIONS_PATH)
        utils.remove_file(UPDATE_REQUIRED_PATH)

    def preprocess_input(self, command_args, emoji_type: EmojiType = EmojiType.ALL):
        self.update()

        command_args = utils.parse_args(self.users_df, command_args)
        if command_args.error != '':
            return self.chat_df, self.reactions_df, command_args

        filtered_chat_df = utils.filter_by_time_df(self.chat_df, command_args)
        filtered_reactions_df = utils.filter_by_time_df(self.reactions_df, command_args)

        filtered_chat_df = utils.filter_emojis_by_emoji_type(filtered_chat_df, emoji_type, 'reaction_emojis')

        filtered_chat_df['reactions_num'] = filtered_chat_df['reaction_emojis'].apply(lambda x: len(x))
        filtered_chat_df = filtered_chat_df.sort_values(['reactions_num', 'timestamp'], ascending=[False, True])
        filtered_chat_df['timestamp'] = filtered_chat_df['timestamp'].dt.tz_convert('Europe/Warsaw')

        if command_args.user is not None:
            filtered_chat_df = filtered_chat_df[filtered_chat_df['final_username'] == command_args.user]

        log.info('Last message:')
        print(filtered_chat_df.tail(1))

        return filtered_chat_df, filtered_reactions_df, command_args

    async def summary(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(args=context.args, expected_args=[ArgType.PERIOD, ArgType.USER])
        chat_df, reactions_df, command_args = self.preprocess_input(command_args, EmojiType.ALL)

        shifted_chat_df = utils.filter_by_shifted_time_df(self.chat_df, command_args)
        shifted_reactions_df = utils.filter_by_shifted_time_df(self.reactions_df, command_args)

        if command_args.error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error)
            return

        sad_reactions_df = utils.filter_emoji_by_emoji_type(reactions_df, EmojiType.NEGATIVE, 'emoji')
        text_only_chat_df = chat_df[chat_df['text'] != '']

        images_num = len(chat_df[chat_df['photo']])
        reactions_received_counts = reactions_df.groupby('reacted_to_username').size().reset_index(name='count').sort_values('count', ascending=False)
        reactions_given_counts = reactions_df.groupby('reacting_username').size().reset_index(name='count').sort_values('count', ascending=False)
        sad_reactions_received_counts = sad_reactions_df.groupby('reacted_to_username').size().reset_index(name='count').sort_values('count', ascending=False)
        sad_reactions_given_counts = sad_reactions_df.groupby('reacting_username').size().reset_index(name='count').sort_values('count', ascending=False)
        message_counts = chat_df.groupby('final_username').size().reset_index(name='count').sort_values('count', ascending=False)

        # Calculate message and reaction count changes
        message_count_change = round((len(chat_df) - len(shifted_chat_df)) / len(shifted_chat_df) * 100, 1) if not shifted_chat_df.empty else 0
        reaction_count_change = round((len(reactions_df) - len(shifted_reactions_df)) / len(shifted_reactions_df) * 100, 1) if not shifted_reactions_df.empty else 0
        message_count_change_text = f'+{message_count_change}%' if message_count_change > 0 else f'{message_count_change}%'
        reaction_count_change_text = f'+{reaction_count_change}%' if reaction_count_change > 0 else f'{reaction_count_change}%'

        # Create summary
        text = "*Chat summary*"
        text += f"({command_args.period_mode.value}):" if command_args.period_time == -1 else f" (past {command_args.period_time}h):"
        text += f"\n- *Total*: *{len(chat_df)} ({message_count_change_text})* messages, *{len(reactions_df)} ({reaction_count_change_text})* reactions and *{images_num}* images"
        text += "\n- *Top spammer*: " + ", ".join([f"{row['final_username']}: *{row['count']}*" for _, row in message_counts.head(3).iterrows()])
        text += "\n- *Most liked*: " + ", ".join([f"{row['reacted_to_username']}: *{row['count']}*" for _, row in reactions_received_counts.head(3).iterrows()])
        text += "\n- *Most liking*: " + ", ".join([f"{row['reacting_username']}: *{row['count']}*" for _, row in reactions_given_counts.head(3).iterrows()])
        text += "\n- *Most disliked*: " + ", ".join(
            [f"{row['reacted_to_username']}: *{row['count']}*" for _, row in sad_reactions_received_counts.head(3).iterrows()])
        text += "\n- *Most disliking*: " + ", ".join(
            [f"{row['reacting_username']}: *{row['count']}*" for _, row in sad_reactions_given_counts.head(3).iterrows()])
        text += "\n- *Top message*: " + ", ".join(
            [f"{row['final_username']} [{utils.dt_to_str(row['timestamp'])}]: {row['text']} [{''.join(row['reaction_emojis'])}]" for _, row in
             text_only_chat_df.head(1).iterrows()])

        text = utils.escape_special_characters(text)
        await context.bot.send_message(chat_id=update.effective_chat.id, text=text, parse_mode=telegram.constants.ParseMode.MARKDOWN_V2)

    async def messages_by_reactions(self, update: Update, context: ContextTypes.DEFAULT_TYPE, emoji_type: EmojiType = EmojiType.ALL):
        """Top or worst 5 messages from selected time period by number of reactions"""
        command_args = CommandArgs(args=context.args, expected_args=[ArgType.PERIOD, ArgType.USER])
        chat_df, reactions_df, command_args = self.preprocess_input(command_args, emoji_type)
        if command_args.error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error)
            return

        chat_df = chat_df[chat_df['text'] != '']
        label = utils.emoji_sentiment_to_label(emoji_type)

        text = f"{label} Cinco messages"
        text += f" by {command_args.user}" if command_args.user is not None else " "
        text += f"({command_args.period_mode.value}):" if command_args.period_time == -1 else f" (past {command_args.period_time}h):"

        for i, (index, row) in enumerate(chat_df.head(5).iterrows()):
            if row['reactions_num'] == 0:
                break
            text += f"\n{i + 1}. {row['final_username']}" if command_args.user is None else f"\n{i + 1}."
            text += f" [{utils.dt_to_str(row['timestamp'])}]:"
            text += f" {row['text']} [{''.join(row['reaction_emojis'])}]"

        await context.bot.send_message(chat_id=update.effective_chat.id, text=text)

    async def memes_by_reactions(self, update: Update, context: ContextTypes.DEFAULT_TYPE, emoji_type: EmojiType = EmojiType.ALL):
        """Top or sad 5 memes (images) from selected time period by number of reactions"""
        command_args = CommandArgs(args=context.args, expected_args=[ArgType.PERIOD, ArgType.USER])
        chat_df, reactions_df, command_args = self.preprocess_input(command_args, emoji_type)
        if command_args.error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error)
            return

        label = utils.emoji_sentiment_to_label(emoji_type)
        text = f"{label} Cinco memes"
        text += ' ' if command_args.user is None else f" by {command_args.user}"
        text += f"({command_args.period_mode.value}):" if command_args.period_time == -1 else f" (past {command_args.period_time}h):"

        await context.bot.send_message(chat_id=update.effective_chat.id, text=text)

        chat_df = chat_df[chat_df['photo'] == True]

        for i, (index, row) in enumerate(chat_df.head(5).iterrows()):
            if row['reactions_num'] == 0:
                break
            img_path = os.path.join(CHAT_IMAGES_DIR_PATH, f'{str(row['message_id'])}.jpg')
            text = f"\n{i + 1}. {row['final_username']}" if command_args.user is None else f"\n{i + 1}."
            text += f" [{utils.dt_to_str(row['timestamp'])}]:"
            text += f" {row['text']} [{''.join(row['reaction_emojis'])}]"

            # text = f"\n{i + 1}. {row['final_username']}: {row['text']} [{''.join(row['reaction_emojis'])}]"
            await context.bot.send_photo(chat_id=update.effective_chat.id, photo=img_path, caption=text)

    async def last_messages(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Display last 5 messages from chat history"""
        command_args = CommandArgs(args=context.args, expected_args=[ArgType.USER])

        chat_df, reactions_df, command_args = self.preprocess_input(command_args, EmojiType.ALL)
        chat_df = chat_df.sort_values(by='timestamp', ascending=False)
        if command_args.error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error)
            return

        text = f"Last 5 messages"
        text += f" by {command_args.user}" if command_args.user is not None else ":"

        for i, (index, row) in enumerate(chat_df.head(5).iterrows()):
            text += f"\n{i + 1}. {row['final_username']}" if command_args.user is None else f"\n{i + 1}."
            text += f" [{utils.dt_to_str(row['timestamp'])}]:"
            text += f" {row['text']} [{''.join(row['reaction_emojis'])}]"

        await context.bot.send_message(chat_id=update.effective_chat.id, text=text)
