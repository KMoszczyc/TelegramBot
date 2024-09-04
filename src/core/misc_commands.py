import logging
from datetime import datetime

import telegram
from telegram import Update
from telegram.ext import ContextTypes

from src.core.command_logger import CommandLogger
from src.models.bot_state import BotState
from src.models.command_args import CommandArgs
from definitions import ozjasz_phrases, bartosiak_phrases, tvp_headlines, tvp_latest_headlines, commands, bible_df, ArgType, shopping_sundays
import src.core.utils as core_utils
import src.stats.utils as stats_utils

log = logging.getLogger(__name__)


class Commands:
    def __init__(self, command_logger: CommandLogger):
        self.command_logger = command_logger
        # CommandLogger.decorate_commands(self, command_logger)

    @staticmethod
    async def cmd_ozjasz(update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(args=context.args, phrases=ozjasz_phrases)
        filtered_phrases, command_args = core_utils.preprocess_input(command_args)
        if command_args.error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error)
            return

        response = core_utils.select_random_phrase(filtered_phrases, 'Nie ma takiej wypowiedzi :(')
        await context.bot.send_message(chat_id=update.effective_chat.id, text=response)

    @staticmethod
    async def cmd_bartosiak(update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(args=context.args, phrases=bartosiak_phrases)
        filtered_phrases, command_args = core_utils.preprocess_input(command_args)
        if command_args.error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error)
            return

        response = core_utils.select_random_phrase(filtered_phrases, 'Nie ma takiej wypowiedzi :(')
        await context.bot.send_message(chat_id=update.effective_chat.id, text=response)

    @staticmethod
    async def cmd_tvp(update: Update, context: ContextTypes.DEFAULT_TYPE):
        merged_headlines = tvp_latest_headlines + tvp_headlines
        command_args = CommandArgs(args=context.args, phrases=merged_headlines)
        filtered_phrases, command_args = core_utils.preprocess_input(command_args)
        if command_args.error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error)
            return

        response = core_utils.select_random_phrase(filtered_phrases, 'Nie ma takiego nagłówka :(')

        await context.bot.send_message(chat_id=update.effective_chat.id, text=response)

    @staticmethod
    async def cmd_tvp_latest(update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(args=context.args, phrases=tvp_latest_headlines)
        filtered_phrases, command_args = core_utils.preprocess_input(command_args)
        if command_args.error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error)
            return

        response = core_utils.select_random_phrase(filtered_phrases, 'Nie ma takiego nagłówka :(')
        await context.bot.send_message(chat_id=update.effective_chat.id, text=response)

    @staticmethod
    async def cmd_tusk(update: Update, context: ContextTypes.DEFAULT_TYPE):
        tusk_headlines = [headline for headline in tvp_headlines if 'tusk' in headline.lower()]
        command_args = CommandArgs(args=context.args, phrases=tusk_headlines)
        filtered_phrases, command_args = core_utils.preprocess_input(command_args)
        if command_args.error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error)
            return

        response = core_utils.select_random_phrase(filtered_phrases, 'Nie ma takiego nagłówka :(')
        await context.bot.send_message(chat_id=update.effective_chat.id, text=response)

    @staticmethod
    async def cmd_are_you_lucky_today(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id

        response = core_utils.are_you_lucky(user_id)
        await context.bot.send_message(chat_id=update.effective_chat.id, text=response)

    @staticmethod
    async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
        response = "Existing commands:\n- /" + '\n- /'.join(commands)
        await context.bot.send_message(chat_id=update.effective_chat.id, text=response)

    @staticmethod
    async def cmd_bible(update: Update, context: ContextTypes.DEFAULT_TYPE, bot_state: BotState):
        command_args = CommandArgs(args=context.args, available_named_args={'prev': ArgType.POSITIVE_INT, 'next': ArgType.POSITIVE_INT, 'all': ArgType.NONE, 'num': ArgType.POSITIVE_INT})
        command_args = core_utils.parse_args(command_args)
        if command_args.error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error)
            return

        filter_phrase = command_args.joined_args_lower
        filtered_df = bible_df[bible_df['text'].str.lower().str.contains(filter_phrase)]
        filtered_df = filtered_df.sample(frac=1)

        if filtered_df.empty:
            response = 'Nie ma takiego cytatu. Beduinom pustynnym weszło post-nut clarity po wyruchaniu kozy. :('
        elif 'num' in command_args.named_args:
            filtered_df = filtered_df.head(command_args.named_args['num'])
            response = core_utils.display_bible_df(filtered_df, label=f'{len(filtered_df)} bible verses with "{filter_phrase}"')
        elif 'all' in command_args.named_args:
            response = core_utils.display_bible_df(filtered_df, label=f'{len(filtered_df)} bible verses with "{filter_phrase}"')
        elif 'prev' in command_args.named_args and bot_state.last_bible_verse_id != -1:
            start_index = max(0, bot_state.last_bible_verse_id - command_args.named_args['prev'])
            filtered_df = bible_df.iloc[start_index:bot_state.last_bible_verse_id]
            label = f'{command_args.named_args['prev']} bible verses before {core_utils.get_full_siglum(bible_df.iloc[bot_state.last_bible_verse_id])}'
            response = core_utils.display_bible_df(filtered_df, label=label, show_siglum=False)
        elif 'next' in command_args.named_args and bot_state.last_bible_verse_id != -1:
            end_index = min(len(bible_df), bot_state.last_bible_verse_id + command_args.named_args['next'] + 1)
            filtered_df = bible_df.iloc[bot_state.last_bible_verse_id + 1:end_index]
            label = f'{command_args.named_args['next']} bible verses after {core_utils.get_full_siglum(bible_df.iloc[bot_state.last_bible_verse_id])}'
            response = core_utils.display_bible_df(filtered_df, label=label, show_siglum=False)
        else:
            random_row = filtered_df.iloc[0]
            bot_state.last_bible_verse_id = random_row.name
            response = f"[{random_row['abbreviation']} {random_row['chapter']}, {random_row['verse']}] {random_row['text']}"

        await context.bot.send_message(chat_id=update.effective_chat.id, text=response)

    @staticmethod
    async def cmd_bible_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
        bible_stats_df = bible_df.drop_duplicates('book')[['book', 'abbreviation']].set_index('abbreviation')
        bible_stats_df['chapter_count'] = bible_df.drop_duplicates(['abbreviation', 'chapter'])[['abbreviation', 'chapter']].set_index('abbreviation').groupby('abbreviation').size()
        bible_stats_df['verse_count'] = bible_df.groupby(['abbreviation']).size()

        bible_stats_df = bible_stats_df.sort_values(by='verse_count', ascending=False)

        text = "``` Bible stats:\n"
        text += "Book".ljust(28) + "Chapters Verses"
        for index, row in bible_stats_df.iterrows():
            text += f"\n[{index}] {row['book']}:".ljust(36) + f"{row['chapter_count']}".ljust(4) + f"{row['verse_count']}"

        text += "```"
        text = stats_utils.escape_special_characters(text)
        await context.bot.send_message(chat_id=update.effective_chat.id, text=text, parse_mode=telegram.constants.ParseMode.MARKDOWN_V2)

    @staticmethod
    async def cmd_show_shopping_sundays(update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(args=context.args, available_named_args={'all': ArgType.NONE})
        command_args = core_utils.parse_args(command_args)
        if command_args.error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error)
            return

        dt_now = datetime.now()
        sundays_dt = [datetime.strptime(date, '%d-%m-%Y') for date in shopping_sundays]
        filtered_sundays = [sunday for sunday in sundays_dt if sunday >= dt_now]
        if 'all' in command_args.named_args:
            response = f'Wszystkie handlowe niedziele w {dt_now.year}:\n - ' + '\n - '.join([core_utils.display_shopping_sunday(sunday) for sunday in sundays_dt])
        elif filtered_sundays:
            response = f'Kolejna handlowa niedziela jest: {core_utils.display_shopping_sunday(filtered_sundays[0])}'
        else:
            response = 'Nie ma już handlowych niedzieli w tym roku :(('

        await context.bot.send_message(chat_id=update.effective_chat.id, text=response)
