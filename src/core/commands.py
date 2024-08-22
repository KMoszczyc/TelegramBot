import logging

from telegram import Update
from telegram.ext import ContextTypes

from src.models.command_args import CommandArgs
from definitions import ozjasz_phrases, bartosiak_phrases, tvp_headlines, tvp_latest_headlines, commands, bible_df
import src.core.utils as core_utils

log = logging.getLogger(__name__)


async def ozjasz(update: Update, context: ContextTypes.DEFAULT_TYPE):
    command_args = CommandArgs(args=context.args, phrases=ozjasz_phrases)
    filtered_phrases, command_args = core_utils.preprocess_input(command_args)
    if command_args.error != '':
        await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error)
        return

    response = core_utils.select_random_phrase(filtered_phrases, 'Nie ma takiej wypowiedzi :(')
    await context.bot.send_message(chat_id=update.effective_chat.id, text=response)


async def bartosiak(update: Update, context: ContextTypes.DEFAULT_TYPE):
    command_args = CommandArgs(args=context.args, phrases=bartosiak_phrases)
    filtered_phrases, command_args = core_utils.preprocess_input(command_args)
    if command_args.error != '':
        await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error)
        return

    response = core_utils.select_random_phrase(filtered_phrases, 'Nie ma takiej wypowiedzi :(')
    await context.bot.send_message(chat_id=update.effective_chat.id, text=response)


async def tvp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    merged_headlines = tvp_latest_headlines + tvp_headlines
    command_args = CommandArgs(args=context.args, phrases=merged_headlines)
    filtered_phrases, command_args = core_utils.preprocess_input(command_args)
    if command_args.error != '':
        await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error)
        return

    response = core_utils.select_random_phrase(filtered_phrases, 'Nie ma takiego nagłówka :(')

    await context.bot.send_message(chat_id=update.effective_chat.id, text=response)


async def tvp_latest(update: Update, context: ContextTypes.DEFAULT_TYPE):
    command_args = CommandArgs(args=context.args, phrases=tvp_latest_headlines)
    filtered_phrases, command_args = core_utils.preprocess_input(command_args)
    if command_args.error != '':
        await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error)
        return

    response = core_utils.select_random_phrase(filtered_phrases, 'Nie ma takiego nagłówka :(')
    await context.bot.send_message(chat_id=update.effective_chat.id, text=response)


async def tusk(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tusk_headlines = [headline for headline in tvp_headlines if 'tusk' in headline.lower()]
    command_args = CommandArgs(args=context.args, phrases=tusk_headlines)
    filtered_phrases, command_args = core_utils.preprocess_input(command_args)
    if command_args.error != '':
        await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error)
        return

    response = core_utils.select_random_phrase(filtered_phrases, 'Nie ma takiego nagłówka :(')
    await context.bot.send_message(chat_id=update.effective_chat.id, text=response)


async def are_you_lucky_today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    response = core_utils.are_you_lucky(user_id)
    await context.bot.send_message(chat_id=update.effective_chat.id, text=response)


async def help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    response = "Existing commands:\n- /" + '\n- /'.join(commands)
    print(len(response), response)
    await context.bot.send_message(chat_id=update.effective_chat.id, text=response)


async def bible(update: Update, context: ContextTypes.DEFAULT_TYPE):
    command_args = CommandArgs(args=context.args)
    command_args = core_utils.parse_args(command_args)
    if command_args.error != '':
        await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error)
        return

    filter_phrase = command_args.joined_args_lower
    filtered_phrases = bible_df[bible_df['text'].str.lower().str.contains(filter_phrase)]

    if filtered_phrases.empty:
        response = 'Nie ma takiego cytatu. Beduinom pustynnym weszło post-nut clarity po wyruchaniu kozy. :('
    else:
        random_row = filtered_phrases.sample(1).iloc[0]
        response = f"[{random_row['abbreviation']} {random_row['chapter']}, {random_row['verse']}] {random_row['text']}"

    await context.bot.send_message(chat_id=update.effective_chat.id, text=response)
