from telegram import Update
from telegram.ext import ContextTypes

from src.models.command_args import CommandArgs
from definitions import ozjasz_phrases, bartosiak_phrases, tvp_headlines, tvp_latest_headlines
import src.core.utils as core_utils


async def ozjasz(update: Update, context: ContextTypes.DEFAULT_TYPE):
    command_args = CommandArgs(args=context.args, phrases=ozjasz_phrases)
    filtered_phrases = core_utils.preprocess_input(command_args)
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


async def help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    commands = ['ozjasz [phrase]', 'tvp [phrase]', 'tvp_latest [phrase]', 'tusk', 'chatstats [today,yesterday,week,month,year,total]', 'topmessages [today,yesterday,week,month,year,total]', 'help']
    response = "Istniejące komendy to:\n- /" + '\n- /'.join(commands)

    await context.bot.send_message(chat_id=update.effective_chat.id, text=response)
