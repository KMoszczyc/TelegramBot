import logging
from datetime import datetime

from telegram import Update
from telegram.ext import ContextTypes

from src.models.command_args import CommandArgs
from definitions import ozjasz_phrases, bartosiak_phrases, tvp_headlines, tvp_latest_headlines, commands, bible_df, ArgType, shopping_sundays
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
    command_args = CommandArgs(args=context.args, available_named_args={'prev': ArgType.POSITIVE_INT, 'next': ArgType.POSITIVE_INT, 'all': ArgType.NONE, 'num': ArgType.POSITIVE_INT})
    command_args = core_utils.parse_args(command_args)
    if command_args.error != '':
        await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error)
        return

    filter_phrase = command_args.joined_args_lower
    filtered_df = bible_df[bible_df['text'].str.lower().str.contains(filter_phrase)]
    filtered_df = filtered_df.sample(frac=1)

    print(f'_{filter_phrase}_')
    if filtered_df.empty:
        response = 'Nie ma takiego cytatu. Beduinom pustynnym weszło post-nut clarity po wyruchaniu kozy. :('
    elif 'num' in command_args.named_args:
        filtered_df = filtered_df.head(command_args.named_args['num'])
        response = core_utils.display_bible_df(filtered_df, filter_phrase)
    elif 'all' in command_args.named_args:
        response = core_utils.display_bible_df(filtered_df, filter_phrase)
    else:
        random_row = filtered_df.iloc[0]
        response = f"[{random_row['abbreviation']} {random_row['chapter']}, {random_row['verse']}] {random_row['text']}"

    await context.bot.send_message(chat_id=update.effective_chat.id, text=response)


async def show_shopping_sundays(update: Update, context: ContextTypes.DEFAULT_TYPE):
    command_args = CommandArgs(args=context.args, available_named_args={'all': ArgType.NONE})
    command_args = core_utils.parse_args(command_args)
    if command_args.error != '':
        await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error)
        return

    dt_now = datetime.now()
    sundays_dt = [datetime.strptime(date, '%d-%m-%Y') for date in shopping_sundays]
    filtered_sundays = [sunday for sunday in sundays_dt if sunday >= dt_now]
    if 'all' in command_args.named_args:
        response = f'Wszystkie niehandlowe niedziele w {dt_now.year}:\n - ' + '\n - '.join([core_utils.display_shopping_sunday(sunday) for sunday in sundays_dt])
    elif filtered_sundays:
        response = f'Kolejna handlowa niedziela to: {core_utils.display_shopping_sunday(filtered_sundays[0])}'
    else:
        response = 'Nie ma już handlowych niedzieli w tym roku :(('

    await context.bot.send_message(chat_id=update.effective_chat.id, text=response)
