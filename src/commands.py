import os.path
import random
from telegram import Update
from telegram.ext import ContextTypes

import src.utils as utils
from definitions import ROOT_DIR

tvp_headlines = utils.read_str_file(os.path.join(ROOT_DIR, 'data/paski-tvp.txt'))
tvp_latest_headlines = utils.read_str_file(os.path.join(ROOT_DIR, 'data/tvp_latest_headlines.txt'))

ozjasz_phrases = utils.read_str_file(os.path.join(ROOT_DIR, 'data/ozjasz-wypowiedzi.txt'))


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await context.bot.send_message(chat_id=update.effective_chat.id, text="I'm a bot, please talk to me!")


async def ozjasz(update: Update, context: ContextTypes.DEFAULT_TYPE):
    filter_phrase = ' '.join(context.args).lower()
    filtered_prases = [phrase for phrase in ozjasz_phrases if filter_phrase in phrase.lower()]
    if filtered_prases:
        response = random.choice(filtered_prases)
    else:
        response = 'Nie ma takiego nagłówka :('

    await context.bot.send_message(chat_id=update.effective_chat.id, text=response)


async def tvp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    filter_phrase = ' '.join(context.args).lower()
    merged_headlines = tvp_latest_headlines + tvp_headlines
    filtered_headlines = [headline for headline in merged_headlines if filter_phrase in headline.lower()]
    if filtered_headlines:
        response = random.choice(filtered_headlines)
    else:
        response = 'Nie ma takiego nagłówka :('

    await context.bot.send_message(chat_id=update.effective_chat.id, text=response)


async def tvp_latest(update: Update, context: ContextTypes.DEFAULT_TYPE):
    filter_phrase = ' '.join(context.args).lower()
    filtered_headlines = [headline for headline in tvp_latest_headlines if filter_phrase in headline.lower()]
    if filtered_headlines:
        response = random.choice(filtered_headlines)
    else:
        response = 'Nie ma takiego nagłówka :('

    await context.bot.send_message(chat_id=update.effective_chat.id, text=response)


async def tusk(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tusk_headlines = [headline for headline in tvp_headlines if 'tusk' in headline.lower()]
    random_headline = random.choice(tusk_headlines)
    await context.bot.send_message(chat_id=update.effective_chat.id, text=random_headline)


async def help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    commands = ['ozjasz', 'tvp', 'tvp_latest', 'tusk', 'help']
    response = "Istniejące komendy to: " + ', '.join(commands)
    # response =  ', '.join(commands)

    await context.bot.send_message(chat_id=update.effective_chat.id, text=response)
