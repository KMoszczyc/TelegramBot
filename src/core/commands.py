import os.path
import random
from telegram import Update
from telegram.ext import ContextTypes

import src.core.utils as utils
from definitions import TVP_HEADLINES_PATH, TVP_LATEST_HEADLINES_PATH, OZJASZ_PHRASES_PATH, BARTOSIAK_PATH

tvp_headlines = utils.read_str_file(TVP_HEADLINES_PATH)
tvp_latest_headlines = utils.read_str_file(TVP_LATEST_HEADLINES_PATH)
ozjasz_phrases = utils.read_str_file(OZJASZ_PHRASES_PATH)
bartosiak_phrases = utils.read_str_file(BARTOSIAK_PATH)


# async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
#     await context.bot.send_message(chat_id=update.effective_chat.id, text="I'm a bot, please talk to me!")


async def ozjasz(update: Update, context: ContextTypes.DEFAULT_TYPE):
    filter_phrase = ' '.join(context.args).lower()
    filtered_prases = [phrase for phrase in ozjasz_phrases if filter_phrase in phrase.lower()]
    if filtered_prases:
        response = random.choice(filtered_prases)
    else:
        response = 'Nie ma takiego nagłówka :('

    await context.bot.send_message(chat_id=update.effective_chat.id, text=response)

async def bartosiak(update: Update, context: ContextTypes.DEFAULT_TYPE):
    filter_phrase = ' '.join(context.args).lower()
    filtered_prases = [phrase for phrase in bartosiak_phrases if filter_phrase in phrase.lower()]
    if filtered_prases:
        response = random.choice(filtered_prases)
    else:
        response = 'Nie ma takiej wypowiedzi :('

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
    print('chat_id:',update.effective_chat.id)
    tusk_headlines = [headline for headline in tvp_headlines if 'tusk' in headline.lower()]
    random_headline = random.choice(tusk_headlines)
    await context.bot.send_message(chat_id=update.effective_chat.id, text=random_headline)


async def help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    commands = ['ozjasz [phrase]', 'tvp [phrase]', 'tvp_latest [phrase]', 'tusk', 'chatstats [today,yesterday,week,month,year,total]', 'topmessages [today,yesterday,week,month,year,total]', 'help']
    response = "Istniejące komendy to:\n- /" + '\n- /'.join(commands)
    # response =  ', '.join(commands)

    await context.bot.send_message(chat_id=update.effective_chat.id, text=response)
