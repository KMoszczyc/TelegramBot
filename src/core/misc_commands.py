import logging
from datetime import datetime

import pandas as pd
import telegram
from telegram import Update
from telegram.ext import ContextTypes

from src.core.command_logger import CommandLogger
from src.core.job_persistance import JobPersistance
from src.models.bot_state import BotState
from src.models.command_args import CommandArgs
from definitions import ozjasz_phrases, bartosiak_phrases, tvp_headlines, tvp_latest_headlines, commands, bible_df, ArgType, shopping_sundays, USERS_PATH, arguments_help, europejskafirma_phrases, \
    boczek_phrases, kiepscy_df, walesa_phrases, HolyTextType, SiglumType, quran_df
import src.core.utils as core_utils
import src.stats.utils as stats_utils

log = logging.getLogger(__name__)


class Commands:
    def __init__(self, command_logger: CommandLogger, job_persistance: JobPersistance, bot_state: BotState):
        self.command_logger = command_logger
        self.job_persistance = job_persistance
        self.bot_state = bot_state
        self.users_df = stats_utils.read_df(USERS_PATH)

    async def cmd_all(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        usernames = self.users_df['final_username'].tolist()
        user_ids = self.users_df.index.tolist()

        text = ''
        for username, user_id in zip(usernames, user_ids):
            text += f"[{username}](tg://user?id={user_id}) "

        await context.bot.send_message(chat_id=update.effective_chat.id, text=text, parse_mode="Markdown")

    async def cmd_ozjasz(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(args=context.args, phrases=ozjasz_phrases, is_text_arg=True)
        filtered_phrases, command_args = core_utils.preprocess_input(self.users_df, command_args)
        if command_args.error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error)
            return

        response = core_utils.select_random_phrase(filtered_phrases, 'Nie ma takiej wypowiedzi :(')
        await context.bot.send_message(chat_id=update.effective_chat.id, text=response)

    async def cmd_boczek(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(args=context.args, expected_args=[ArgType.TEXT_MULTISPACED])
        command_args = core_utils.parse_args(self.users_df, command_args)
        if command_args.error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error)
            return

        curse = core_utils.select_random_phrase(boczek_phrases, 'Nie ma takiej wypowiedzi :(')
        response = f"{command_args.string} to {curse}" if command_args.string != '' else curse
        await context.bot.send_message(chat_id=update.effective_chat.id, text=response)

    async def cmd_europejskafirma(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(args=context.args, phrases=europejskafirma_phrases, is_text_arg=True)
        filtered_phrases, command_args = core_utils.preprocess_input(self.users_df, command_args)
        if command_args.error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error)
            return

        response = core_utils.select_random_phrase(filtered_phrases, 'Nie ma takiej wypowiedzi :(')
        await context.bot.send_message(chat_id=update.effective_chat.id, text=response)

    async def cmd_bartosiak(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(args=context.args, phrases=bartosiak_phrases, is_text_arg=True)
        filtered_phrases, command_args = core_utils.preprocess_input(self.users_df, command_args)
        if command_args.error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error)
            return

        response = core_utils.select_random_phrase(filtered_phrases, 'Nie ma takiej wypowiedzi :(')
        await context.bot.send_message(chat_id=update.effective_chat.id, text=response)

    async def cmd_tvp(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        merged_headlines = tvp_latest_headlines + tvp_headlines
        command_args = CommandArgs(args=context.args, phrases=merged_headlines, is_text_arg=True)
        filtered_phrases, command_args = core_utils.preprocess_input(self.users_df, command_args)
        if command_args.error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error)
            return

        response = core_utils.select_random_phrase(filtered_phrases, 'Nie ma takiego nagłówka :(')

        await context.bot.send_message(chat_id=update.effective_chat.id, text=response)

    async def cmd_tvp_latest(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(args=context.args, phrases=tvp_latest_headlines, is_text_arg=True)
        filtered_phrases, command_args = core_utils.preprocess_input(self.users_df, command_args)
        if command_args.error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error)
            return

        response = core_utils.select_random_phrase(filtered_phrases, 'Nie ma takiego nagłówka :(')
        await context.bot.send_message(chat_id=update.effective_chat.id, text=response)

    async def cmd_tusk(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        tusk_headlines = [headline for headline in tvp_headlines if 'tusk' in headline.lower()]
        command_args = CommandArgs(args=context.args, phrases=tusk_headlines, is_text_arg=True)
        filtered_phrases, command_args = core_utils.preprocess_input(self.users_df, command_args)
        if command_args.error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error)
            return

        response = core_utils.select_random_phrase(filtered_phrases, 'Nie ma takiego nagłówka :(')
        await context.bot.send_message(chat_id=update.effective_chat.id, text=response)

    async def cmd_walesa(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(args=context.args, phrases=walesa_phrases, is_text_arg=True)
        filtered_phrases, command_args = core_utils.preprocess_input(self.users_df, command_args)
        if command_args.error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error)
            return

        response = core_utils.select_random_phrase(filtered_phrases, 'Nie ma takiego przedmiotu :(').replace(r"\n", "\n")
        await context.bot.send_message(chat_id=update.effective_chat.id, text=response)

    async def cmd_are_you_lucky_today(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(args=context.args, is_text_arg=True)
        command_args = core_utils.parse_args(self.users_df, command_args)

        args_provided = command_args.joined_args != ''
        lucky_text_number = core_utils.text_to_number(command_args.joined_args) if command_args.joined_args != '' else 0
        user_id = update.effective_user.id
        lucky_input = user_id + lucky_text_number

        response = core_utils.are_you_lucky(lucky_input, args_provided)
        await context.bot.send_message(chat_id=update.effective_chat.id, text=response)

    async def cmd_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        text = "Existing commands:\n- /" + '\n- /'.join(commands)
        text += "\n\n *Arguments*:\n" + "\n".join(arguments_help)

        text = stats_utils.escape_special_characters(text)
        await context.bot.send_message(chat_id=update.effective_chat.id, text=text, parse_mode=telegram.constants.ParseMode.MARKDOWN_V2)

    async def cmd_bible(self, update: Update, context: ContextTypes.DEFAULT_TYPE, bot_state: BotState):
        command_args = CommandArgs(args=context.args, is_text_arg=True,
                                   available_named_args={'prev': ArgType.POSITIVE_INT, 'next': ArgType.POSITIVE_INT, 'all': ArgType.NONE, 'num': ArgType.POSITIVE_INT, 'count': ArgType.NONE,
                                                         'book': ArgType.STRING, 'chapter': ArgType.POSITIVE_INT},
                                   available_named_args_aliases={'p': 'prev', 'n': 'next', 'a': 'all', 'c': 'count', 'b': 'book', 'ch': 'chapter'})
        command_args = core_utils.parse_args(self.users_df, command_args)
        if command_args.error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error)
            return

        filter_phrase = command_args.joined_args_lower
        filtered_df = bible_df[bible_df['text'].str.lower().str.contains(filter_phrase)]

        response = ''
        if 'book' in command_args.named_args:
            abbrevarions = bible_df['abbreviation'].unique()
            books = bible_df['book'].unique()
            matched_abbreviation = core_utils.match_substr_to_list_of_texts(command_args.named_args['book'], abbrevarions)
            matched_book_name = core_utils.match_substr_to_list_of_texts(command_args.named_args['book'], books)

            if matched_abbreviation is not None:
                filtered_df = filtered_df[filtered_df['abbreviation'] == matched_abbreviation]

            if matched_book_name is not None:
                filtered_df = filtered_df[filtered_df['book'] == matched_book_name]

            if matched_book_name is None or matched_abbreviation is None:
                response += f'[{filtered_df.iloc[0]['abbreviation']}] {filtered_df.iloc[0]['book']}, '

        if 'chapter' in command_args.named_args:  # in chapter mode we read a random chapter from the bible
            random_row = filtered_df.sample(frac=1).iloc[0]
            book = random_row['book']
            chapter = random_row['chapter']
            filtered_df = bible_df[(bible_df['book'] == book) & (bible_df['chapter'] == chapter)].head(command_args.named_args['chapter'])
            response += core_utils.display_holy_text_df(filtered_df, bot_state, HolyTextType.BIBLE, label=f'{len(filtered_df)} verses from chapter {chapter}', show_siglum=False)
            await context.bot.send_message(chat_id=update.effective_chat.id, text=response)
            return
        else:
            filtered_df = filtered_df.sample(frac=1)

        response, error = self.handle_holy_text_named_params(command_args, filtered_df, bible_df, bot_state, filter_phrase, HolyTextType.BIBLE)
        if error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=error)
            return

        await context.bot.send_message(chat_id=update.effective_chat.id, text=response)

    async def cmd_quran(self, update: Update, context: ContextTypes.DEFAULT_TYPE, bot_state: BotState):
        command_args = CommandArgs(args=context.args, is_text_arg=True,
                                   available_named_args={'prev': ArgType.POSITIVE_INT, 'next': ArgType.POSITIVE_INT, 'all': ArgType.NONE, 'num': ArgType.POSITIVE_INT, 'count': ArgType.NONE,
                                                          'chapter': ArgType.STRING, 'verse': ArgType.STRING},
                                   available_named_args_aliases={'p': 'prev', 'n': 'next', 'a': 'all', 'c': 'count', 'ch': 'chapter', 'num': 'num', 'v': 'verse'})
        command_args = core_utils.parse_args(self.users_df, command_args)
        if command_args.error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error)
            return

        filter_phrase = command_args.joined_args_lower
        filtered_df = quran_df[quran_df['text'].str.lower().str.contains(filter_phrase)]
        filtered_df = filtered_df.sample(frac=1)

        response = ''
        if 'chapter' in command_args.named_args:
            books = quran_df['chapter_name'].unique()
            matched_chapter_name = core_utils.match_substr_to_list_of_texts(command_args.named_args['chapter'], books)

            if matched_chapter_name is not None:
                filtered_df = filtered_df[filtered_df['chapter_name'] == matched_chapter_name]

            if matched_chapter_name is None:
                response += f'[Sura {filtered_df.iloc[0]['chapter_nr']}]. {filtered_df.iloc[0]['chapter_name']}, '

        response, error = self.handle_holy_text_named_params(command_args, filtered_df, quran_df, bot_state, filter_phrase, HolyTextType.QURAN)
        if error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=error)
            return

        await context.bot.send_message(chat_id=update.effective_chat.id, text=response)


    def handle_holy_text_named_params(self, command_args, filtered_df, raw_df, bot_state, filter_phrase, holy_text_type):
        error = ''
        last_verse_id = bot_state.last_bible_verse_id if holy_text_type == HolyTextType.BIBLE else bot_state.last_quran_verse_id
        if filtered_df.empty:
            response = 'Nie ma takiego wersetu. Beduinom pustynnym weszło post-nut clarity po wyruchaniu kozy. :('
        elif 'num' in command_args.named_args:
            df = filtered_df.head(command_args.named_args['num'])
            response = core_utils.display_holy_text_df(df, bot_state, holy_text_type, label=f'{len(df)} {holy_text_type.value} verses with "{filter_phrase}"')
        elif 'all' in command_args.named_args:
            response = core_utils.display_holy_text_df(filtered_df, bot_state, holy_text_type, label=f'{len(filtered_df)} {holy_text_type.value} verses with "{filter_phrase}"')
        elif 'prev' in command_args.named_args and last_verse_id != -1:
            start_index = max(0, last_verse_id - command_args.named_args['prev'])
            filtered_df = raw_df.iloc[start_index:last_verse_id]
            label = f'{command_args.named_args['prev']} {holy_text_type.value} verses before {core_utils.get_siglum(raw_df.iloc[last_verse_id], holy_text_type, SiglumType.FULL)}'
            response = core_utils.display_holy_text_df(filtered_df, bot_state, holy_text_type, label=label, show_siglum=False)
        elif 'next' in command_args.named_args and last_verse_id != -1:
            end_index = min(len(filtered_df), last_verse_id + command_args.named_args['next'] + 1)
            filtered_df = raw_df.iloc[last_verse_id + 1:end_index]
            label = f'{command_args.named_args['next']} {holy_text_type.value} verses after {core_utils.get_siglum(raw_df.iloc[last_verse_id], holy_text_type, SiglumType.FULL)}'
            response = core_utils.display_holy_text_df(filtered_df, bot_state, holy_text_type, label=label, show_siglum=False)
        elif 'count' in command_args.named_args:
            random_row = filtered_df.iloc[0]
            self.bot_state.set_holy_text_last_verse_id(random_row.name, holy_text_type)
            response = f'{len(filtered_df)} {holy_text_type.value} verses with "{filter_phrase}": \n\n'
            response += f'[{core_utils.get_siglum(random_row, holy_text_type, SiglumType.SHORT)}] {random_row["text"]}'
        elif 'verse' in command_args.named_args:
            response, error = core_utils.parse_quran_verse_arg(command_args.named_args['verse'], bot_state, holy_text_type)
        else:
            random_row = filtered_df.iloc[0]
            self.bot_state.set_holy_text_last_verse_id(random_row.name, holy_text_type)
            response = f"[{core_utils.get_siglum(random_row, holy_text_type, SiglumType.SHORT)}] {random_row['text']}"

        return response, error


    async def cmd_bible_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
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

    async def cmd_show_shopping_sundays(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(args=context.args, available_named_args={'all': ArgType.NONE})
        command_args = core_utils.parse_args(self.users_df, command_args)
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

    async def cmd_remind_me(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(args=context.args, expected_args=[ArgType.PERIOD, ArgType.TEXT_MULTISPACED], optional=[False, False], min_string_length=1, max_string_length=1000)
        command_args = core_utils.parse_args(self.users_df, command_args)
        if command_args.error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error)
            return

        dt, error = core_utils.period_offset_to_dt(command_args)
        if error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=error)
            return

        self.job_persistance.save_job(job_queue=context.job_queue, dt=dt, func=core_utils.e, args=[update.effective_chat.id, update.message.message_id, command_args.string])
        response = f"You're gonna get pinged at {core_utils.dt_to_pretty_str(dt)}."
        await context.bot.send_message(chat_id=update.effective_chat.id, text=response)

    async def cmd_kiepscy(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(args=context.args, expected_args=[ArgType.TEXT_MULTISPACED], min_string_length=1, max_string_length=1000)
        command_args = core_utils.parse_args(self.users_df, command_args)
        if command_args.error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error)
            return

        search_phrase = command_args.string
        search_phrases = command_args.strings

        if search_phrases:  # use & operator to match multiple words
            regex = core_utils.regexify_multiword_filter(search_phrases)
            matching_by_title_df = kiepscy_df[kiepscy_df['title'].str.contains(regex, case=False)]
            matching_by_description_df = kiepscy_df[kiepscy_df['description'].str.contains(regex, case=False)]
        else:
            matching_by_title_df = kiepscy_df[kiepscy_df['title'].str.contains(search_phrase, case=False)]
            matching_by_description_df = kiepscy_df[kiepscy_df['description'].str.contains(search_phrase, case=False)]
        merged_df = pd.concat([matching_by_title_df, matching_by_description_df], ignore_index=True)
        merged_df['nr'] = merged_df['nr'].replace('—', '999').astype(int)
        merged_df = merged_df.drop_duplicates('nr').sort_values('nr').reset_index(drop=True)

        text = f"Kiepscy episodes that match [{search_phrase}]:\n"
        last_text = text
        for i, (index, row) in enumerate(merged_df.iterrows()):
            text += f"- *{row['nr']}: {row['title']}* - {row['description']}\n"
            if len(text) > 4096:
                response = stats_utils.escape_special_characters(last_text)
                await context.bot.send_message(chat_id=update.effective_chat.id, text=response, parse_mode=telegram.constants.ParseMode.MARKDOWN_V2)
                text = ""
            last_text = text
        text = stats_utils.escape_special_characters(text)
        await context.bot.send_message(chat_id=update.effective_chat.id, text=text, parse_mode=telegram.constants.ParseMode.MARKDOWN_V2)

    async def cmd_kiepscyurl(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(args=context.args, expected_args=[ArgType.POSITIVE_INT], number_limit=1000)
        command_args = core_utils.parse_args(self.users_df, command_args)
        if command_args.error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error)
            return

        episode_nr = str(command_args.number)
        matching_episode_df = kiepscy_df[kiepscy_df['nr'] == episode_nr]
        if matching_episode_df.empty:
            await context.bot.send_message(chat_id=update.effective_chat.id, text=f"Nie ma takiego epizodu :(")
            return
        row = matching_episode_df.iloc[0]
        text = f"*{episode_nr}: {row['title']}* - {row['url']}"

        text = stats_utils.escape_special_characters(text)
        await context.bot.send_message(chat_id=update.effective_chat.id, text=text, parse_mode=telegram.constants.ParseMode.MARKDOWN_V2)
