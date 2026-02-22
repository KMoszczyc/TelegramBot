import logging
from datetime import datetime

import pandas as pd
import telegram
from telegram import Update
from telegram.ext import ContextTypes

import src.core.utils as core_utils
import src.stats.utils as stats_utils
from src.config.assets import Assets
from src.config.constants import LONG_MESSAGE_LIMIT
from src.config.enums import ArgType, ErrorMessage, HolyTextType, SiglumType, Table
from src.core.command_logger import CommandLogger
from src.core.job_persistance import JobPersistance
from src.models.bot_state import BotState
from src.models.command_args import CommandArgs
from src.models.db.db import DB

log = logging.getLogger(__name__)


class Commands:
    def __init__(self, command_logger: CommandLogger, job_persistance: JobPersistance, bot_state: BotState, db: DB, assets: Assets):
        self.command_logger = command_logger
        self.job_persistance = job_persistance
        self.bot_state = bot_state
        self.db = db
        self.assets = assets
        self.users_df = self.db.load_table(Table.USERS)
        self.users_map = stats_utils.get_users_map(self.users_df)

    async def cmd_all(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        usernames = self.users_df["final_username"].tolist()
        user_ids = self.users_df.index.tolist()

        text = ""
        for username, user_id in zip(usernames, user_ids, strict=False):
            text += f"[{username}](tg://user?id={user_id}) "

        await context.bot.send_message(
            chat_id=update.effective_chat.id, text=text, parse_mode="Markdown", message_thread_id=update.message.message_thread_id
        )

    async def cmd_ozjasz(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(args=context.args, phrases=self.assets.ozjasz_phrases, is_text_arg=True)
        filtered_phrases, command_args = core_utils.preprocess_input(self.users_df, command_args)
        if command_args.error != "":
            await context.bot.send_message(
                chat_id=update.effective_chat.id, text=command_args.error, message_thread_id=update.message.message_thread_id
            )
            return
        log.info(update.message)

        response = core_utils.select_random_phrase(filtered_phrases, ErrorMessage.NO_SUCH_PHRASE)
        await context.bot.send_message(chat_id=update.effective_chat.id, text=response, message_thread_id=update.message.message_thread_id)

    async def cmd_boczek(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(args=context.args, expected_args=[ArgType.TEXT_MULTISPACED])
        command_args = core_utils.parse_args(self.users_df, command_args)
        if command_args.error != "":
            await context.bot.send_message(
                chat_id=update.effective_chat.id, text=command_args.error, message_thread_id=update.message.message_thread_id
            )
            return

        curse = core_utils.select_random_phrase(self.assets.boczek_phrases, ErrorMessage.NO_SUCH_PHRASE)
        response = f"{command_args.string} to {curse}" if command_args.string != "" else curse
        await context.bot.send_message(chat_id=update.effective_chat.id, text=response, message_thread_id=update.message.message_thread_id)

    async def cmd_europejskafirma(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(args=context.args, phrases=self.assets.europejskafirma_phrases, is_text_arg=True)
        filtered_phrases, command_args = core_utils.preprocess_input(self.users_df, command_args)
        if command_args.error != "":
            await context.bot.send_message(
                chat_id=update.effective_chat.id, text=command_args.error, message_thread_id=update.message.message_thread_id
            )
            return

        response = core_utils.select_random_phrase(filtered_phrases, ErrorMessage.NO_SUCH_PHRASE)
        await context.bot.send_message(chat_id=update.effective_chat.id, text=response, message_thread_id=update.message.message_thread_id)

    async def cmd_bartosiak(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(args=context.args, phrases=self.assets.bartosiak_phrases, is_text_arg=True)
        filtered_phrases, command_args = core_utils.preprocess_input(self.users_df, command_args)
        if command_args.error != "":
            await context.bot.send_message(
                chat_id=update.effective_chat.id, text=command_args.error, message_thread_id=update.message.message_thread_id
            )
            return

        response = core_utils.select_random_phrase(filtered_phrases, ErrorMessage.NO_SUCH_PHRASE)
        await context.bot.send_message(chat_id=update.effective_chat.id, text=response, message_thread_id=update.message.message_thread_id)

    async def cmd_tvp(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        merged_headlines = self.assets.tvp_latest_headlines + self.assets.tvp_headlines
        command_args = CommandArgs(args=context.args, phrases=merged_headlines, is_text_arg=True)
        filtered_phrases, command_args = core_utils.preprocess_input(self.users_df, command_args)
        if command_args.error != "":
            await context.bot.send_message(
                chat_id=update.effective_chat.id, text=command_args.error, message_thread_id=update.message.message_thread_id
            )
            return

        response = core_utils.select_random_phrase(filtered_phrases, ErrorMessage.NO_SUCH_HEADLINE)

        await context.bot.send_message(chat_id=update.effective_chat.id, text=response, message_thread_id=update.message.message_thread_id)

    async def cmd_tvp_latest(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(args=context.args, phrases=self.assets.tvp_latest_headlines, is_text_arg=True)
        filtered_phrases, command_args = core_utils.preprocess_input(self.users_df, command_args)
        if command_args.error != "":
            await context.bot.send_message(
                chat_id=update.effective_chat.id, text=command_args.error, message_thread_id=update.message.message_thread_id
            )
            return

        response = core_utils.select_random_phrase(filtered_phrases, ErrorMessage.NO_SUCH_HEADLINE)
        await context.bot.send_message(chat_id=update.effective_chat.id, text=response, message_thread_id=update.message.message_thread_id)

    async def cmd_tusk(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        tusk_headlines = [headline for headline in self.assets.tvp_headlines if "tusk" in headline.lower()]
        command_args = CommandArgs(args=context.args, phrases=tusk_headlines, is_text_arg=True)
        filtered_phrases, command_args = core_utils.preprocess_input(self.users_df, command_args)
        if command_args.error != "":
            await context.bot.send_message(
                chat_id=update.effective_chat.id, text=command_args.error, message_thread_id=update.message.message_thread_id
            )
            return

        response = core_utils.select_random_phrase(filtered_phrases, ErrorMessage.NO_SUCH_HEADLINE)
        await context.bot.send_message(chat_id=update.effective_chat.id, text=response, message_thread_id=update.message.message_thread_id)

    async def cmd_walesa(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(args=context.args, phrases=self.assets.walesa_phrases, is_text_arg=True)
        filtered_phrases, command_args = core_utils.preprocess_input(self.users_df, command_args)
        if command_args.error != "":
            await context.bot.send_message(
                chat_id=update.effective_chat.id, text=command_args.error, message_thread_id=update.message.message_thread_id
            )
            return

        response = core_utils.select_random_phrase(filtered_phrases, ErrorMessage.NO_SUCH_ITEM).replace(r"\n", "\n")
        await context.bot.send_message(chat_id=update.effective_chat.id, text=response, message_thread_id=update.message.message_thread_id)

    async def cmd_are_you_lucky_today(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(args=context.args, is_text_arg=True)
        command_args = core_utils.parse_args(self.users_df, command_args)

        args_provided = command_args.joined_args != ""
        lucky_text_number = core_utils.text_to_number(command_args.joined_args) if command_args.joined_args != "" else 0
        user_id = update.effective_user.id
        lucky_input = user_id + lucky_text_number

        _, response = core_utils.are_you_lucky(lucky_input, args_provided)
        await context.bot.send_message(chat_id=update.effective_chat.id, text=response, message_thread_id=update.message.message_thread_id)

    async def cmd_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_infos = self.assets.commands + self.assets.arguments_help
        command_args = CommandArgs(args=context.args, phrases=command_infos, is_text_arg=True)
        filtered_commands, command_args = core_utils.preprocess_input(self.users_df, command_args)
        if command_args.error != "":
            await context.bot.send_message(
                chat_id=update.effective_chat.id, text=command_args.error, message_thread_id=update.message.message_thread_id
            )
            return

        if command_args.joined_args != "" and filtered_commands:
            text = "Existing commands:\n- /" + "\n- /".join(filtered_commands)
        else:
            text = "Existing commands:\n- /" + "\n- /".join(self.assets.commands)
            text += "\n\n *Arguments*:\n" + "\n".join(self.assets.arguments_help)

        text = stats_utils.escape_special_characters(text)
        while len(text) > 1:
            index = min(4096, len(text))
            message = text[:index]
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=message,
                parse_mode=telegram.constants.ParseMode.MARKDOWN_V2,
                message_thread_id=update.message.message_thread_id,
            )
            text = text[index:]

    async def cmd_bible(self, update: Update, context: ContextTypes.DEFAULT_TYPE, bot_state: BotState):
        command_args = CommandArgs(
            args=context.args,
            is_text_arg=True,
            available_named_args={
                "prev": ArgType.POSITIVE_INT,
                "next": ArgType.POSITIVE_INT,
                "all": ArgType.NONE,
                "num": ArgType.POSITIVE_INT,
                "count": ArgType.NONE,
                "book": ArgType.STRING,
                "chapter": ArgType.POSITIVE_INT,
            },
            available_named_args_aliases={"p": "prev", "n": "next", "a": "all", "c": "count", "b": "book", "ch": "chapter"},
        )
        command_args = core_utils.parse_args(self.users_df, command_args)
        if command_args.error != "":
            await context.bot.send_message(
                chat_id=update.effective_chat.id, text=command_args.error, message_thread_id=update.message.message_thread_id
            )
            return

        filter_phrase = command_args.joined_args_lower
        bible_df = self.assets.bible_df
        filtered_df = bible_df[bible_df["text"].str.lower().str.contains(filter_phrase)]

        response = ""
        if "book" in command_args.named_args:
            abbreviation = bible_df["abbreviation"].unique()
            books = bible_df["book"].unique()
            matched_abbreviation = core_utils.match_substr_to_list_of_texts(command_args.named_args["book"], abbreviation)
            matched_book_name = core_utils.match_substr_to_list_of_texts(command_args.named_args["book"], books)

            if matched_abbreviation is not None:
                filtered_df = filtered_df[filtered_df["abbreviation"] == matched_abbreviation]

            if matched_book_name is not None:
                filtered_df = filtered_df[filtered_df["book"] == matched_book_name]

            if matched_book_name is None or matched_abbreviation is None:
                response += f"[{filtered_df.iloc[0]['abbreviation']}] {filtered_df.iloc[0]['book']}, "

        if "chapter" in command_args.named_args:  # in chapter mode we read a random chapter from the bible
            random_row = filtered_df.sample(frac=1).iloc[0]
            book = random_row["book"]
            chapter = random_row["chapter"]
            filtered_df = bible_df[(bible_df["book"] == book) & (bible_df["chapter"] == chapter)].head(command_args.named_args["chapter"])
            response += core_utils.display_holy_text_df(
                filtered_df, bot_state, HolyTextType.BIBLE, label=f"{len(filtered_df)} verses from chapter {chapter}", show_siglum=False
            )
            await context.bot.send_message(
                chat_id=update.effective_chat.id, text=response, message_thread_id=update.message.message_thread_id
            )
            return
        else:
            filtered_df = filtered_df.sample(frac=1)

        response, error = self.handle_holy_text_named_params(
            command_args, filtered_df, bible_df, bot_state, filter_phrase, HolyTextType.BIBLE
        )
        if error != "":
            await context.bot.send_message(chat_id=update.effective_chat.id, text=error, message_thread_id=update.message.message_thread_id)
            return

        await context.bot.send_message(chat_id=update.effective_chat.id, text=response, message_thread_id=update.message.message_thread_id)

    async def cmd_quran(self, update: Update, context: ContextTypes.DEFAULT_TYPE, bot_state: BotState):
        command_args = CommandArgs(
            args=context.args,
            is_text_arg=True,
            available_named_args={
                "prev": ArgType.POSITIVE_INT,
                "next": ArgType.POSITIVE_INT,
                "all": ArgType.NONE,
                "num": ArgType.POSITIVE_INT,
                "count": ArgType.NONE,
                "chapter": ArgType.STRING,
                "verse": ArgType.STRING,
            },
            available_named_args_aliases={"p": "prev", "n": "next", "a": "all", "c": "count", "ch": "chapter", "num": "num", "v": "verse"},
        )
        quran_df = self.assets.quran_df
        command_args = core_utils.parse_args(self.users_df, command_args)
        if command_args.error != "":
            await context.bot.send_message(
                chat_id=update.effective_chat.id, text=command_args.error, message_thread_id=update.message.message_thread_id
            )
            return

        filter_phrase = command_args.joined_args_lower
        filtered_df = quran_df[quran_df["text"].str.lower().str.contains(filter_phrase)]
        filtered_df = filtered_df.sample(frac=1)

        response = ""
        if "chapter" in command_args.named_args:
            books = quran_df["chapter_name"].unique()
            matched_chapter_name = core_utils.match_substr_to_list_of_texts(command_args.named_args["chapter"], books)

            if matched_chapter_name is not None:
                filtered_df = filtered_df[filtered_df["chapter_name"] == matched_chapter_name]

            if matched_chapter_name is None:
                response += f"[Sura {filtered_df.iloc[0]['chapter_nr']}]. {filtered_df.iloc[0]['chapter_name']}, "

        response, error = self.handle_holy_text_named_params(
            command_args, filtered_df, quran_df, bot_state, filter_phrase, HolyTextType.QURAN
        )
        if error != "":
            await context.bot.send_message(chat_id=update.effective_chat.id, text=error, message_thread_id=update.message.message_thread_id)
            return

        await context.bot.send_message(chat_id=update.effective_chat.id, text=response, message_thread_id=update.message.message_thread_id)

    def handle_holy_text_named_params(self, command_args, filtered_df, raw_df, bot_state, filter_phrase, holy_text_type):
        error = ""
        last_verse_id = bot_state.last_bible_verse_id if holy_text_type == HolyTextType.BIBLE else bot_state.last_quran_verse_id
        if filtered_df.empty:
            response = ErrorMessage.NO_SUCH_VERSE
        elif "num" in command_args.named_args:
            df = filtered_df.head(command_args.named_args["num"])
            response = core_utils.display_holy_text_df(
                df, bot_state, holy_text_type, label=f'{len(df)} {holy_text_type.value} verses with "{filter_phrase}"'
            )
        elif "all" in command_args.named_args:
            response = core_utils.display_holy_text_df(
                filtered_df, bot_state, holy_text_type, label=f'{len(filtered_df)} {holy_text_type.value} verses with "{filter_phrase}"'
            )
        elif "prev" in command_args.named_args and last_verse_id != -1:
            start_index = max(0, last_verse_id - command_args.named_args["prev"])
            filtered_df = raw_df.iloc[start_index:last_verse_id]
            label = f"{command_args.named_args['prev']} {holy_text_type.value} verses before {core_utils.get_siglum(raw_df.iloc[last_verse_id], holy_text_type, SiglumType.FULL)}"
            response = core_utils.display_holy_text_df(filtered_df, bot_state, holy_text_type, label=label, show_siglum=False)
        elif "next" in command_args.named_args and last_verse_id != -1:
            end_index = min(len(filtered_df), last_verse_id + command_args.named_args["next"] + 1)
            filtered_df = raw_df.iloc[last_verse_id + 1 : end_index]
            label = f"{command_args.named_args['next']} {holy_text_type.value} verses after {core_utils.get_siglum(raw_df.iloc[last_verse_id], holy_text_type, SiglumType.FULL)}"
            response = core_utils.display_holy_text_df(filtered_df, bot_state, holy_text_type, label=label, show_siglum=False)
        elif "count" in command_args.named_args:
            random_row = filtered_df.iloc[0]
            self.bot_state.set_holy_text_last_verse_id(random_row.name, holy_text_type)
            response = f'{len(filtered_df)} {holy_text_type.value} verses with "{filter_phrase}": \n\n'
            response += f"[{core_utils.get_siglum(random_row, holy_text_type, SiglumType.SHORT)}] {random_row['text']}"
        elif "verse" in command_args.named_args:
            response, error = core_utils.parse_quran_verse_arg(raw_df, command_args.named_args["verse"], bot_state, holy_text_type)
        else:
            random_row = filtered_df.iloc[0]
            self.bot_state.set_holy_text_last_verse_id(random_row.name, holy_text_type)
            response = f"[{core_utils.get_siglum(random_row, holy_text_type, SiglumType.SHORT)}] {random_row['text']}"

        return response, error

    async def cmd_bible_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        bible_df = self.assets.bible_df
        bible_stats_df = bible_df.drop_duplicates("book")[["book", "abbreviation"]].set_index("abbreviation")
        bible_stats_df["chapter_count"] = (
            bible_df.drop_duplicates(["abbreviation", "chapter"])[["abbreviation", "chapter"]]
            .set_index("abbreviation")
            .groupby("abbreviation")
            .size()
        )
        bible_stats_df["verse_count"] = bible_df.groupby(["abbreviation"]).size()

        bible_stats_df = bible_stats_df.sort_values(by="verse_count", ascending=False)

        text = "``` Bible stats:\n"
        text += "Book".ljust(28) + "Chapters Verses"
        for index, row in bible_stats_df.iterrows():
            text += f"\n[{index}] {row['book']}:".ljust(36) + f"{row['chapter_count']}".ljust(4) + f"{row['verse_count']}"

        text += "```"
        text = stats_utils.escape_special_characters(text)
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=text,
            parse_mode=telegram.constants.ParseMode.MARKDOWN_V2,
            message_thread_id=update.message.message_thread_id,
        )

    async def cmd_show_shopping_sundays(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(args=context.args, available_named_args={"all": ArgType.NONE})
        command_args = core_utils.parse_args(self.users_df, command_args)
        if command_args.error != "":
            await context.bot.send_message(
                chat_id=update.effective_chat.id, text=command_args.error, message_thread_id=update.message.message_thread_id
            )
            return

        dt_now = datetime.now().date()
        sundays_dt = [datetime.strptime(date, "%d-%m-%Y").date() for date in self.assets.shopping_sundays]
        filtered_shopping_sundays = [sunday for sunday in sundays_dt if sunday >= dt_now]
        next_shopping_sunday = filtered_shopping_sundays[0] if filtered_shopping_sundays else None
        if "all" in command_args.named_args:
            response = f"Wszystkie handlowe niedziele w {dt_now.year}:\n - " + "\n - ".join(
                [core_utils.display_shopping_sunday(sunday) for sunday in sundays_dt]
            )
        elif next_shopping_sunday == dt_now:
            response = "Dziś niedziela handlowa! Zapylaj do lidla po wege kiełbaski czy do aldi po lampke na promocji!"
        elif next_shopping_sunday:
            response = f"Kolejna handlowa niedziela jest: {core_utils.display_shopping_sunday(filtered_shopping_sundays[0])}"
        else:
            response = "Nie ma już handlowych niedzieli w tym roku :(("

        await context.bot.send_message(chat_id=update.effective_chat.id, text=response, message_thread_id=update.message.message_thread_id)

    async def cmd_remind_me(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(
            args=context.args,
            expected_args=[ArgType.PERIOD, ArgType.TEXT_MULTISPACED],
            optional=[False, False],
            min_string_length=1,
            max_string_length=1000,
        )
        command_args = core_utils.parse_args(self.users_df, command_args)
        if command_args.error != "":
            await context.bot.send_message(
                chat_id=update.effective_chat.id, text=command_args.error, message_thread_id=update.message.message_thread_id
            )
            return

        dt, error = core_utils.period_offset_to_dt(command_args)
        if error != "":
            await context.bot.send_message(chat_id=update.effective_chat.id, text=error, message_thread_id=update.message.message_thread_id)
            return

        self.job_persistance.save_job(
            job_queue=context.job_queue,
            dt=dt,
            func=core_utils.send_response_message,
            args=[update.effective_chat.id, update.message.message_id, command_args.string],
        )
        response = f"You're gonna get pinged at {core_utils.dt_to_pretty_str(dt)}."
        await context.bot.send_message(chat_id=update.effective_chat.id, text=response, message_thread_id=update.message.message_thread_id)

    async def cmd_kiepscy(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(args=context.args, expected_args=[ArgType.TEXT_MULTISPACED], min_string_length=1, max_string_length=1000)
        command_args = core_utils.parse_args(self.users_df, command_args)
        kiepscy_df = self.assets.kiepscy_df

        # await context.bot.send_message(chat_id=update.effective_chat.id, text='Temporarily disabled :(')
        # return
        if command_args.error != "":
            await context.bot.send_message(
                chat_id=update.effective_chat.id, text=command_args.error, message_thread_id=update.message.message_thread_id
            )
            return

        search_phrase = command_args.string
        search_phrases = command_args.strings

        if search_phrases:  # use & operator to match multiple words
            regex = core_utils.regexify_multiword_filter(search_phrases)
            matching_by_title_df = kiepscy_df[kiepscy_df["title"].str.contains(regex, case=False)]
            matching_by_description_df = kiepscy_df[kiepscy_df["description"].str.contains(regex, case=False)]
        else:
            matching_by_title_df = kiepscy_df[kiepscy_df["title"].str.contains(search_phrase, case=False)]
            matching_by_description_df = kiepscy_df[kiepscy_df["description"].str.contains(search_phrase, case=False)]
        merged_df = pd.concat([matching_by_title_df, matching_by_description_df], ignore_index=True)
        merged_df["nr"] = merged_df["nr"].replace("—", "999").astype(int)
        merged_df = merged_df.drop_duplicates("nr").sort_values("nr").reset_index(drop=True)

        if search_phrase == "" and not search_phrases:
            random_row = merged_df.sample(frac=1).iloc[1]
            text = f"*{random_row['nr']}: {random_row['title']}* - {random_row['description']}\n"
            text = stats_utils.escape_special_characters(text)
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=text,
                parse_mode=telegram.constants.ParseMode.MARKDOWN_V2,
                message_thread_id=update.message.message_thread_id,
            )
            return

        text = f"Kiepscy episodes that match [{search_phrase}]:\n"
        last_text = text
        message_sent_count = 0
        for _, row in merged_df.iterrows():
            text += f"- *{row['nr']}: {row['title']}* - {row['description']}\n"
            if message_sent_count >= LONG_MESSAGE_LIMIT:
                return
            if len(text) > 4096:
                response = stats_utils.escape_special_characters(last_text)
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=response,
                    parse_mode=telegram.constants.ParseMode.MARKDOWN_V2,
                    message_thread_id=update.message.message_thread_id,
                )
                text = ""
                message_sent_count += 1
            last_text = text
        text = stats_utils.escape_special_characters(text)
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=text,
            parse_mode=telegram.constants.ParseMode.MARKDOWN_V2,
            message_thread_id=update.message.message_thread_id,
        )

    async def cmd_kiepscyurl(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(args=context.args, expected_args=[ArgType.POSITIVE_INT], max_number=1000)
        command_args = core_utils.parse_args(self.users_df, command_args)
        kiepscy_df = self.assets.kiepscy_df
        if command_args.error != "":
            await context.bot.send_message(
                chat_id=update.effective_chat.id, text=command_args.error, message_thread_id=update.message.message_thread_id
            )
            return

        episode_nr = str(command_args.number)
        matching_episode_df = kiepscy_df[kiepscy_df["nr"] == episode_nr]
        if matching_episode_df.empty:
            await context.bot.send_message(
                chat_id=update.effective_chat.id, text=ErrorMessage.NO_SUCH_EPISODE, message_thread_id=update.message.message_thread_id
            )
            return
        row = matching_episode_df.iloc[0]
        text = f"*{episode_nr}: {row['title']}* - {row['url']}"

        text = stats_utils.escape_special_characters(text)
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=text,
            parse_mode=telegram.constants.ParseMode.MARKDOWN_V2,
            message_thread_id=update.message.message_thread_id,
        )
