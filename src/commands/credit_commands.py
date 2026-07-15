import asyncio
import copy
import logging

import telegram
from telegram import InlineKeyboardButton, Update
from telegram.ext import ContextTypes

import src.core.utils as core_utils
import src.stats.utils as stats_utils
from src.config.assets import Assets
from src.config.constants import (
    CRITICAL_FAILURE_CHANCE,
    CRITICAL_SUCCESS_CHANCE,
    FLAG_QUIZ_TIMEOUT_SECONDS,
    MAP_QUIZ_TIMEOUT_SECONDS,
    MIN_QUIZ_TIME_TO_ANSWER_SECONDS,
    TOURNAMENT_BET_TIMEOUT_SECONDS,
    TOURNAMENT_DEFAULT_ROUNDS,
    TOURNAMENT_JOIN_TIMEOUT_SECONDS,
    TOURNAMENT_MAX_ROUNDS,
    TOURNAMENT_SPIN_DELAY_SECONDS,
)
from src.config.enums import (
    BET_EVENTS,
    QUIZ_EVENTS,
    STEAL_EVENTS,
    ArgType,
    CreditActionType,
    MessageType,
    Table,
    TournamentState,
    TournamentType,
)
from src.core.command_logger import CommandLogger
from src.core.job_persistance import JobPersistance
from src.models.bot_state import BotState
from src.models.command_args import CommandArgs
from src.models.credits import Credits
from src.models.db.db import DB
from src.models.event_manager import EventManager
from src.models.flag_quiz import FlagQuiz
from src.models.map_quiz import MapQuiz
from src.models.quiz_model import QuizModel
from src.models.roulette import Roulette
from src.models.roulette_tournament import RouletteTournament
from src.stats import charts

log = logging.getLogger(__name__)


class CreditCommands:
    def __init__(
        self, command_logger: CommandLogger, job_persistance: JobPersistance, bot_state: BotState, credits: Credits, db: DB, assets: Assets
    ):
        self.command_logger = command_logger
        self.job_persistance = job_persistance
        self.bot_state = bot_state
        self.credits = credits
        self.db = db
        self.assets = assets
        self.users_df = self.db.load_table(Table.USERS)
        self.users_map = stats_utils.get_users_map(self.users_df)
        self.roulette = Roulette(self.credits)
        self.event_manager = EventManager()

        for event in STEAL_EVENTS:
            self.event_manager.add_event("steal", event)
        for event in BET_EVENTS:
            self.event_manager.add_event("bet", event)
        for event in QUIZ_EVENTS:
            self.event_manager.add_event("quiz", event)

        bot_state.init_quiz_map(self.users_df)
        self.active_tournaments: dict[int, RouletteTournament] = {}

    async def cmd_get_credits(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        can_get_credits = self.bot_state.update_get_credits_limits(user_id)
        if not can_get_credits:
            message = stats_utils.escape_special_characters("You already got your credits today :)")
            await core_utils.send_message(update, context, MessageType.MARKDOWN_TEXT, message)
            return

        message = self.credits.get_daily_credits(user_id)
        await core_utils.send_message(update, context, MessageType.MARKDOWN_TEXT, message)

    async def cmd_show_credit_leaderboard(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        message = self.credits.show_credit_leaderboard(self.users_map)
        await core_utils.send_message(update, context, MessageType.MARKDOWN_TEXT, message)

    async def cmd_show_top_bet_leaderboard(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(args=context.args, expected_args=[ArgType.USER, ArgType.PERIOD], optional=[True, True])
        command_args = core_utils.parse_args(self.users_df, command_args)
        if command_args.error != "":
            await core_utils.send_message(update, context, MessageType.TEXT, command_args.error)
            return

        message = self.credits.show_top_bet_leaderboard(self.users_map, command_args)
        await core_utils.send_message(update, context, MessageType.MARKDOWN_TEXT, message)

    async def cmd_show_steal_leaderboard(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(args=context.args, expected_args=[ArgType.USER, ArgType.PERIOD], optional=[True, True])
        command_args = core_utils.parse_args(self.users_df, command_args)
        if command_args.error != "":
            await core_utils.send_message(update, context, MessageType.TEXT, command_args.error)
            return

        message = self.credits.show_steal_leaderboard(self.users_map, command_args)
        await core_utils.send_message(update, context, MessageType.MARKDOWN_TEXT, message)

    async def cmd_bet(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(
            args=context.args,
            expected_args=[ArgType.TEXT, ArgType.POSITIVE_INT],
            optional=[False, False],
            min_number=1,
            max_number=10000000,
            max_string_length=1000,
        )
        command_args = core_utils.parse_args(self.users_df, command_args)
        if command_args.error != "":
            await core_utils.send_message(update, context, MessageType.TEXT, command_args.error)
            return

        bet_size = command_args.number
        bet_type_arg = command_args.string

        message = stats_utils.escape_special_characters("The roulette is spinning...")
        await core_utils.send_message(update, context, MessageType.MARKDOWN_TEXT, message)
        await asyncio.sleep(5)

        message, success = self.roulette.play(update.effective_user.id, bet_size, bet_type_arg)
        if success and core_utils.roll(CRITICAL_SUCCESS_CHANCE) and (event := self.event_manager.get_random_event("bet", "success")):
            effect = event.apply_effect(amount=bet_size)
            self.credits.update_credits(update.effective_user.id, effect.credit_change, CreditActionType.BET)
            message += f"\n\n**Critical Success!** {effect.message}"
        elif not success and core_utils.roll(CRITICAL_FAILURE_CHANCE) and (event := self.event_manager.get_random_event("bet", "failure")):
            effect = event.apply_effect(amount=bet_size)
            self.credits.update_credits(update.effective_user.id, effect.credit_change, CreditActionType.BET)
            message += f"\n\n**Critical Failure!** {effect.message}"

        message = stats_utils.escape_special_characters(message)
        await core_utils.send_message(update, context, MessageType.MARKDOWN_TEXT, message)

    async def cmd_steal_credits(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(args=context.args, expected_args=[ArgType.USER, ArgType.POSITIVE_INT], min_number=1, max_number=10000000)
        command_args = core_utils.parse_args(self.users_df, command_args)
        if command_args.error != "":
            await core_utils.send_message(update, context, MessageType.TEXT, command_args.error)
            return

        user_id = update.effective_user.id
        if user_id == command_args.user_id:
            message = "You want to steal from yourself? lol xd"
            await core_utils.send_message(update, context, MessageType.TEXT, message)
            return

        can_steal = self.bot_state.update_steal_credits_limits(user_id)
        if not can_steal:
            message = stats_utils.escape_special_characters("You have reached your daily steal quota, no more thieving today :(")
            await core_utils.send_message(update, context, MessageType.MARKDOWN_TEXT, message)
            return

        success, message = self.credits.validate_steal(
            target_user_id=command_args.user_id, amount=command_args.number, users_map=self.users_map
        )
        if not success:
            await core_utils.send_message(update, context, MessageType.MARKDOWN_TEXT, message)
            return

        p = self.credits.calculate_steal_chance(target_user_id=command_args.user_id, amount=command_args.number)
        robbed_username = self.users_map[command_args.user_id]
        waiting_message = stats_utils.escape_special_characters(
            f"Attempting to steal *{command_args.number}* credits from *{robbed_username}* [*{p * 100:.1f}%* chance].."
        )
        await core_utils.send_message(update, context, MessageType.MARKDOWN_TEXT, waiting_message)
        await asyncio.sleep(5)

        message, success = self.credits.steal_credits(
            user_id=user_id, target_user_id=command_args.user_id, amount=command_args.number, users_map=self.users_map
        )
        if success and core_utils.roll(CRITICAL_SUCCESS_CHANCE) and (event := self.event_manager.get_random_event("steal", "success")):
            effect = event.apply_effect(amount=command_args.number)
            self.credits.update_credits(user_id, effect.credit_change, CreditActionType.STEAL)
            message += f"\n\n**Critical Success!** {effect.message}"
        elif (
            not success and core_utils.roll(CRITICAL_FAILURE_CHANCE) and (event := self.event_manager.get_random_event("steal", "failure"))
        ):
            effect = event.apply_effect(amount=command_args.number)
            self.credits.update_credits(user_id, effect.credit_change, CreditActionType.STEAL)
            message += f"\n\n**Critical Failure!** {effect.message}"

        message = stats_utils.escape_special_characters(message)
        await core_utils.send_message(update, context, MessageType.MARKDOWN_TEXT, message)

    async def cmd_gift_credits(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(args=context.args, expected_args=[ArgType.USER, ArgType.POSITIVE_INT], min_number=1, max_number=10000000)
        command_args = core_utils.parse_args(self.users_df, command_args)
        if command_args.error != "":
            await core_utils.send_message(update, context, MessageType.TEXT, command_args.error)
            return

        source_user_id = update.effective_user.id
        target_user_id = command_args.user_id
        if source_user_id == command_args.user_id:
            message = "You can't gift credits to yourself!"
            await core_utils.send_message(update, context, MessageType.TEXT, message)
            return

        result = self.credits.gift_credits(
            source_user_id=source_user_id, target_user_id=target_user_id, amount=command_args.number, users_map=self.users_map
        )
        message = stats_utils.escape_special_characters(result)
        await core_utils.send_message(update, context, MessageType.MARKDOWN_TEXT, message)

    async def cmd_quiz(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(
            args=context.args, available_named_args={"category": ArgType.STRING, "type": ArgType.STRING, "difficulty": ArgType.STRING}
        )
        command_args = core_utils.parse_args(self.users_df, command_args)
        if command_args.error != "":
            await core_utils.send_message(update, context, MessageType.TEXT, command_args.error)
            return

        filtered_quiz_df = copy.deepcopy(self.assets.quiz_df)
        if "category" in command_args.named_args:
            filtered_quiz_df = filtered_quiz_df[filtered_quiz_df["category"].str.contains(command_args.named_args["category"], case=False)]
        if "difficulty" in command_args.named_args:
            filtered_quiz_df = filtered_quiz_df[filtered_quiz_df["difficulty"].str.contains(command_args.named_args["difficulty"])]
        if "type" in command_args.named_args:
            filtered_quiz_df = filtered_quiz_df[filtered_quiz_df["type"].str.contains(command_args.named_args["type"])]

        if filtered_quiz_df.empty:
            message = "No questions in database with these parameters. :["
            await core_utils.send_message(update, context, MessageType.TEXT, message)
            return

        random_quiz_id = self.bot_state.get_random_quiz_id(filtered_quiz_df, update.effective_user.id)
        if random_quiz_id == -1:
            message = "There are questions in the database with these parameters, but you have already answered them."
            await core_utils.send_message(update, context, MessageType.TEXT, message)
            return

        random_quiz = filtered_quiz_df[filtered_quiz_df["quiz_id"] == random_quiz_id].iloc[0]
        buttons = []
        for answer in random_quiz["answers"]:
            buttons.append(InlineKeyboardButton(answer, callback_data=answer))

        buttons = [buttons] if random_quiz["type"] == "boolean" else [[buttons[0], buttons[1]], [buttons[2], buttons[3]]]
        reply_markup = telegram.InlineKeyboardMarkup(buttons)
        message = stats_utils.escape_special_characters(
            f"*[{random_quiz['category']}, {random_quiz['difficulty']}]*\n\n{random_quiz['question']}"
        )
        reply = await update.message.reply_text(
            message,
            reply_markup=reply_markup,
            parse_mode=telegram.constants.ParseMode.MARKDOWN_V2,
            message_thread_id=update.message.message_thread_id,
        )

        # Store quiz in cache for inline btn callbacks to work
        seconds_to_answer = MIN_QUIZ_TIME_TO_ANSWER_SECONDS + len(random_quiz["question"].split())
        quiz = QuizModel(
            quiz_id=random_quiz_id,
            user_id=update.effective_user.id,
            question=random_quiz["question"],
            difficulty=random_quiz["difficulty"],
            type=random_quiz["type"],
            correct_answer=random_quiz["correct_answer"],
            display_answer=random_quiz["display_answer"],
            start_dt=core_utils.get_dt_now(),
            seconds_to_answer=seconds_to_answer,
        )
        self.bot_state.quiz_cache[reply.message_id] = quiz

    async def btn_quiz_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Parses the CallbackQuery and updates the message text."""
        query = update.callback_query
        cached_quiz = self.bot_state.quiz_cache[query.message.message_id]

        if query.from_user.id != cached_quiz.user_id:  # prevent other people from answering someone else's quiz
            return
        await query.edit_message_reply_markup(reply_markup=None)  # remove answer buttons, as the quiz is over

        time_elapsed = (core_utils.get_dt_now() - cached_quiz.start_dt).total_seconds()
        if time_elapsed > cached_quiz.seconds_to_answer:
            message = stats_utils.escape_special_characters(
                f"Time's out! You've had *{int(cached_quiz.seconds_to_answer)}s* to answer, yet it took you *{time_elapsed:.2f}s*, lol."
            )
            await core_utils.send_message(update, context, MessageType.MARKDOWN_TEXT, message)
            return

        if cached_quiz.correct_answer != query.data:  # apply penalty for incorrect answer
            credit_penalty = cached_quiz.get_credit_penalty()
            user_credits, success = self.credits.update_credits(
                user_id=cached_quiz.user_id, credit_change=credit_penalty, action_type=CreditActionType.QUIZ
            )
            message = (
                f"Answer: *{query.data}* is incorrect. The correct answer was *{cached_quiz.correct_answer}*. You lose *{abs(credit_penalty)}* credits [*{user_credits}* left] :["
                if success
                else f"Answer: *{query.data}* is incorrect. The correct answer was *{cached_quiz.correct_answer}*, but because you're so poor you won't lose any credits for it :]"
            )
            if core_utils.roll(CRITICAL_FAILURE_CHANCE) and (event := self.event_manager.get_random_event("quiz", "failure")):
                effect = event.apply_effect(amount=credit_penalty)
                self.credits.update_credits(cached_quiz.user_id, effect.credit_change, CreditActionType.QUIZ)
                message += f"\n\n**Critical Failure!** {effect.message}"
            message = stats_utils.escape_special_characters(message)
            await core_utils.send_message(update, context, MessageType.MARKDOWN_TEXT, message)
            return

        # credit payout for correct answer :)
        credit_payout = cached_quiz.get_credit_payout()
        user_credits, _ = self.credits.update_credits(
            user_id=cached_quiz.user_id, credit_change=credit_payout, action_type=CreditActionType.QUIZ
        )
        message = f"Answer: *{query.data}* is correct! You receive *{credit_payout}* credits! [*{user_credits}* in total]"
        if core_utils.roll(CRITICAL_FAILURE_CHANCE) and (event := self.event_manager.get_random_event("quiz", "success")):
            effect = event.apply_effect(amount=credit_payout)
            self.credits.update_credits(cached_quiz.user_id, effect.credit_change, CreditActionType.QUIZ)
            message += f"\n\n**Critical Success!** {effect.message}"

        message = stats_utils.escape_special_characters(message)
        await core_utils.send_message(update, context, MessageType.MARKDOWN_TEXT, message)

    async def cmd_guess_person_on_a_map(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(
            args=context.args,
            available_named_args={"category": ArgType.STRING, "difficulty": ArgType.STRING, "extended_description": ArgType.NONE},
        )
        command_args = core_utils.parse_args(self.users_df, command_args)
        if command_args.error != "":
            await core_utils.send_message(update, context, MessageType.TEXT, command_args.error)
            return

        user_id = update.effective_user.id
        if user_id in self.bot_state.map_quiz_cache or user_id in self.bot_state.flag_quiz_cache:
            message = "You already have an active quiz! Answer it first or wait for the timeout."
            await core_utils.send_message(update, context, MessageType.TEXT, message)
            return

        famous_people_trivia_df = copy.deepcopy(self.assets.famous_people_trivia_df)
        if "category" in command_args.named_args:
            filtered_df = famous_people_trivia_df[
                famous_people_trivia_df["category"].str.contains(command_args.named_args["category"], case=False)
            ]
        else:
            filtered_df = famous_people_trivia_df

        available_difficulties = []
        for diff, (start_idx, end_idx) in MapQuiz.DIFFICULTY_INDEX_RANGES.items():
            if not filtered_df[(filtered_df.index >= start_idx) & (filtered_df.index < end_idx)].empty:
                available_difficulties.append(diff)

        if not available_difficulties:
            if "category" in command_args.named_args:
                categories = sorted(famous_people_trivia_df["category"].dropna().unique().tolist())
                cats_str = ", ".join(categories)
                message = f"No persons found for the specified category.\nAvailable categories:\n{cats_str}"
            else:
                message = "No persons found."
            await core_utils.send_message(update, context, MessageType.TEXT, message)
            return

        if "difficulty" in command_args.named_args:
            difficulty = command_args.named_args["difficulty"].lower()
            if difficulty not in MapQuiz.DIFFICULTY_INDEX_RANGES:
                message = f"Invalid difficulty. Available: {', '.join(MapQuiz.DIFFICULTY_INDEX_RANGES.keys())}"
                await core_utils.send_message(update, context, MessageType.TEXT, message)
                return
            if difficulty not in available_difficulties:
                message = "No persons found for the specified category and difficulty."
                await core_utils.send_message(update, context, MessageType.TEXT, message)
                return
            chosen_diff = difficulty
        else:
            import random

            chosen_diff = random.choice(available_difficulties)

        start_idx, end_idx = MapQuiz.DIFFICULTY_INDEX_RANGES[chosen_diff]
        filtered_df = filtered_df[(filtered_df.index >= start_idx) & (filtered_df.index < end_idx)]

        map_quiz = MapQuiz()
        image_path, person = map_quiz.guess_random_person_on_map(filtered_df)

        reward, _ = MapQuiz.get_reward(chosen_diff, "category" in command_args.named_args, 0)
        caption = (
            f"Difficulty: {chosen_diff.replace('_', ' ').title()}\nTime to answer: {MAP_QUIZ_TIMEOUT_SECONDS}s\nReward: {reward} credits"
        )
        if "category" in command_args.named_args:
            caption += f"\nCategory: {person['category']}"

        await update.message.reply_photo(photo=image_path, caption=caption, message_thread_id=update.message.message_thread_id)

        self.bot_state.map_quiz_cache[user_id] = {
            "chat_id": update.effective_chat.id,
            "thread_id": update.message.message_thread_id,
            "person": person,
            "difficulty": chosen_diff,
            "category_specified": "category" in command_args.named_args,
            "extended_description": "extended_description" in command_args.named_args,
            "tips_given": 0,
            "job": context.job_queue.run_once(self.map_quiz_timeout, MAP_QUIZ_TIMEOUT_SECONDS, data=user_id),
        }

    async def map_quiz_timeout(self, context: ContextTypes.DEFAULT_TYPE):
        user_id = context.job.data
        if user_id in self.bot_state.map_quiz_cache:
            cached_quiz = self.bot_state.map_quiz_cache[user_id]
            chat_id = cached_quiz.get("chat_id")
            thread_id = cached_quiz.get("thread_id")
            person = cached_quiz.get("person")

            self.bot_state.map_quiz_cache.pop(user_id, None)

            if chat_id is not None and person is not None:
                display_name = MapQuiz.get_person_display_name(person)
                extended = cached_quiz.get("extended_description", False)
                description = MapQuiz.get_person_description(person, extended=extended)
                message = stats_utils.escape_special_characters(f"Time's up! The person was *{display_name}*.\n\n{description}")
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=message,
                    parse_mode=telegram.constants.ParseMode.MARKDOWN_V2,
                    message_thread_id=thread_id,
                )

    async def handle_map_quiz_answer(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not update.message or not update.message.text:
            return

        user_id = update.effective_user.id
        if user_id not in self.bot_state.map_quiz_cache:
            log.debug(f"handle_map_quiz_answer: user {user_id} not in cache")
            return

        cached_quiz = self.bot_state.map_quiz_cache[user_id]
        if update.message.message_thread_id != cached_quiz["thread_id"]:
            log.info(
                f"handle_map_quiz_answer: thread_id mismatch. Msg: {update.message.message_thread_id}, Cache: {cached_quiz['thread_id']}"
            )
            return

        person = cached_quiz["person"]
        valid_answers = MapQuiz.get_valid_answers(person)
        user_answer = update.message.text.lower().strip()
        extended = cached_quiz.get("extended_description", False)
        log.info(f"handle_map_quiz_answer: answer={user_answer}, extended={extended}, valid={valid_answers}")

        if user_answer == "!tip":
            tips = MapQuiz.get_tips(person)
            tips_given = cached_quiz.get("tips_given", 0)
            if tips_given < len(tips):
                tip_text = tips[tips_given]
                cached_quiz["tips_given"] = tips_given + 1

                current_reward, decrease = MapQuiz.get_reward(
                    cached_quiz.get("difficulty", "crazy"), cached_quiz.get("category_specified", False), cached_quiz["tips_given"]
                )

                message = stats_utils.escape_special_characters(
                    f"💡 Tip {tips_given + 1}/{len(tips)} - [{current_reward} credits, -{decrease}%]:\n\n{tip_text}"
                )
            else:
                message = stats_utils.escape_special_characters("No more tips available for this person!")

            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                reply_to_message_id=update.message.message_id,
                text=message,
                parse_mode=telegram.constants.ParseMode.MARKDOWN_V2,
                message_thread_id=update.message.message_thread_id,
            )
            return

        cached_quiz["job"].schedule_removal()
        self.bot_state.map_quiz_cache.pop(user_id, None)

        display_name = MapQuiz.get_person_display_name(person)

        is_correct = MapQuiz.is_answer_correct(user_answer, valid_answers)

        if is_correct:
            reward, _ = MapQuiz.get_reward(
                cached_quiz.get("difficulty", "crazy"), cached_quiz.get("category_specified", False), cached_quiz.get("tips_given", 0)
            )

            user_credits, _ = self.credits.update_credits(user_id=user_id, credit_change=reward, action_type=CreditActionType.QUIZ)
            description = MapQuiz.get_person_description(person, extended=extended)
            message = (
                f"Correct! The person is *{display_name}*.\nYou receive *{reward}* credits! [*{user_credits}* in total]\n\n{description}"
            )
        else:
            description = MapQuiz.get_person_description(person, extended=extended)
            message = f"Wrong! The correct answer was *{display_name}*.\n\n{description}"

        message = stats_utils.escape_special_characters(message)
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            reply_to_message_id=update.message.message_id,
            text=message,
            parse_mode=telegram.constants.ParseMode.MARKDOWN_V2,
            message_thread_id=update.message.message_thread_id,
        )

    async def cmd_guess_flag(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(
            args=context.args,
            available_named_args={"continent": ArgType.STRING, "difficulty": ArgType.STRING},
        )
        command_args = core_utils.parse_args(self.users_df, command_args)
        if command_args.error != "":
            await core_utils.send_message(update, context, MessageType.TEXT, command_args.error)
            return

        user_id = update.effective_user.id
        if user_id in self.bot_state.map_quiz_cache or user_id in self.bot_state.flag_quiz_cache:
            message = "You already have an active quiz! Answer it first or wait for the timeout."
            await core_utils.send_message(update, context, MessageType.TEXT, message)
            return

        continent_specified = "continent" in command_args.named_args
        if continent_specified:
            raw_continent = command_args.named_args["continent"]
            countries_df = self.assets.countries.get_countries(continent=raw_continent)
        else:
            countries_df = self.assets.countries.df

        if countries_df.empty:
            if continent_specified:
                continents = sorted(self.assets.countries.df["continent"].dropna().unique().tolist())
                conts_str = ", ".join(continents)
                message = f"No flags found for the specified continent.\nAvailable continents:\n{conts_str}"
            else:
                message = "No flags found."
            await core_utils.send_message(update, context, MessageType.TEXT, message)
            return

        available_difficulties = [
            diff for diff in ["easy", "medium", "hard", "crazy"] if not countries_df[countries_df["difficulty"] == diff].empty
        ]
        if not available_difficulties:
            message = "No flags found."
            await core_utils.send_message(update, context, MessageType.TEXT, message)
            return

        if "difficulty" in command_args.named_args:
            difficulty = command_args.named_args["difficulty"].lower()
            if difficulty not in ["easy", "medium", "hard", "crazy"]:
                message = "Invalid difficulty. Available: easy, medium, hard, crazy"
                await core_utils.send_message(update, context, MessageType.TEXT, message)
                return
            if difficulty not in available_difficulties:
                message = "No flags found for the specified continent and difficulty."
                await core_utils.send_message(update, context, MessageType.TEXT, message)
                return
            filtered_countries = countries_df[countries_df["difficulty"] == difficulty]
            country = self.assets.countries.pop_random_country(filtered_df=filtered_countries)
            chosen_diff = difficulty
        else:
            # No difficulty filter — use the shuffle queue so every country appears
            # exactly once before any repeats (eliminates birthday-paradox clustering).
            # Pass filtered_df only when a continent was specified, so the queue is
            # still used for the pure "random" case.
            filter_df = countries_df if continent_specified else None
            country = self.assets.countries.pop_random_country(filtered_df=filter_df)
            chosen_diff = country["difficulty"]

        flag_quiz = FlagQuiz()
        image_path = flag_quiz.get_image_path(country)

        reward, _ = FlagQuiz.get_reward(chosen_diff, continent_specified, 0)
        caption = (
            f"Difficulty: {chosen_diff.replace('_', ' ').title()}\nTime to answer: {FLAG_QUIZ_TIMEOUT_SECONDS}s\nReward: {reward} credits"
        )
        if continent_specified:
            caption += f"\nContinent: {country['continent']}"

        await update.message.reply_photo(photo=image_path, caption=caption, message_thread_id=update.message.message_thread_id)

        self.bot_state.flag_quiz_cache[user_id] = {
            "chat_id": update.effective_chat.id,
            "thread_id": update.message.message_thread_id,
            "country": country,
            "difficulty": chosen_diff,
            "continent_specified": continent_specified,
            "tips_given": 0,
            "job": context.job_queue.run_once(self.flag_quiz_timeout, FLAG_QUIZ_TIMEOUT_SECONDS, data=user_id),
        }

    async def flag_quiz_timeout(self, context: ContextTypes.DEFAULT_TYPE):
        user_id = context.job.data
        if user_id in self.bot_state.flag_quiz_cache:
            cached_quiz = self.bot_state.flag_quiz_cache[user_id]
            chat_id = cached_quiz.get("chat_id")
            thread_id = cached_quiz.get("thread_id")
            country = cached_quiz.get("country")

            self.bot_state.flag_quiz_cache.pop(user_id, None)

            if chat_id is not None and country is not None:
                display_name = FlagQuiz.get_country_display_name(country)
                description = FlagQuiz.get_country_description(country)
                message = stats_utils.escape_special_characters(f"Time's up! The country was *{display_name}*.\n\n{description}")
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=message,
                    parse_mode=telegram.constants.ParseMode.MARKDOWN_V2,
                    message_thread_id=thread_id,
                )

    async def handle_flag_quiz_answer(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not update.message or not update.message.text:
            return

        user_id = update.effective_user.id
        if user_id not in self.bot_state.flag_quiz_cache:
            log.debug(f"handle_flag_quiz_answer: user {user_id} not in cache")
            return

        cached_quiz = self.bot_state.flag_quiz_cache[user_id]
        if update.message.message_thread_id != cached_quiz["thread_id"]:
            log.info(
                f"handle_flag_quiz_answer: thread_id mismatch. Msg: {update.message.message_thread_id}, Cache: {cached_quiz['thread_id']}"
            )
            return

        country = cached_quiz["country"]
        valid_answers = FlagQuiz.get_valid_answers(country)
        user_answer = update.message.text.lower().strip()
        log.info(f"handle_flag_quiz_answer: answer={user_answer}, valid={valid_answers}")

        if user_answer == "!tip":
            tips = FlagQuiz.get_tips(country, continent_specified=cached_quiz.get("continent_specified", False))
            tips_given = cached_quiz.get("tips_given", 0)
            if tips_given < len(tips):
                tip_text = tips[tips_given]
                cached_quiz["tips_given"] = tips_given + 1

                current_reward, decrease = FlagQuiz.get_reward(
                    cached_quiz.get("difficulty", "crazy"), cached_quiz.get("continent_specified", False), cached_quiz["tips_given"]
                )

                message = stats_utils.escape_special_characters(
                    f"💡 Tip {tips_given + 1}/{len(tips)} - [{current_reward} credits, -{decrease}%]:\n\n{tip_text}"
                )
            else:
                message = stats_utils.escape_special_characters("No more tips available for this flag!")

            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                reply_to_message_id=update.message.message_id,
                text=message,
                parse_mode=telegram.constants.ParseMode.MARKDOWN_V2,
                message_thread_id=update.message.message_thread_id,
            )
            return

        cached_quiz["job"].schedule_removal()
        self.bot_state.flag_quiz_cache.pop(user_id, None)

        display_name = FlagQuiz.get_country_display_name(country)
        is_correct = FlagQuiz.is_answer_correct(user_answer, valid_answers)

        if is_correct:
            reward, _ = FlagQuiz.get_reward(
                cached_quiz.get("difficulty", "crazy"), cached_quiz.get("continent_specified", False), cached_quiz.get("tips_given", 0)
            )

            user_credits, _ = self.credits.update_credits(user_id=user_id, credit_change=reward, action_type=CreditActionType.QUIZ)
            description = FlagQuiz.get_country_description(country)
            message = (
                f"Correct! The country is *{display_name}*.\nYou receive *{reward}* credits! [*{user_credits}* in total]\n\n{description}"
            )
        else:
            description = FlagQuiz.get_country_description(country)
            message = f"Wrong! The correct answer was *{display_name}*.\n\n{description}"

        message = stats_utils.escape_special_characters(message)
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            reply_to_message_id=update.message.message_id,
            text=message,
            parse_mode=telegram.constants.ParseMode.MARKDOWN_V2,
            message_thread_id=update.message.message_thread_id,
        )

    async def cmd_steal_graph(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(
            args=context.args,
            expected_args=[ArgType.USER, ArgType.PERIOD],
            optional=[True, True],
            available_named_args={"all_attempts": ArgType.NONE, "credits": ArgType.NONE},
        )
        command_args = core_utils.parse_args(self.users_df, command_args)
        if command_args.error != "":
            await core_utils.send_message(update, context, MessageType.TEXT, command_args.error)
            return

        filtered_credits_df = self.credits.credit_history_df[self.credits.credit_history_df["action_type"] == CreditActionType.STEAL.value]
        filtered_credits_df = stats_utils.filter_by_time_df(filtered_credits_df, command_args, "timestamp")
        if "all_attempts" not in command_args.named_args:
            filtered_credits_df = filtered_credits_df[filtered_credits_df["success"]]

        filtered_credits_df["robbing_username"] = filtered_credits_df["user_id"].apply(lambda x: self.users_map[x])
        filtered_credits_df["robbed_username"] = filtered_credits_df["target_user_id"].apply(lambda x: self.users_map[x])

        text = core_utils.generate_response_headline(command_args, label="Steal Graph")
        path = charts.create_bidirectional_relationship_graph(
            filtered_credits_df,
            "robbing_username",
            "robbed_username",
            "Steal Network",
        )
        current_message_type = MessageType.IMAGE
        await core_utils.send_message(update, context, current_message_type, text, path)

    async def cmd_tournament(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(
            args=context.args,
            expected_args=[ArgType.TEXT, ArgType.POSITIVE_INT],
            optional=[False, False],
            available_named_args={"rounds": ArgType.POSITIVE_INT, "wait": ArgType.POSITIVE_INT},
            min_number=1,
            max_number=100_000_000,
        )
        command_args = core_utils.parse_args(self.users_df, command_args)
        if command_args.error != "":
            await core_utils.send_message(update, context, MessageType.TEXT, command_args.error)
            return

        tournament_type_str = command_args.string.lower()
        try:
            tournament_type = TournamentType(tournament_type_str)
        except ValueError:
            valid = ", ".join(t.value for t in TournamentType)
            await core_utils.send_message(update, context, MessageType.TEXT, f"Invalid tournament type. Available: {valid}")
            return

        chat_id = update.effective_chat.id
        if chat_id in self.active_tournaments:
            await core_utils.send_message(update, context, MessageType.TEXT, "A tournament is already running in this chat.")
            return

        user_id = update.effective_user.id
        if self.bot_state.is_tournament_banned(user_id, tournament_type.value):
            await core_utils.send_message(update, context, MessageType.TEXT, "You are banned from this tournament type today.")
            return

        buy_in = command_args.number
        if user_id not in self.credits.credits or self.credits.credits[user_id] < buy_in:
            current = self.credits.credits.get(user_id, 0)
            await core_utils.send_message(update, context, MessageType.TEXT, f"Not enough credits. You have {current} but need {buy_in}.")
            return

        max_rounds = TOURNAMENT_DEFAULT_ROUNDS
        if "rounds" in command_args.named_args:
            max_rounds = min(int(command_args.named_args["rounds"]), TOURNAMENT_MAX_ROUNDS)
            max_rounds = max(max_rounds, 1)

        join_timeout = TOURNAMENT_JOIN_TIMEOUT_SECONDS
        if "wait" in command_args.named_args:
            join_timeout = min(int(command_args.named_args["wait"]), 300)
            join_timeout = max(join_timeout, 1)

        username = core_utils.get_username(update.effective_user.first_name, update.effective_user.last_name)
        thread_id = update.message.message_thread_id

        tournament = RouletteTournament(chat_id, thread_id, user_id, username, self.credits, buy_in, max_rounds)
        self.active_tournaments[chat_id] = tournament

        header = tournament.format_header()
        message = stats_utils.escape_special_characters(
            f"{header}\n\nBuy-in: *{buy_in}* credits | Rounds: *{max_rounds}*\n"
            f"*{username}* started the tournament!\n\n"
            f"Type *join* within {join_timeout}s to enter or *start* to begin early!"
        )
        await core_utils.send_message(update, context, MessageType.MARKDOWN_TEXT, message)

        context.job_queue.run_once(
            self._on_join_timeout,
            join_timeout,
            data={"chat_id": chat_id, "thread_id": thread_id},
            name=f"tournament_join_{chat_id}",
        )

    async def _on_join_timeout(self, context: ContextTypes.DEFAULT_TYPE):
        data = context.job.data
        chat_id, thread_id = data["chat_id"], data["thread_id"]
        tournament = self.active_tournaments.get(chat_id)
        if not tournament or not tournament.is_active:
            return

        if not tournament.has_enough_players():
            message = tournament.cancel_and_refund()
            del self.active_tournaments[chat_id]
            message = stats_utils.escape_special_characters(f"{tournament.format_header()}\n\n{message}")
            await context.bot.send_message(
                chat_id=chat_id,
                text=message,
                parse_mode=telegram.constants.ParseMode.MARKDOWN_V2,
                message_thread_id=thread_id,
            )
            return

        await self._announce_tournament_start(context, chat_id, thread_id)

    async def _announce_tournament_start(self, context: ContextTypes.DEFAULT_TYPE, chat_id: int, thread_id: int):
        tournament = self.active_tournaments.get(chat_id)
        if not tournament or not tournament.is_active:
            return

        tournament.state = TournamentState.STARTING
        players_list = "\n".join(f"• {p.username}" for p in tournament.players.values())
        msg = f"🎰 *Roulette Tournament* starts in *10 seconds!*\n\n*Players:*\n{players_list}"
        message = stats_utils.escape_special_characters(msg)
        await context.bot.send_message(
            chat_id=chat_id,
            text=message,
            parse_mode=telegram.constants.ParseMode.MARKDOWN_V2,
            message_thread_id=thread_id,
        )

        context.job_queue.run_once(
            self._on_start_countdown_finished,
            10,
            data={"chat_id": chat_id, "thread_id": thread_id},
            name=f"tournament_start_{chat_id}",
        )

    async def _on_start_countdown_finished(self, context: ContextTypes.DEFAULT_TYPE):
        data = context.job.data
        chat_id, thread_id = data["chat_id"], data["thread_id"]
        await self._start_betting_round(context, chat_id, thread_id)

    async def _start_betting_round(self, context: ContextTypes.DEFAULT_TYPE, chat_id: int, thread_id: int):
        tournament = self.active_tournaments.get(chat_id)
        if not tournament or not tournament.is_active:
            return

        round_msg = tournament.start_betting_round()
        header = tournament.format_header()
        message = stats_utils.escape_special_characters(f"{header}\n\n{round_msg}")
        await context.bot.send_message(
            chat_id=chat_id,
            text=message,
            parse_mode=telegram.constants.ParseMode.MARKDOWN_V2,
            message_thread_id=thread_id,
        )

        context.job_queue.run_once(
            self._on_bet_timeout,
            TOURNAMENT_BET_TIMEOUT_SECONDS,
            data={"chat_id": chat_id, "thread_id": thread_id},
            name=f"tournament_bet_{chat_id}",
        )

    async def _on_bet_timeout(self, context: ContextTypes.DEFAULT_TYPE):
        data = context.job.data
        chat_id, thread_id = data["chat_id"], data["thread_id"]
        await self._process_round(context, chat_id, thread_id)

    async def _process_round(self, context: ContextTypes.DEFAULT_TYPE, chat_id: int, thread_id: int):
        tournament = self.active_tournaments.get(chat_id)
        if not tournament or not tournament.is_active:
            return

        if tournament.state != TournamentState.BETTING:
            return

        if not tournament.has_active_bets():
            await self._finish_tournament(context, chat_id, thread_id, "No bets placed. Tournament ending.")
            return

        tournament.state = TournamentState.SPINNING
        header = tournament.format_header()
        bets_msg = tournament.format_bets_summary()
        message = stats_utils.escape_special_characters(f"{header}\n\n{bets_msg}\n\nThe roulette is spinning...")
        await context.bot.send_message(
            chat_id=chat_id,
            text=message,
            parse_mode=telegram.constants.ParseMode.MARKDOWN_V2,
            message_thread_id=thread_id,
        )

        await asyncio.sleep(TOURNAMENT_SPIN_DELAY_SECONDS)

        result_msg = tournament.resolve_round()
        message = stats_utils.escape_special_characters(f"{header}\n\n{result_msg}")
        await context.bot.send_message(
            chat_id=chat_id,
            text=message,
            parse_mode=telegram.constants.ParseMode.MARKDOWN_V2,
            message_thread_id=thread_id,
        )

        if tournament.is_last_round():
            await self._finish_tournament(context, chat_id, thread_id)
        else:
            await asyncio.sleep(2)
            await self._start_betting_round(context, chat_id, thread_id)

    async def _finish_tournament(self, context: ContextTypes.DEFAULT_TYPE, chat_id: int, thread_id: int, extra_msg: str = ""):
        tournament = self.active_tournaments.get(chat_id)
        if not tournament:
            return

        final_msg, zeroed_user_ids = tournament.get_final_results()

        for user_id in zeroed_user_ids:
            self.bot_state.ban_from_tournament(user_id, tournament.tournament_type.value)

        del self.active_tournaments[chat_id]

        if extra_msg:
            final_msg = f"{extra_msg}\n\n{final_msg}"

        message = stats_utils.escape_special_characters(final_msg)
        await context.bot.send_message(
            chat_id=chat_id,
            text=message,
            parse_mode=telegram.constants.ParseMode.MARKDOWN_V2,
            message_thread_id=thread_id,
        )

    async def handle_tournament_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not update.message or not update.message.text:
            return

        chat_id = update.effective_chat.id
        tournament = self.active_tournaments.get(chat_id)
        if not tournament or not tournament.is_active:
            return

        if update.message.message_thread_id != tournament.thread_id:
            return

        user_id = update.effective_user.id
        text = update.message.text.strip().lower()
        username = core_utils.get_username(update.effective_user.first_name, update.effective_user.last_name)

        if text == "join" and tournament.state.value == "joining":
            if self.bot_state.is_tournament_banned(user_id, tournament.tournament_type.value):
                response = "You are banned from this tournament type today."
            else:
                response, _ = tournament.add_player(user_id, username)
            response = stats_utils.escape_special_characters(f"{tournament.format_header()}\n\n{response}")
            await context.bot.send_message(
                chat_id=chat_id,
                text=response,
                parse_mode=telegram.constants.ParseMode.MARKDOWN_V2,
                message_thread_id=update.message.message_thread_id,
            )
            return

        if text == "start" and tournament.state.value == "joining":
            if user_id != tournament.host_user_id:
                response = "Only the tournament host can start early."
            elif not tournament.has_enough_players():
                response = "At least 2 players are needed to start the tournament."
            else:
                jobs = context.job_queue.get_jobs_by_name(f"tournament_join_{chat_id}")
                for job in jobs:
                    job.schedule_removal()
                await self._announce_tournament_start(context, chat_id, update.message.message_thread_id)
                return
            response = stats_utils.escape_special_characters(f"{tournament.format_header()}\n\n{response}")
            await context.bot.send_message(
                chat_id=chat_id,
                text=response,
                parse_mode=telegram.constants.ParseMode.MARKDOWN_V2,
                message_thread_id=update.message.message_thread_id,
            )
            return

        response = tournament.handle_game_message(user_id, update.message.text.strip())
        if response:
            response = stats_utils.escape_special_characters(f"{tournament.format_header()}\n\n{response}")
            await context.bot.send_message(
                chat_id=chat_id,
                text=response,
                parse_mode=telegram.constants.ParseMode.MARKDOWN_V2,
                message_thread_id=update.message.message_thread_id,
            )

            if tournament.all_bets_placed():
                jobs = context.job_queue.get_jobs_by_name(f"tournament_bet_{chat_id}")
                for job in jobs:
                    job.schedule_removal()
                await self._process_round(context, chat_id, update.message.message_thread_id)
