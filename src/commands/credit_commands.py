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
    CAPITAL_QUIZ_TIMEOUT_SECONDS,
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
from src.models.core.bot_state import BotState
from src.models.core.credits import Credits
from src.models.data.command_args import CommandArgs
from src.models.db.db import DB
from src.models.events.event_manager import EventManager
from src.models.games.roulette import Roulette
from src.models.quizzes import tip_message
from src.models.quizzes.capital_quiz import CapitalQuiz
from src.models.quizzes.flag_quiz import FlagQuiz
from src.models.quizzes.map_quiz import MapQuiz
from src.models.quizzes.quiz_model import QuizModel
from src.models.quizzes.quiz_replay import (
    ReplayUpdateProxy,
    build_replay_keyboard,
    format_replay_command_name,
    parse_replay_callback_data,
)
from src.models.tournaments.roulette_tournament import RouletteTournament
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

        user_id = update.effective_user.id
        self._set_quiz_replay_args(user_id, "quiz", context.args)

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
            await core_utils.send_message(
                update,
                context,
                MessageType.MARKDOWN_TEXT,
                message,
                reply_markup=build_replay_keyboard("quiz", cached_quiz.user_id),
            )
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
            await core_utils.send_message(
                update,
                context,
                MessageType.MARKDOWN_TEXT,
                message,
                reply_markup=build_replay_keyboard("quiz", cached_quiz.user_id),
            )
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
        await core_utils.send_message(
            update,
            context,
            MessageType.MARKDOWN_TEXT,
            message,
            reply_markup=build_replay_keyboard("quiz", cached_quiz.user_id),
        )

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
        if self._has_active_guess_quiz(user_id):
            message = "You already have an active quiz! Answer it first or wait for the timeout."
            await core_utils.send_message(update, context, MessageType.TEXT, message)
            return

        self._set_quiz_replay_args(user_id, "map", context.args)

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

        category_specified = "category" in command_args.named_args
        caption = map_quiz.build_quiz_prompt_text(person, chosen_diff, MAP_QUIZ_TIMEOUT_SECONDS, category_specified)
        total_tips = len(map_quiz.get_tips(person, continent_specified=category_specified))
        keyboard = tip_message.build_initial_tip_keyboard(MapQuiz, user_id, chosen_diff, category_specified, total_tips)
        escaped_caption = stats_utils.escape_special_characters(caption)

        sent = await update.message.reply_photo(
            photo=image_path,
            caption=escaped_caption,
            parse_mode=telegram.constants.ParseMode.MARKDOWN_V2,
            reply_markup=keyboard,
            message_thread_id=update.message.message_thread_id,
        )

        self.bot_state.map_quiz_cache[user_id] = {
            "chat_id": update.effective_chat.id,
            "thread_id": update.message.message_thread_id,
            "person": person,
            "difficulty": chosen_diff,
            "category_specified": category_specified,
            "continent_specified": category_specified,
            "extended_description": "extended_description" in command_args.named_args,
            "tips_given": 0,
            "tip_lines": [],
            "message_id": sent.message_id,
            "has_photo": True,
            "base_text": caption,
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
            await self._remove_tip_button(context, cached_quiz)

            if chat_id is not None and person is not None:
                quiz_instance = MapQuiz()
                extended = cached_quiz.get("extended_description", False)
                message = stats_utils.escape_special_characters(quiz_instance.format_timeout_message(person, extended=extended))
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=message,
                    parse_mode=telegram.constants.ParseMode.MARKDOWN_V2,
                    message_thread_id=thread_id,
                    reply_markup=build_replay_keyboard("map", user_id),
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
        valid_answers = MapQuiz().get_valid_answers(person)
        user_answer = update.message.text.lower().strip()
        extended = cached_quiz.get("extended_description", False)
        log.info(f"handle_map_quiz_answer: answer={user_answer}, extended={extended}, valid={valid_answers}")

        if user_answer == "!tip":
            tip_line = tip_message.reveal_next_tip(cached_quiz, MapQuiz(), MapQuiz, person)
            if tip_line is None:
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    reply_to_message_id=update.message.message_id,
                    text=stats_utils.escape_special_characters("No more tips available for this person!"),
                    parse_mode=telegram.constants.ParseMode.MARKDOWN_V2,
                    message_thread_id=update.message.message_thread_id,
                )
            else:
                await self._show_tip(
                    context, cached_quiz, user_id, MapQuiz, MapQuiz(), tip_line, fallback_reply_to=update.message.message_id
                )
            return

        cached_quiz["job"].schedule_removal()
        self.bot_state.map_quiz_cache.pop(user_id, None)
        await self._remove_tip_button(context, cached_quiz)

        quiz_instance = MapQuiz()
        is_correct = MapQuiz.is_answer_correct(user_answer, valid_answers)

        if is_correct:
            reward, _ = MapQuiz.get_reward(
                cached_quiz.get("difficulty", "crazy"), cached_quiz.get("category_specified", False), cached_quiz.get("tips_given", 0)
            )

            user_credits, _ = self.credits.update_credits(user_id=user_id, credit_change=reward, action_type=CreditActionType.QUIZ)
            message = quiz_instance.format_correct_message(person, reward, total_credits=user_credits, extended=extended)
        else:
            message = quiz_instance.format_wrong_message(person, extended=extended)

        message = stats_utils.escape_special_characters(message)
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            reply_to_message_id=update.message.message_id,
            text=message,
            parse_mode=telegram.constants.ParseMode.MARKDOWN_V2,
            message_thread_id=update.message.message_thread_id,
            reply_markup=build_replay_keyboard("map", user_id),
        )

    async def _cmd_guess_country_generic(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        quiz_class,
        cache_dict: dict,
        timeout_seconds: int,
        timeout_callback,
        title: str,
    ):
        command_args = CommandArgs(
            args=context.args,
            available_named_args={"continent": ArgType.STRING, "difficulty": ArgType.STRING},
        )
        command_args = core_utils.parse_args(self.users_df, command_args)
        if command_args.error != "":
            await core_utils.send_message(update, context, MessageType.TEXT, command_args.error)
            return

        user_id = update.effective_user.id
        if self._has_active_guess_quiz(user_id):
            message = "You already have an active quiz! Answer it first or wait for the timeout."
            await core_utils.send_message(update, context, MessageType.TEXT, message)
            return

        self._set_quiz_replay_args(user_id, quiz_class.QUIZ_TYPE, context.args)

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
                message = f"No countries found for the specified continent.\nAvailable continents:\n{conts_str}"
            else:
                message = "No countries found."
            await core_utils.send_message(update, context, MessageType.TEXT, message)
            return

        available_difficulties = [
            diff for diff in ["easy", "medium", "hard", "crazy"] if not countries_df[countries_df["difficulty"] == diff].empty
        ]
        if not available_difficulties:
            message = "No countries found."
            await core_utils.send_message(update, context, MessageType.TEXT, message)
            return

        if "difficulty" in command_args.named_args:
            difficulty = command_args.named_args["difficulty"].lower()
            if difficulty not in ["easy", "medium", "hard", "crazy"]:
                message = "Invalid difficulty. Available: easy, medium, hard, crazy"
                await core_utils.send_message(update, context, MessageType.TEXT, message)
                return
            if difficulty not in available_difficulties:
                message = "No countries found for the specified continent and difficulty."
                await core_utils.send_message(update, context, MessageType.TEXT, message)
                return
            filtered_countries = countries_df[countries_df["difficulty"] == difficulty]
            country = self.assets.countries.pop_random_country(filtered_df=filtered_countries)
            chosen_diff = difficulty
        else:
            filter_df = countries_df if continent_specified else None
            country = self.assets.countries.pop_random_country(filtered_df=filter_df)
            chosen_diff = country["difficulty"]

        quiz_instance = quiz_class()
        image_path = quiz_instance.get_image_path(country)

        base_text = quiz_instance.build_quiz_prompt_text(country, chosen_diff, timeout_seconds, continent_specified, title=title)
        total_tips = len(quiz_instance.get_tips(country, continent_specified=continent_specified))
        keyboard = tip_message.build_initial_tip_keyboard(quiz_class, user_id, chosen_diff, continent_specified, total_tips)
        escaped_text = stats_utils.escape_special_characters(base_text)

        if image_path:
            sent = await update.message.reply_photo(
                photo=image_path,
                caption=escaped_text,
                parse_mode=telegram.constants.ParseMode.MARKDOWN_V2,
                reply_markup=keyboard,
                message_thread_id=update.message.message_thread_id,
            )
        else:
            sent = await update.message.reply_text(
                text=escaped_text,
                parse_mode=telegram.constants.ParseMode.MARKDOWN_V2,
                reply_markup=keyboard,
                message_thread_id=update.message.message_thread_id,
            )

        cache_dict[user_id] = {
            "chat_id": update.effective_chat.id,
            "thread_id": update.message.message_thread_id,
            "country": country,
            "difficulty": chosen_diff,
            "continent_specified": continent_specified,
            "tips_given": 0,
            "tip_lines": [],
            "message_id": sent.message_id,
            "has_photo": bool(image_path),
            "base_text": base_text,
            "job": context.job_queue.run_once(timeout_callback, timeout_seconds, data=user_id),
        }

    def _has_active_guess_quiz(self, user_id: int) -> bool:
        """Check if *user_id* currently has an active guess quiz session."""
        has_method = getattr(self.bot_state, "has_active_guess_quiz", None)
        if callable(has_method) and not hasattr(has_method, "assert_called"):
            return bool(has_method(user_id))
        return (
            user_id in getattr(self.bot_state, "map_quiz_cache", {})
            or user_id in getattr(self.bot_state, "flag_quiz_cache", {})
            or user_id in getattr(self.bot_state, "capital_quiz_cache", {})
        )

    def _find_active_guess_quiz(self, user_id: int):
        """Return the (cache, quiz class) pair holding *user_id*'s active quiz, or None."""
        find_method = getattr(self.bot_state, "find_active_guess_quiz", None)
        if callable(find_method) and not hasattr(find_method, "assert_called"):
            return find_method(user_id)
        for cache_dict, quiz_class in (
            (getattr(self.bot_state, "map_quiz_cache", {}), MapQuiz),
            (getattr(self.bot_state, "flag_quiz_cache", {}), FlagQuiz),
            (getattr(self.bot_state, "capital_quiz_cache", {}), CapitalQuiz),
        ):
            if user_id in cache_dict:
                return cache_dict, quiz_class
        return None

    def _set_quiz_replay_args(self, user_id: int, quiz_type: str, args: list[str] | None = None) -> None:
        set_method = getattr(self.bot_state, "set_quiz_replay_args", None)
        if callable(set_method) and not hasattr(set_method, "assert_called"):
            set_method(user_id, quiz_type, args)
            return
        self.bot_state.quiz_replay_cache[user_id][quiz_type] = list(args) if args else []

    def _get_quiz_replay_args(self, user_id: int, quiz_type: str) -> list[str]:
        get_method = getattr(self.bot_state, "get_quiz_replay_args", None)
        if callable(get_method) and not hasattr(get_method, "assert_called"):
            return list(get_method(user_id, quiz_type))
        return list(self.bot_state.quiz_replay_cache.get(user_id, {}).get(quiz_type, []))

    async def _remove_tip_button(self, context: ContextTypes.DEFAULT_TYPE, cached_quiz: dict):
        if cached_quiz.get("message_id") is None:
            return
        try:
            await context.bot.edit_message_reply_markup(
                chat_id=cached_quiz["chat_id"], message_id=cached_quiz["message_id"], reply_markup=None
            )
        except telegram.error.TelegramError as e:
            log.warning(f"Could not remove tip button: {e}")

    async def _update_quiz_message(self, context: ContextTypes.DEFAULT_TYPE, cached_quiz: dict, text: str, reply_markup):
        edit_kwargs = {
            "chat_id": cached_quiz["chat_id"],
            "message_id": cached_quiz["message_id"],
            "parse_mode": telegram.constants.ParseMode.MARKDOWN_V2,
            "reply_markup": reply_markup,
        }
        try:
            if cached_quiz.get("has_photo"):
                await context.bot.edit_message_caption(caption=text, **edit_kwargs)
            else:
                await context.bot.edit_message_text(text=text, **edit_kwargs)
        except telegram.error.TelegramError as e:
            log.warning(f"Could not edit quiz message: {e}")

    async def _show_tip(
        self,
        context: ContextTypes.DEFAULT_TYPE,
        cached_quiz: dict,
        user_id: int,
        quiz_class,
        quiz_instance,
        tip_line: str,
        fallback_reply_to: int | None = None,
    ):
        """Show a freshly revealed tip inside the quiz message, or as a reply when it doesn't fit (or the quiz message is unknown)."""
        plan = tip_message.plan_tip_display(cached_quiz, quiz_instance, quiz_class, user_id, tip_line)
        if plan.can_edit:
            await self._update_quiz_message(context, cached_quiz, plan.edit_text, plan.keyboard)
            return

        await context.bot.send_message(
            chat_id=plan.chat_id,
            reply_to_message_id=plan.message_id if plan.message_id is not None else fallback_reply_to,
            text=plan.overflow_text,
            parse_mode=telegram.constants.ParseMode.MARKDOWN_V2,
            message_thread_id=plan.thread_id,
        )
        if plan.message_id is not None:
            try:
                await context.bot.edit_message_reply_markup(chat_id=plan.chat_id, message_id=plan.message_id, reply_markup=plan.keyboard)
            except telegram.error.TelegramError as e:
                log.warning(f"Could not update tip button: {e}")

    async def btn_quiz_tip_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle a press of the tip button attached to a flag/capital/person quiz message."""
        query = update.callback_query
        owner_id = tip_message.parse_tip_callback_data(query.data)
        if owner_id is None or query.from_user.id != owner_id:
            await query.answer("This isn't your quiz!")
            return

        active_quiz = self._find_active_guess_quiz(owner_id)
        if active_quiz is None or active_quiz[0][owner_id].get("message_id") != query.message.message_id:
            await query.answer("This quiz is already over.")
            return

        cache_dict, quiz_class = active_quiz
        cached_quiz = cache_dict[owner_id]
        quiz_instance = quiz_class()
        item = cached_quiz.get("country") or cached_quiz.get("person")
        tip_line = tip_message.reveal_next_tip(cached_quiz, quiz_instance, quiz_class, item)
        if tip_line is None:
            await query.answer("No more tips available!")
            return

        await query.answer()
        await self._show_tip(context, cached_quiz, owner_id, quiz_class, quiz_instance, tip_line)

    async def btn_quiz_replay_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle a press of the Play Again button attached to a quiz answer message."""
        query = update.callback_query
        if not query:
            return

        parsed = parse_replay_callback_data(query.data)
        if not parsed:
            await query.answer()
            return

        quiz_type, owner_id = parsed
        caller_id = query.from_user.id if query.from_user else None
        if caller_id != owner_id:
            cmd_name = format_replay_command_name(quiz_type)
            await query.answer(f"Start your own quiz with /{cmd_name}!")
            return

        await query.answer()

        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except telegram.error.TelegramError as e:
            log.warning(f"Could not remove replay button: {e}")

        cached_args = self._get_quiz_replay_args(owner_id, quiz_type)
        context.args = list(cached_args)
        replay_update = ReplayUpdateProxy(update, query.message, query.from_user)

        match quiz_type:
            case "quiz":
                await self.cmd_quiz(replay_update, context)
            case "map":
                await self.cmd_guess_person_on_a_map(replay_update, context)
            case "flag":
                await self.cmd_guess_flag(replay_update, context)
            case "capital":
                await self.cmd_guess_capital(replay_update, context)
            case _:
                log.error(f"Unknown quiz_type for replay: {quiz_type}")

    async def _guess_country_timeout_generic(self, context: ContextTypes.DEFAULT_TYPE, cache_dict: dict, quiz_class):
        user_id = context.job.data
        if user_id in cache_dict:
            cached_quiz = cache_dict[user_id]
            chat_id = cached_quiz.get("chat_id")
            thread_id = cached_quiz.get("thread_id")
            country = cached_quiz.get("country")

            cache_dict.pop(user_id, None)
            await self._remove_tip_button(context, cached_quiz)

            if chat_id is not None and country is not None:
                quiz_instance = quiz_class()
                message = stats_utils.escape_special_characters(quiz_instance.format_timeout_message(country))
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=message,
                    parse_mode=telegram.constants.ParseMode.MARKDOWN_V2,
                    message_thread_id=thread_id,
                    reply_markup=build_replay_keyboard(quiz_class.QUIZ_TYPE, user_id),
                )

    async def _handle_guess_country_answer_generic(self, update: Update, context: ContextTypes.DEFAULT_TYPE, cache_dict: dict, quiz_class):
        if not update.message or not update.message.text:
            return

        user_id = update.effective_user.id
        if user_id not in cache_dict:
            return

        cached_quiz = cache_dict[user_id]
        if update.message.message_thread_id != cached_quiz["thread_id"]:
            return

        country = cached_quiz["country"]
        quiz_instance = quiz_class()
        valid_answers = quiz_instance.get_valid_answers(country)
        user_answer = update.message.text.lower().strip()

        if user_answer == "!tip":
            tip_line = tip_message.reveal_next_tip(cached_quiz, quiz_instance, quiz_class, country)
            if tip_line is None:
                type_name = "flag" if quiz_class.QUIZ_TYPE == "flag" else "quiz"
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    reply_to_message_id=update.message.message_id,
                    text=stats_utils.escape_special_characters(f"No more tips available for this {type_name}!"),
                    parse_mode=telegram.constants.ParseMode.MARKDOWN_V2,
                    message_thread_id=update.message.message_thread_id,
                )
            else:
                await self._show_tip(
                    context, cached_quiz, user_id, quiz_class, quiz_instance, tip_line, fallback_reply_to=update.message.message_id
                )
            return

        cached_quiz["job"].schedule_removal()
        cache_dict.pop(user_id, None)
        await self._remove_tip_button(context, cached_quiz)

        is_correct = quiz_class.is_answer_correct(user_answer, valid_answers)

        if is_correct:
            reward, _ = quiz_class.get_reward(
                cached_quiz.get("difficulty", "crazy"), cached_quiz.get("continent_specified", False), cached_quiz.get("tips_given", 0)
            )

            user_credits, _ = self.credits.update_credits(user_id=user_id, credit_change=reward, action_type=CreditActionType.QUIZ)
            message = quiz_instance.format_correct_message(country, reward, total_credits=user_credits)
        else:
            message = quiz_instance.format_wrong_message(country)

        message = stats_utils.escape_special_characters(message)
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            reply_to_message_id=update.message.message_id,
            text=message,
            parse_mode=telegram.constants.ParseMode.MARKDOWN_V2,
            message_thread_id=update.message.message_thread_id,
            reply_markup=build_replay_keyboard(quiz_class.QUIZ_TYPE, user_id),
        )

    async def cmd_guess_flag(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self._cmd_guess_country_generic(
            update, context, FlagQuiz, self.bot_state.flag_quiz_cache, FLAG_QUIZ_TIMEOUT_SECONDS, self.flag_quiz_timeout, ""
        )

    async def flag_quiz_timeout(self, context: ContextTypes.DEFAULT_TYPE):
        await self._guess_country_timeout_generic(context, self.bot_state.flag_quiz_cache, FlagQuiz)

    async def handle_flag_quiz_answer(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self._handle_guess_country_answer_generic(update, context, self.bot_state.flag_quiz_cache, FlagQuiz)

    async def cmd_guess_capital(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self._cmd_guess_country_generic(
            update,
            context,
            CapitalQuiz,
            self.bot_state.capital_quiz_cache,
            CAPITAL_QUIZ_TIMEOUT_SECONDS,
            self.capital_quiz_timeout,
            "\U0001f3db Guess the Capital!",
        )  # noqa: B023

    async def capital_quiz_timeout(self, context: ContextTypes.DEFAULT_TYPE):
        await self._guess_country_timeout_generic(context, self.bot_state.capital_quiz_cache, CapitalQuiz)

    async def handle_capital_quiz_answer(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self._handle_guess_country_answer_generic(update, context, self.bot_state.capital_quiz_cache, CapitalQuiz)

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
