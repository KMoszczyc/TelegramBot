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
    MAP_QUIZ_TIMEOUT_SECONDS,
    MIN_QUIZ_TIME_TO_ANSWER_SECONDS,
)
from src.config.enums import BET_EVENTS, QUIZ_EVENTS, STEAL_EVENTS, ArgType, CreditActionType, MessageType, Table
from src.core.command_logger import CommandLogger
from src.core.job_persistance import JobPersistance
from src.models.bot_state import BotState
from src.models.command_args import CommandArgs
from src.models.credits import Credits
from src.models.db.db import DB
from src.models.event_manager import EventManager
from src.models.map_quiz import MapQuiz
from src.models.quiz_model import QuizModel
from src.models.roulette import Roulette
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
        if user_id in self.bot_state.map_quiz_cache:
            message = "You already have an active map quiz! Answer it first or wait for the timeout."
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
            log.info(f"handle_map_quiz_answer: user {user_id} not in cache")
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
