import asyncio
import copy
import logging

import telegram
from telegram import InlineKeyboardButton, Update
from telegram.ext import ContextTypes

import src.core.utils as core_utils
import src.stats.utils as stats_utils
from definitions import (
    BET_EVENTS,
    CRITICAL_FAILURE_CHANCE,
    CRITICAL_SUCCESS_CHANCE,
    MIN_QUIZ_TIME_TO_ANSWER_SECONDS,
    QUIZ_EVENTS,
    STEAL_EVENTS,
    USERS_PATH,
    ArgType,
    CreditActionType,
    MessageType,
    quiz_df,
)
from src.core.command_logger import CommandLogger
from src.core.job_persistance import JobPersistance
from src.models.bot_state import BotState
from src.models.command_args import CommandArgs
from src.models.credits import Credits
from src.models.event_manager import EventManager
from src.models.quiz_model import QuizModel
from src.models.roulette import Roulette
from src.stats import charts

log = logging.getLogger(__name__)


class CreditCommands:
    def __init__(self, command_logger: CommandLogger, job_persistance: JobPersistance, bot_state: BotState, credits: Credits):
        self.command_logger = command_logger
        self.job_persistance = job_persistance
        self.bot_state = bot_state
        self.users_df = stats_utils.read_df(USERS_PATH)
        self.users_map = stats_utils.get_users_map(self.users_df)
        self.credits = credits
        self.roulette = Roulette(self.credits)
        self.event_manager = EventManager()

        for event in STEAL_EVENTS:
            self.event_manager.add_event('steal', event)
        for event in BET_EVENTS:
            self.event_manager.add_event('bet', event)
        for event in QUIZ_EVENTS:
            self.event_manager.add_event('quiz', event)

        bot_state.init_quiz_map(self.users_df)

    async def cmd_get_credits(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        can_get_credits = self.bot_state.update_get_credits_limits(user_id)
        if not can_get_credits:
            message = stats_utils.escape_special_characters("You already got your credits today :)")
            await context.bot.send_message(chat_id=update.effective_chat.id, text=message, parse_mode=telegram.constants.ParseMode.MARKDOWN_V2, message_thread_id=update.message.message_thread_id)
            return

        message = self.credits.get_daily_credits(user_id)
        await context.bot.send_message(chat_id=update.effective_chat.id, text=message, parse_mode=telegram.constants.ParseMode.MARKDOWN_V2, message_thread_id=update.message.message_thread_id)

    async def cmd_show_credit_leaderboard(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        message = self.credits.show_credit_leaderboard(self.users_map)
        await context.bot.send_message(chat_id=update.effective_chat.id, text=message, parse_mode=telegram.constants.ParseMode.MARKDOWN_V2, message_thread_id=update.message.message_thread_id)

    async def cmd_show_top_bet_leaderboard(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(args=context.args, expected_args=[ArgType.USER, ArgType.PERIOD], optional=[True, True])
        command_args = core_utils.parse_args(self.users_df, command_args)
        if command_args.error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error, message_thread_id=update.message.message_thread_id)
            return

        message = self.credits.show_top_bet_leaderboard(self.users_map, command_args)
        await context.bot.send_message(chat_id=update.effective_chat.id, text=message, parse_mode=telegram.constants.ParseMode.MARKDOWN_V2, message_thread_id=update.message.message_thread_id)

    async def cmd_show_steal_leaderboard(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(args=context.args, expected_args=[ArgType.USER, ArgType.PERIOD], optional=[True, True])
        command_args = core_utils.parse_args(self.users_df, command_args)
        if command_args.error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error, message_thread_id=update.message.message_thread_id)
            return

        message = self.credits.show_steal_leaderboard(self.users_map, command_args)
        await context.bot.send_message(chat_id=update.effective_chat.id, text=message, parse_mode=telegram.constants.ParseMode.MARKDOWN_V2, message_thread_id=update.message.message_thread_id)

    async def cmd_bet(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(args=context.args, expected_args=[ArgType.TEXT, ArgType.POSITIVE_INT], optional=[False, False], min_number=1, max_number=10000000,
                                   max_string_length=1000)
        command_args = core_utils.parse_args(self.users_df, command_args)
        if command_args.error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error, message_thread_id=update.message.message_thread_id)
            return

        bet_size = command_args.number
        bet_type_arg = command_args.string

        await context.bot.send_message(chat_id=update.effective_chat.id, text=stats_utils.escape_special_characters("The roulette is spinning..."),
                                       parse_mode=telegram.constants.ParseMode.MARKDOWN_V2, message_thread_id=update.message.message_thread_id)
        await asyncio.sleep(5)

        message, success = self.roulette.play(update.effective_user.id, bet_size, bet_type_arg)
        if success and core_utils.roll(CRITICAL_SUCCESS_CHANCE) and (event := self.event_manager.get_random_event('bet', 'success')):
            effect = event.apply_effect(amount=bet_size)
            self.credits.update_credits(update.effective_user.id, effect.credit_change, CreditActionType.BET)
            message += f"\n\n**Critical Success!** {effect.message}"
        elif not success and core_utils.roll(CRITICAL_FAILURE_CHANCE) and (event := self.event_manager.get_random_event('bet', 'failure')):
            effect = event.apply_effect(amount=bet_size)
            self.credits.update_credits(update.effective_user.id, effect.credit_change, CreditActionType.BET)
            message += f"\n\n**Critical Failure!** {effect.message}"

        message = stats_utils.escape_special_characters(message)
        await context.bot.send_message(chat_id=update.effective_chat.id, text=message, parse_mode=telegram.constants.ParseMode.MARKDOWN_V2, message_thread_id=update.message.message_thread_id)

    async def cmd_steal_credits(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(args=context.args, expected_args=[ArgType.USER, ArgType.POSITIVE_INT], min_number=1, max_number=10000000)
        command_args = core_utils.parse_args(self.users_df, command_args)
        if command_args.error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error, message_thread_id=update.message.message_thread_id)
            return

        user_id = update.effective_user.id
        if user_id == command_args.user_id:
            await context.bot.send_message(chat_id=update.effective_chat.id, text="You want to steal from yourself? lol xd", message_thread_id=update.message.message_thread_id)
            return

        can_steal = self.bot_state.update_steal_credits_limits(user_id)
        if not can_steal:
            message = stats_utils.escape_special_characters("You have reached your daily steal quota, no more thieving today :(")
            await context.bot.send_message(chat_id=update.effective_chat.id, text=message, parse_mode=telegram.constants.ParseMode.MARKDOWN_V2, message_thread_id=update.message.message_thread_id)
            return

        success, message = self.credits.validate_steal(target_user_id=command_args.user_id, amount=command_args.number, users_map=self.users_map)
        if not success:
            await context.bot.send_message(chat_id=update.effective_chat.id, text=message, parse_mode=telegram.constants.ParseMode.MARKDOWN_V2, message_thread_id=update.message.message_thread_id)
            return

        p = self.credits.calculate_steal_chance(target_user_id=command_args.user_id, amount=command_args.number)
        robbed_username = self.users_map[command_args.user_id]
        waiting_message = stats_utils.escape_special_characters(f"Attempting to steal *{command_args.number}* credits from *{robbed_username}* [*{p * 100:.1f}%* chance]..")
        await context.bot.send_message(chat_id=update.effective_chat.id, text=waiting_message, parse_mode=telegram.constants.ParseMode.MARKDOWN_V2, message_thread_id=update.message.message_thread_id)
        await asyncio.sleep(5)

        message, success = self.credits.steal_credits(user_id=user_id, target_user_id=command_args.user_id, amount=command_args.number, users_map=self.users_map)
        if success and core_utils.roll(CRITICAL_SUCCESS_CHANCE) and (event := self.event_manager.get_random_event('steal', 'success')):
            effect = event.apply_effect(amount=command_args.number)
            self.credits.update_credits(user_id, effect.credit_change, CreditActionType.STEAL)
            message += f"\n\n**Critical Success!** {effect.message}"
        elif not success and core_utils.roll(CRITICAL_FAILURE_CHANCE) and (event := self.event_manager.get_random_event('steal', 'failure')):
            effect = event.apply_effect(amount=command_args.number)
            self.credits.update_credits(user_id, effect.credit_change, CreditActionType.STEAL)
            message += f"\n\n**Critical Failure!** {effect.message}"

        message = stats_utils.escape_special_characters(message)
        await context.bot.send_message(chat_id=update.effective_chat.id, text=message, parse_mode=telegram.constants.ParseMode.MARKDOWN_V2, message_thread_id=update.message.message_thread_id)

    async def cmd_gift_credits(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(args=context.args, expected_args=[ArgType.USER, ArgType.POSITIVE_INT], min_number=1, max_number=10000000)
        command_args = core_utils.parse_args(self.users_df, command_args)
        if command_args.error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error, message_thread_id=update.message.message_thread_id)
            return

        source_user_id = update.effective_user.id
        target_user_id = command_args.user_id
        if source_user_id == command_args.user_id:
            await context.bot.send_message(chat_id=update.effective_chat.id, text="You can't gift credits to yourself!", message_thread_id=update.message.message_thread_id)
            return

        result = self.credits.gift_credits(source_user_id=source_user_id, target_user_id=target_user_id, amount=command_args.number, users_map=self.users_map)
        message = stats_utils.escape_special_characters(result)
        await context.bot.send_message(chat_id=update.effective_chat.id, text=message, parse_mode=telegram.constants.ParseMode.MARKDOWN_V2, message_thread_id=update.message.message_thread_id)

    async def cmd_quiz(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(args=context.args, available_named_args={'category': ArgType.STRING, 'type': ArgType.STRING, 'difficulty': ArgType.STRING})
        command_args = core_utils.parse_args(self.users_df, command_args)
        if command_args.error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error, message_thread_id=update.message.message_thread_id)
            return

        filtered_quiz_df = copy.deepcopy(quiz_df)
        if 'category' in command_args.named_args:
            filtered_quiz_df = filtered_quiz_df[filtered_quiz_df['category'].str.contains(command_args.named_args['category'], case=False)]
        if 'difficulty' in command_args.named_args:
            filtered_quiz_df = filtered_quiz_df[filtered_quiz_df['difficulty'].str.contains(command_args.named_args['difficulty'])]
        if 'type' in command_args.named_args:
            filtered_quiz_df = filtered_quiz_df[filtered_quiz_df['type'].str.contains(command_args.named_args['type'])]

        if filtered_quiz_df.empty:
            await context.bot.send_message(chat_id=update.effective_chat.id, text='No questions in database with these parameters. :[', message_thread_id=update.message.message_thread_id)
            return

        random_quiz_id = self.bot_state.get_random_quiz_id(filtered_quiz_df, update.effective_user.id)
        if random_quiz_id == -1:
            await context.bot.send_message(chat_id=update.effective_chat.id, text='There are questions in the database with these parameters, but you have already answered them.',
                                           message_thread_id=update.message.message_thread_id)
            return

        random_quiz = filtered_quiz_df[filtered_quiz_df['quiz_id'] == random_quiz_id].iloc[0]
        buttons = []
        for answer in random_quiz['answers']:
            buttons.append(InlineKeyboardButton(answer, callback_data=answer))

        if random_quiz['type'] == 'boolean':
            buttons = [buttons]
        else:
            buttons = [[buttons[0], buttons[1]], [buttons[2], buttons[3]]]
        reply_markup = telegram.InlineKeyboardMarkup(buttons)
        message = stats_utils.escape_special_characters(f"*[{random_quiz['category']}, {random_quiz['difficulty']}]*\n\n{random_quiz['question']}")
        reply = await update.message.reply_text(message, reply_markup=reply_markup, parse_mode=telegram.constants.ParseMode.MARKDOWN_V2, message_thread_id=update.message.message_thread_id)

        # Store quiz in cache for inline btn callbacks to work
        seconds_to_answer = MIN_QUIZ_TIME_TO_ANSWER_SECONDS + len(random_quiz['question'].split())
        quiz = QuizModel(quiz_id=random_quiz_id, user_id=update.effective_user.id, question=random_quiz['question'], difficulty=random_quiz['difficulty'], type=random_quiz['type'],
                         correct_answer=random_quiz['correct_answer'], display_answer=random_quiz['display_answer'],
                         start_dt=core_utils.get_dt_now(), seconds_to_answer=seconds_to_answer)
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
            message = stats_utils.escape_special_characters(f"Time's out! You've had *{int(cached_quiz.seconds_to_answer)}s* to answer, yet it took you *{time_elapsed:.2f}s*, lol.")
            await context.bot.send_message(chat_id=update.effective_chat.id, text=message, parse_mode=telegram.constants.ParseMode.MARKDOWN_V2, message_thread_id=query.message.message_thread_id)
            return

        if cached_quiz.correct_answer != query.data:  # apply penalty for incorrect answer
            credit_penalty = cached_quiz.get_credit_penalty()
            user_credits, success = self.credits.update_credits(user_id=cached_quiz.user_id, credit_change=credit_penalty, action_type=CreditActionType.QUIZ)
            message = f"Answer: *{query.data}* is incorrect. You lose *{abs(credit_penalty)}* credits [*{user_credits}* left] :[" if success else f"Answer: *{query.data}* is incorrect, but because you're so poor you won't lose any credits for it :]"
            if core_utils.roll(CRITICAL_FAILURE_CHANCE) and (event := self.event_manager.get_random_event('quiz', 'failure')):
                effect = event.apply_effect(amount=credit_penalty)
                self.credits.update_credits(cached_quiz.user_id, effect.credit_change, CreditActionType.QUIZ)
                message += f"\n\n**Critical Failure!** {effect.message}"
            message = stats_utils.escape_special_characters(message)
            await context.bot.send_message(chat_id=update.effective_chat.id, text=message, parse_mode=telegram.constants.ParseMode.MARKDOWN_V2, message_thread_id=query.message.message_thread_id)
            return

        # credit payout for correct answer :)
        credit_payout = cached_quiz.get_credit_payout()
        user_credits, _ = self.credits.update_credits(user_id=cached_quiz.user_id, credit_change=credit_payout, action_type=CreditActionType.QUIZ)
        message = f"Answer: *{query.data}* is correct! You receive *{credit_payout}* credits! [*{user_credits}* in total]"
        if core_utils.roll(CRITICAL_FAILURE_CHANCE) and (event := self.event_manager.get_random_event('quiz', 'success')):
            effect = event.apply_effect(amount=credit_payout)
            self.credits.update_credits(cached_quiz.user_id, effect.credit_change, CreditActionType.QUIZ)
            message += f"\n\n**Critical Success!** {effect.message}"

        message = stats_utils.escape_special_characters(message)
        await context.bot.send_message(chat_id=update.effective_chat.id, text=message, parse_mode=telegram.constants.ParseMode.MARKDOWN_V2, message_thread_id=query.message.message_thread_id)

    async def cmd_steal_graph(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        command_args = CommandArgs(args=context.args, expected_args=[ArgType.USER, ArgType.PERIOD], optional=[True, True], available_named_args={'all_attempts': ArgType.NONE})
        command_args = core_utils.parse_args(self.users_df, command_args)
        if command_args.error != '':
            await context.bot.send_message(chat_id=update.effective_chat.id, text=command_args.error, message_thread_id=update.message.message_thread_id)
            return

        filtered_credits_df = self.credits.credit_history_df[self.credits.credit_history_df['action_type'] == CreditActionType.STEAL.value]
        filtered_credits_df = stats_utils.filter_by_time_df(filtered_credits_df, command_args, 'timestamp')
        if 'all_attempts' not in command_args.named_args:
            filtered_credits_df = filtered_credits_df[filtered_credits_df['success'] == True]

        filtered_credits_df['robbing_username'] = filtered_credits_df['user_id'].apply(lambda x: self.users_map[x])
        filtered_credits_df['robbed_username'] = filtered_credits_df['target_user_id'].apply(lambda x: self.users_map[x])

        text = core_utils.generate_response_headline(command_args, label='Steal Graph')
        path = charts.create_bidirectional_relationship_graph(filtered_credits_df, 'robbing_username', 'robbed_username', 'Steal Network')
        current_message_type = MessageType.IMAGE
        await core_utils.send_message(update, context, current_message_type, path, text)
