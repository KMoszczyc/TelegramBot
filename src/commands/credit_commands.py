import asyncio
import logging

from telegram import Update
from telegram.ext import ContextTypes

import src.core.utils as core_utils
import src.stats.utils as stats_utils
from src.commands.quiz_commands import QuizCommands
from src.commands.tournament_commands import TournamentCommands
from src.config.assets import Assets
from src.config.constants import (
    CRITICAL_FAILURE_CHANCE,
    CRITICAL_SUCCESS_CHANCE,
)
from src.config.enums import (
    BET_EVENTS,
    STEAL_EVENTS,
    ArgType,
    CreditActionType,
    MessageType,
    Table,
)
from src.core.command_logger import CommandLogger
from src.core.job_persistance import JobPersistance
from src.models.core.bot_state import BotState
from src.models.core.credits import Credits
from src.models.data.command_args import CommandArgs
from src.models.db.db import DB
from src.models.events.event_manager import EventManager
from src.models.games.roulette import Roulette
from src.stats import charts

log = logging.getLogger(__name__)


class CreditCommands(QuizCommands, TournamentCommands):
    """Handles credit-based gambling, theft, gifting, and leaderboard operations.

    Inherits from QuizCommands and TournamentCommands to maintain unified command
    compatibility across bot configurations and legacy test suites.
    """

    def __init__(
        self,
        command_logger: CommandLogger,
        job_persistance: JobPersistance,
        bot_state: BotState,
        credits: Credits,
        db: DB,
        assets: Assets,
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

        QuizCommands.__init__(self, command_logger, job_persistance, bot_state, credits, db, assets)
        TournamentCommands.__init__(self, command_logger, job_persistance, bot_state, credits, db, assets)

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
