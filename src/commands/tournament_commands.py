import asyncio
import logging

import telegram
from telegram import Update
from telegram.ext import ContextTypes

import src.core.utils as core_utils
import src.stats.utils as stats_utils
from src.config.assets import Assets
from src.config.constants import (
    TOURNAMENT_BET_TIMEOUT_SECONDS,
    TOURNAMENT_DEFAULT_ROUNDS,
    TOURNAMENT_JOIN_TIMEOUT_SECONDS,
    TOURNAMENT_MAX_ROUNDS,
    TOURNAMENT_SPIN_DELAY_SECONDS,
)
from src.config.enums import ArgType, MessageType, Table, TournamentState, TournamentType
from src.core.command_logger import CommandLogger
from src.core.job_persistance import JobPersistance
from src.models.core.bot_state import BotState
from src.models.core.credits import Credits
from src.models.data.command_args import CommandArgs
from src.models.db.db import DB
from src.models.tournaments.roulette_tournament import RouletteTournament

log = logging.getLogger(__name__)


class TournamentCommands:
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
        self.active_tournaments: dict[int, RouletteTournament] = {}

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
            TournamentType(tournament_type_str)
        except ValueError:
            valid = ", ".join(t.value for t in TournamentType)
            await core_utils.send_message(update, context, MessageType.TEXT, f"Invalid tournament type. Available: {valid}")
            return

        chat_id = update.effective_chat.id
        if chat_id in self.active_tournaments:
            await core_utils.send_message(update, context, MessageType.TEXT, "A tournament is already running in this chat.")
            return

        user_id = update.effective_user.id
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

        final_msg = tournament.get_final_results()

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
