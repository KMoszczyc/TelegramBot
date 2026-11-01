import asyncio
import logging

import telegram
from telegram import InputMediaPhoto, Update
from telegram.ext import ContextTypes

import src.core.utils as core_utils
import src.stats.utils as stats_utils
from src.config.assets import Assets
from src.config.constants import (
    POKER_ALL_IN_RUNOUT_DELAY_SECONDS,
    POKER_BOT_ACTION_DELAY_SECONDS,
    POKER_CALLBACK_PREFIX,
    POKER_NEXT_HAND_DELAY_SECONDS,
    POKER_STREET_REVEAL_DELAY_SECONDS,
    POKER_TURN_TIMEOUT_SECONDS,
    TOURNAMENT_BET_TIMEOUT_SECONDS,
    TOURNAMENT_DEFAULT_ROUNDS,
    TOURNAMENT_JOIN_TIMEOUT_SECONDS,
    TOURNAMENT_LOBBY_CALLBACK_PREFIX,
    TOURNAMENT_MAX_ROUNDS,
    TOURNAMENT_SPIN_DELAY_SECONDS,
)
from src.config.enums import ArgType, MessageType, PokerAction, Table, TournamentState, TournamentType
from src.core.command_logger import CommandLogger
from src.core.job_persistance import JobPersistance
from src.models.core.bot_state import BotState
from src.models.core.credits import Credits
from src.models.data.command_args import CommandArgs
from src.models.db.db import DB
from src.models.tournaments.base_tournament import BaseTournament
from src.models.tournaments.poker_table_renderer import render_poker_table
from src.models.tournaments.poker_tournament import PokerActionResult, PokerTournament
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
        self.active_tournaments: dict[int, BaseTournament] = {}

    async def cmd_poker(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Start a Texas Hold'em Poker game directly via /poker <buy_in> [rounds=5] [wait=120]."""
        command_args = CommandArgs(
            args=context.args,
            expected_args=[ArgType.POSITIVE_INT],
            optional=[False],
            available_named_args={"rounds": ArgType.POSITIVE_INT, "wait": ArgType.POSITIVE_INT},
            min_number=1,
            max_number=100_000_000,
        )
        command_args = core_utils.parse_args(self.users_df, command_args)
        if command_args.error != "":
            await core_utils.send_message(update, context, MessageType.TEXT, command_args.error)
            return

        await self._create_and_start_lobby(update, context, TournamentType.POKER, command_args)

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

        await self._create_and_start_lobby(update, context, tournament_type, command_args)

    async def _create_and_start_lobby(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        tournament_type: TournamentType,
        command_args: CommandArgs,
    ):
        if command_args.number is None:
            await core_utils.send_message(
                update,
                context,
                MessageType.TEXT,
                "Missing required argument: buy_in (e.g. /poker 100).",
            )
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

        if tournament_type == TournamentType.POKER:
            tournament: BaseTournament = PokerTournament(
                chat_id,
                thread_id,
                user_id,
                username,
                self.credits,
                buy_in,
                max_rounds,
                auto_runout=False,
                auto_bot_turns=False,
            )
            reply_markup = tournament.build_lobby_keyboard()
        else:
            tournament = RouletteTournament(chat_id, thread_id, user_id, username, self.credits, buy_in, max_rounds)
            reply_markup = None

        self.active_tournaments[chat_id] = tournament

        header = tournament.format_header()
        message = stats_utils.escape_special_characters(
            f"{header}\n\nBuy-in: *{buy_in}* credits | Rounds: *{max_rounds}*\n"
            f"*{username}* started the tournament!\n\n"
            f"Click *Join Table* (or type *join*) within {join_timeout}s to enter, *Add Bot* for AI opponents, or *Start Game* (or type *start*) to begin!"
        )
        await core_utils.send_message(update, context, MessageType.MARKDOWN_TEXT, message, reply_markup=reply_markup)

        context.job_queue.run_once(
            self._on_join_timeout,
            join_timeout,
            data={"chat_id": chat_id, "thread_id": thread_id},
            name=f"tournament_join_{chat_id}",
        )

    def _cancel_jobs(self, context: ContextTypes.DEFAULT_TYPE, job_name: str) -> None:
        if not context.job_queue:
            return
        for job in context.job_queue.get_jobs_by_name(job_name):
            job.schedule_removal()

    async def btn_tournament_lobby_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle [💺 Join Table], [🤖 Add Bot], and [▶️ Start Game] inline buttons on the lobby message."""
        query = update.callback_query
        if not query or not query.data:
            return

        chat_id = update.effective_chat.id
        tournament = self.active_tournaments.get(chat_id)
        if not tournament or not tournament.is_active or tournament.state != TournamentState.JOINING:
            await query.answer("This lobby is no longer active.", show_alert=True)
            return

        action = query.data.removeprefix(TOURNAMENT_LOBBY_CALLBACK_PREFIX)
        user_id = update.effective_user.id
        username = core_utils.get_username(update.effective_user.first_name, update.effective_user.last_name)

        if action == "join":
            response, ok = tournament.add_player(user_id, username)
            if not ok:
                await query.answer(response.replace("*", ""), show_alert=True)
                return
            await query.answer("Joined!")
            escaped = stats_utils.escape_special_characters(f"{tournament.format_header()}\n\n{response}")
            await context.bot.send_message(
                chat_id=chat_id,
                text=escaped,
                parse_mode=telegram.constants.ParseMode.MARKDOWN_V2,
                message_thread_id=tournament.thread_id,
            )

        elif action == "add_bot":
            if not isinstance(tournament, PokerTournament):
                await query.answer("AI bots are only available in Poker.", show_alert=True)
                return
            if user_id != tournament.host_user_id:
                await query.answer("Only the tournament host can add bots.", show_alert=True)
                return
            response, ok = tournament.add_bot_player()
            if not ok:
                await query.answer(response.replace("*", ""), show_alert=True)
                return
            await query.answer("Bot added!")
            escaped = stats_utils.escape_special_characters(f"{tournament.format_header()}\n\n{response}")
            await context.bot.send_message(
                chat_id=chat_id,
                text=escaped,
                parse_mode=telegram.constants.ParseMode.MARKDOWN_V2,
                message_thread_id=tournament.thread_id,
            )

        elif action == "start":
            if user_id != tournament.host_user_id:
                await query.answer("Only the tournament host can start the game early.", show_alert=True)
                return
            if isinstance(tournament, PokerTournament) and not tournament.has_enough_players():
                tournament.add_bot_player()
            if not tournament.has_enough_players():
                await query.answer("At least 2 players are needed to start.", show_alert=True)
                return
            await query.answer("Starting game!")
            self._cancel_jobs(context, f"tournament_join_{chat_id}")
            await self._announce_tournament_start(context, chat_id, tournament.thread_id)

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
        if isinstance(tournament, PokerTournament):
            await self._start_betting_round(context, chat_id, thread_id)
            return

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
        await self._start_betting_round(context, data["chat_id"], data["thread_id"])

    async def _start_betting_round(self, context: ContextTypes.DEFAULT_TYPE, chat_id: int, thread_id: int):
        tournament = self.active_tournaments.get(chat_id)
        if not tournament or not tournament.is_active:
            return

        if isinstance(tournament, PokerTournament):
            self._cancel_jobs(context, f"poker_turn_{chat_id}")
            self._cancel_jobs(context, f"poker_next_{chat_id}")
            tournament.start_betting_round()
            await self._update_or_repost_poker_table(context, tournament, re_render_image=True, force_new_message=True)
            if tournament.is_all_in_runout:
                await self._animate_all_in_runout(context, tournament)
            elif tournament.state == TournamentState.ROUND_RESULT:
                await self._handle_poker_hand_ended(context, tournament)
            elif tournament.has_pending_auto_turn():
                await self._run_poker_auto_turns(context, tournament)
            else:
                self._schedule_poker_turn_timer(context, tournament)
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

    def _schedule_poker_turn_timer(self, context: ContextTypes.DEFAULT_TYPE, tournament: PokerTournament) -> None:
        self._cancel_jobs(context, f"poker_turn_{tournament.chat_id}")
        if tournament.state == TournamentState.BETTING and tournament.current_turn_user_id is not None and context.job_queue:
            context.job_queue.run_once(
                self._on_poker_turn_timeout,
                POKER_TURN_TIMEOUT_SECONDS,
                data={"chat_id": tournament.chat_id, "thread_id": tournament.thread_id},
                name=f"poker_turn_{tournament.chat_id}",
            )

    async def _on_poker_turn_timeout(self, context: ContextTypes.DEFAULT_TYPE):
        data = context.job.data
        chat_id = data["chat_id"]
        tournament = self.active_tournaments.get(chat_id)
        if not isinstance(tournament, PokerTournament) or not tournament.is_active:
            return

        res = tournament.handle_turn_timeout()
        if res.valid:
            await self._after_poker_action(context, tournament, res)

    async def _on_poker_next_hand_timeout(self, context: ContextTypes.DEFAULT_TYPE):
        data = context.job.data
        chat_id, thread_id = data["chat_id"], data["thread_id"]
        tournament = self.active_tournaments.get(chat_id)
        if not isinstance(tournament, PokerTournament) or not tournament.is_active:
            return
        if tournament.state != TournamentState.ROUND_RESULT:
            return

        if tournament.is_last_round():
            await self._finish_tournament(context, chat_id, thread_id)
        else:
            await self._start_betting_round(context, chat_id, thread_id)

    async def _update_or_repost_poker_table(
        self,
        context: ContextTypes.DEFAULT_TYPE,
        tournament: PokerTournament,
        re_render_image: bool,
        force_new_message: bool = False,
    ) -> None:
        """Smart Edit / Repost: Edit table in-place if at bottom of chat; delete and repost if chat messages moved it up."""
        caption = stats_utils.escape_special_characters(tournament.format_table_caption())
        keyboard = tournament.build_table_keyboard()
        need_repost = force_new_message or tournament.table_message_id is None or tournament.chat_messages_since_table > 0

        if not need_repost and tournament.table_message_id is not None:
            try:
                if re_render_image:
                    png_bytes = render_poker_table(tournament)
                    await context.bot.edit_message_media(
                        chat_id=tournament.chat_id,
                        message_id=tournament.table_message_id,
                        media=InputMediaPhoto(
                            media=png_bytes,
                            caption=caption,
                            parse_mode=telegram.constants.ParseMode.MARKDOWN_V2,
                        ),
                        reply_markup=keyboard,
                    )
                else:
                    await context.bot.edit_message_caption(
                        chat_id=tournament.chat_id,
                        message_id=tournament.table_message_id,
                        caption=caption,
                        parse_mode=telegram.constants.ParseMode.MARKDOWN_V2,
                        reply_markup=keyboard,
                    )
                return
            except Exception as e:
                log.debug("Fallback to reposting poker table after edit error: %s", e)

        old_msg_id = tournament.table_message_id if not force_new_message else None
        png_bytes = render_poker_table(tournament)
        sent = await context.bot.send_photo(
            chat_id=tournament.chat_id,
            photo=png_bytes,
            caption=caption,
            parse_mode=telegram.constants.ParseMode.MARKDOWN_V2,
            reply_markup=keyboard,
            message_thread_id=tournament.thread_id,
        )
        tournament.table_message_id = sent.message_id
        tournament.chat_messages_since_table = 0

        if old_msg_id is not None:
            try:
                await context.bot.delete_message(chat_id=tournament.chat_id, message_id=old_msg_id)
            except Exception as e:
                log.debug("Could not delete old mid-hand poker table message %s: %s", old_msg_id, e)

    async def _animate_all_in_runout(self, context: ContextTypes.DEFAULT_TYPE, tournament: PokerTournament) -> None:
        while tournament.is_all_in_runout and tournament.is_active:
            await asyncio.sleep(POKER_ALL_IN_RUNOUT_DELAY_SECONDS)
            tournament.step_all_in_runout()
            await self._update_or_repost_poker_table(context, tournament, re_render_image=True)
        if tournament.state == TournamentState.ROUND_RESULT:
            await self._handle_poker_hand_ended(context, tournament)

    async def _run_poker_auto_turns(
        self,
        context: ContextTypes.DEFAULT_TYPE,
        tournament: PokerTournament,
    ) -> None:
        """Step through consecutive AI bot, AFK, or premove turns with a delay between bots."""
        safety = 0
        while tournament.is_active and tournament.has_pending_auto_turn() and not tournament.is_all_in_runout and safety < 32:
            safety += 1
            uid = tournament.current_turn_user_id
            if uid is None:
                break
            player = tournament.players[uid]
            if player.is_bot or player.is_afk:
                await asyncio.sleep(POKER_BOT_ACTION_DELAY_SECONDS)
                if not tournament.is_active or not tournament.has_pending_auto_turn():
                    break

            step_res = tournament.step_auto_turn()
            if step_res is None:
                break

            if tournament.is_all_in_runout:
                await self._update_or_repost_poker_table(context, tournament, re_render_image=True)
                await self._animate_all_in_runout(context, tournament)
                return

            if step_res.street_changed or (step_res.hand_ended and not step_res.won_by_fold):
                await asyncio.sleep(POKER_STREET_REVEAL_DELAY_SECONDS)
                await self._update_or_repost_poker_table(context, tournament, re_render_image=True)
            elif step_res.hand_ended:
                await self._update_or_repost_poker_table(context, tournament, re_render_image=True)
            elif tournament.has_pending_auto_turn():
                await self._update_or_repost_poker_table(context, tournament, re_render_image=False)
            else:
                await self._update_or_repost_poker_table(context, tournament, re_render_image=True)

        if tournament.state == TournamentState.ROUND_RESULT:
            await self._handle_poker_hand_ended(context, tournament)
        elif tournament.state == TournamentState.BETTING:
            self._schedule_poker_turn_timer(context, tournament)

    async def _after_poker_action(
        self,
        context: ContextTypes.DEFAULT_TYPE,
        tournament: PokerTournament,
        res: PokerActionResult,
    ) -> None:
        self._cancel_jobs(context, f"poker_turn_{tournament.chat_id}")

        if tournament.is_all_in_runout:
            await self._update_or_repost_poker_table(context, tournament, re_render_image=True)
            await self._animate_all_in_runout(context, tournament)
            return

        if res.street_changed or (res.hand_ended and not res.won_by_fold):
            await asyncio.sleep(POKER_STREET_REVEAL_DELAY_SECONDS)

        if res.hand_ended:
            await self._update_or_repost_poker_table(context, tournament, re_render_image=True)
            await self._handle_poker_hand_ended(context, tournament)
            return

        if tournament.has_pending_auto_turn():
            await self._update_or_repost_poker_table(context, tournament, re_render_image=res.street_changed)
            await self._run_poker_auto_turns(context, tournament)
        else:
            await self._update_or_repost_poker_table(context, tournament, re_render_image=True)
            self._schedule_poker_turn_timer(context, tournament)

    async def _handle_poker_hand_ended(self, context: ContextTypes.DEFAULT_TYPE, tournament: PokerTournament) -> None:
        self._cancel_jobs(context, f"poker_turn_{tournament.chat_id}")
        self._cancel_jobs(context, f"poker_next_{tournament.chat_id}")

        if tournament.is_last_round() and tournament.won_by_fold_user_id is None:
            await self._finish_tournament(context, tournament.chat_id, tournament.thread_id)
            return

        if context.job_queue:
            context.job_queue.run_once(
                self._on_poker_next_hand_timeout,
                POKER_NEXT_HAND_DELAY_SECONDS,
                data={"chat_id": tournament.chat_id, "thread_id": tournament.thread_id},
                name=f"poker_next_{tournament.chat_id}",
            )

    async def btn_poker_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle all inline button interactions on the poker table message."""
        query = update.callback_query
        if not query or not query.data:
            return

        chat_id = update.effective_chat.id
        tournament = self.active_tournaments.get(chat_id)
        if not isinstance(tournament, PokerTournament) or not tournament.is_active:
            await query.answer("This poker game is no longer active.", show_alert=True)
            return

        user_id = update.effective_user.id
        payload = query.data.removeprefix(POKER_CALLBACK_PREFIX)

        if payload == "cards":
            alert_text = tournament.get_player_cards_alert(user_id)
            await query.answer(alert_text, show_alert=True)
            return

        if payload == "sit_in":
            msg, ok = tournament.sit_back_in(user_id)
            await query.answer(msg, show_alert=not ok)
            if ok:
                await self._update_or_repost_poker_table(context, tournament, re_render_image=False)
            return

        if payload == "flex":
            msg, ok = tournament.reveal_winner_cards(user_id)
            await query.answer(msg, show_alert=not ok)
            if ok:
                await self._update_or_repost_poker_table(context, tournament, re_render_image=True)
            return

        if payload == "next_hand":
            if user_id not in tournament.players:
                await query.answer("Only seated players can advance the hand.", show_alert=True)
                return
            if tournament.state != TournamentState.ROUND_RESULT:
                await query.answer("Hand is still in progress.", show_alert=True)
                return
            await query.answer("Dealing next hand!")
            self._cancel_jobs(context, f"poker_next_{chat_id}")
            if tournament.is_last_round():
                await self._finish_tournament(context, chat_id, tournament.thread_id)
            else:
                await self._start_betting_round(context, chat_id, tournament.thread_id)
            return

        action_map = {
            "fold": (PokerAction.FOLD, None),
            "check": (PokerAction.CHECK, None),
            "call": (PokerAction.CALL, None),
            "all_in": (PokerAction.ALL_IN, None),
        }
        if payload in action_map:
            action, amount = action_map[payload]
        elif payload.startswith("raise:"):
            action = PokerAction.RAISE
            amount = int(payload.split(":", 1)[1])
        else:
            await query.answer("Unknown action.", show_alert=True)
            return

        if user_id != tournament.current_turn_user_id and action in (PokerAction.FOLD, PokerAction.CHECK, PokerAction.CALL):
            _, premove_msg = tournament.set_premove(user_id, action)
            await query.answer(premove_msg, show_alert=True)
            return

        res = tournament.process_action(user_id, action, raise_to_amount=amount)
        if not res.valid:
            await query.answer(res.message.replace("*", ""), show_alert=True)
            return

        await query.answer()
        await self._after_poker_action(context, tournament, res)

    async def _on_bet_timeout(self, context: ContextTypes.DEFAULT_TYPE):
        data = context.job.data
        await self._process_round(context, data["chat_id"], data["thread_id"])

    async def _process_round(self, context: ContextTypes.DEFAULT_TYPE, chat_id: int, thread_id: int):
        tournament = self.active_tournaments.get(chat_id)
        if not isinstance(tournament, RouletteTournament) or not tournament.is_active:
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

        self._cancel_jobs(context, f"poker_turn_{chat_id}")
        self._cancel_jobs(context, f"poker_next_{chat_id}")
        self._cancel_jobs(context, f"tournament_bet_{chat_id}")

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
        raw_text = update.message.text.strip()
        text = raw_text.lower()
        username = core_utils.get_username(update.effective_user.first_name, update.effective_user.last_name)

        if text == "join" and tournament.state == TournamentState.JOINING:
            response, _ = tournament.add_player(user_id, username)
            response = stats_utils.escape_special_characters(f"{tournament.format_header()}\n\n{response}")
            await context.bot.send_message(
                chat_id=chat_id,
                text=response,
                parse_mode=telegram.constants.ParseMode.MARKDOWN_V2,
                message_thread_id=update.message.message_thread_id,
            )
            return

        if text == "start" and tournament.state == TournamentState.JOINING:
            if user_id != tournament.host_user_id:
                response = "Only the tournament host can start early."
            else:
                if isinstance(tournament, PokerTournament) and not tournament.has_enough_players():
                    tournament.add_bot_player()
                if not tournament.has_enough_players():
                    response = "At least 2 players are needed to start the tournament."
                else:
                    self._cancel_jobs(context, f"tournament_join_{chat_id}")
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

        if isinstance(tournament, PokerTournament):
            tournament.chat_messages_since_table += 1
            if tournament.state == TournamentState.BETTING and user_id == tournament.current_turn_user_id and raw_text.isdigit():
                res = tournament.process_action(user_id, PokerAction.RAISE, raise_to_amount=int(raw_text))
                if res.valid:
                    await self._after_poker_action(context, tournament, res)
                else:
                    err_msg = stats_utils.escape_special_characters(res.message)
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=err_msg,
                        parse_mode=telegram.constants.ParseMode.MARKDOWN_V2,
                        message_thread_id=update.message.message_thread_id,
                    )
            return

        response = tournament.handle_game_message(user_id, raw_text)
        if response:
            response = stats_utils.escape_special_characters(f"{tournament.format_header()}\n\n{response}")
            await context.bot.send_message(
                chat_id=chat_id,
                text=response,
                parse_mode=telegram.constants.ParseMode.MARKDOWN_V2,
                message_thread_id=update.message.message_thread_id,
            )

            if isinstance(tournament, RouletteTournament) and tournament.all_bets_placed():
                self._cancel_jobs(context, f"tournament_bet_{chat_id}")
                await self._process_round(context, chat_id, update.message.message_thread_id)
