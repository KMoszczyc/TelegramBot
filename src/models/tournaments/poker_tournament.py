import random
from collections import Counter
from dataclasses import dataclass, field

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from src.config.constants import (
    MAX_POKER_PLAYERS,
    POKER_ACTION_LOG_DISPLAY_COUNT,
    POKER_BLIND_ESCALATION_FACTOR,
    POKER_BLIND_ESCALATION_INTERVAL,
    POKER_BOT_NAMES,
    POKER_CALLBACK_PREFIX,
    POKER_INITIAL_ANTE_RATIO,
    POKER_INITIAL_BB_RATIO,
    POKER_INITIAL_SB_RATIO,
    POKER_MAX_CONSECUTIVE_TIMEOUTS,
    POKER_NEXT_HAND_DELAY_SECONDS,
    POKER_TURN_TIMEOUT_SECONDS,
    TOURNAMENT_LOBBY_CALLBACK_PREFIX,
)
from src.config.enums import PokerAction, PokerHandRank, PokerStreet, TournamentState, TournamentType
from src.models.games.poker_hand import Card, EvaluatedHand, create_shuffled_deck, evaluate_best_hand
from src.models.tournaments.base_tournament import BaseTournament, TournamentPlayer


@dataclass
class PokerPlayer(TournamentPlayer):
    hole_cards: list[Card] = field(default_factory=list)
    current_bet: int = 0
    total_hand_bet: int = 0
    in_current_hand: bool = False
    folded: bool = False
    all_in: bool = False
    acted_this_street: bool = False
    consecutive_timeouts: int = 0
    is_afk: bool = False
    revealed_cards: bool = False
    last_action_label: str = ""
    premove: PokerAction | None = None
    premove_street_bet: int = 0
    hands_won: int = 0
    hands_played: int = 0
    biggest_pot_won: int = 0
    best_hand_desc: str = ""


@dataclass
class SidePot:
    amount: int
    eligible_user_ids: list[int]


@dataclass
class PokerActionResult:
    valid: bool
    message: str
    street_changed: bool = False
    hand_ended: bool = False
    won_by_fold: bool = False


class PokerTournament(BaseTournament):
    """Multiplayer No-Limit Texas Hold'em Poker tournament."""

    max_players: int | None = MAX_POKER_PLAYERS

    def __init__(
        self,
        chat_id: int,
        thread_id: int,
        host_user_id: int,
        host_username: str,
        credits_obj,
        buy_in: int,
        max_rounds: int,
        rng: random.Random | None = None,
        auto_runout: bool = True,
        auto_bot_turns: bool = True,
    ):
        self._rng = rng
        self.auto_runout = auto_runout
        self.auto_bot_turns = auto_bot_turns
        self.is_all_in_runout: bool = False
        self.players: dict[int, PokerPlayer] = {}
        self.seat_order: list[int] = []
        self.street: PokerStreet = PokerStreet.PRE_FLOP
        self.deck: list[Card] = []
        self.community_cards: list[Card] = []
        self.pot: int = 0
        self.current_street_bet: int = 0
        self.min_raise_increment: int = 0
        self.dealer_user_id: int | None = None
        self.sb_user_id: int | None = None
        self.bb_user_id: int | None = None
        self.current_turn_user_id: int | None = None
        self.street_action_log: list[str] = []
        self.last_hand_summary: str = ""
        self.last_hand_winners: list[int] = []
        self.won_by_fold_user_id: int | None = None
        self.table_message_id: int | None = None
        self.chat_messages_since_table: int = 0
        super().__init__(chat_id, thread_id, host_user_id, host_username, credits_obj, buy_in, max_rounds)

    def _create_player(self, user_id: int, username: str, is_host: bool) -> PokerPlayer:
        return PokerPlayer(user_id=user_id, username=username, is_host=is_host, tournament_credits=self.buy_in)

    def add_bot_player(self) -> tuple[str, bool]:
        """Add a casino-funded AI bot opponent to the poker table."""
        if self.state != TournamentState.JOINING:
            return "Tournament is not accepting players right now.", False

        if self.max_players is not None and len(self.players) >= self.max_players:
            return f"Table is full (max *{self.max_players}* players).", False

        existing_bot_ids = [uid for uid in self.players if uid < 0]
        bot_id = min(existing_bot_ids, default=0) - 1
        existing_names = {p.username for p in self.players.values()}
        available_names = [name for name in POKER_BOT_NAMES if name not in existing_names]
        bot_name = available_names[0] if available_names else f"🤖 Bot {abs(bot_id)}"

        self.players[bot_id] = PokerPlayer(
            user_id=bot_id,
            username=bot_name,
            is_host=False,
            tournament_credits=self.buy_in,
            is_bot=True,
        )
        return f"*{bot_name}* joined the table! [{len(self.players)} players]", True

    def get_human_player_count(self) -> int:
        return sum(1 for p in self.players.values() if not p.is_bot)

    def get_active_human_player_count(self) -> int:
        return sum(1 for p in self.players.values() if not p.is_bot and p.tournament_credits > 0)

    @property
    def tournament_type(self) -> TournamentType:
        return TournamentType.POKER

    def format_header(self) -> str:
        round_info = f" — Hand {self.round_number}/{self.max_rounds}" if self.round_number > 0 else ""
        return f"🃏 *Texas Hold'em Poker*{round_info}"

    def get_blinds_and_ante(self, round_num: int | None = None) -> tuple[int, int, int]:
        """Return (small_blind, big_blind, ante) for the specified round number."""
        r = max(1, round_num if round_num is not None else self.round_number)
        level = (r - 1) // POKER_BLIND_ESCALATION_INTERVAL
        scale = POKER_BLIND_ESCALATION_FACTOR**level

        base_sb = max(1, round(self.buy_in * POKER_INITIAL_SB_RATIO))
        base_bb = max(2, round(self.buy_in * POKER_INITIAL_BB_RATIO))
        base_ante = round(self.buy_in * POKER_INITIAL_ANTE_RATIO)

        sb = max(1, round(base_sb * scale))
        bb = max(sb * 2, round(base_bb * scale))
        ante = round(base_ante * scale)
        return sb, bb, ante

    def _ensure_seat_order(self) -> None:
        for uid in self.players:
            if uid not in self.seat_order:
                self.seat_order.append(uid)

    def _active_seat_uids(self) -> list[int]:
        self._ensure_seat_order()
        return [uid for uid in self.seat_order if self.players[uid].tournament_credits > 0]

    def _hand_uids(self) -> list[int]:
        self._ensure_seat_order()
        return [uid for uid in self.seat_order if self.players[uid].in_current_hand]

    def _non_folded_uids(self) -> list[int]:
        return [uid for uid in self._hand_uids() if not self.players[uid].folded]

    def _can_act_uids(self) -> list[int]:
        return [uid for uid in self._hand_uids() if not self.players[uid].folded and not self.players[uid].all_in]

    def _next_uid_in_list(self, uid_list: list[int], after_uid: int | None) -> int:
        if not uid_list:
            raise ValueError("uid_list cannot be empty")
        if after_uid is None:
            return uid_list[0]
        if after_uid in uid_list:
            idx = uid_list.index(after_uid)
            return uid_list[(idx + 1) % len(uid_list)]
        self._ensure_seat_order()
        if after_uid in self.seat_order:
            start_idx = self.seat_order.index(after_uid)
            for offset in range(1, len(self.seat_order) + 1):
                candidate = self.seat_order[(start_idx + offset) % len(self.seat_order)]
                if candidate in uid_list:
                    return candidate
        return uid_list[0]

    def _post_forced_bet(self, player: PokerPlayer, amount: int, count_as_street_bet: bool) -> int:
        actual = min(amount, player.tournament_credits)
        player.tournament_credits -= actual
        player.total_hand_bet += actual
        if count_as_street_bet:
            player.current_bet += actual
        self.pot += actual
        if player.tournament_credits == 0:
            player.all_in = True
        return actual

    def start_betting_round(self) -> str:
        """Start a new poker hand (round)."""
        self.round_number += 1
        self.state = TournamentState.BETTING
        self.street = PokerStreet.PRE_FLOP
        self.community_cards = []
        self.pot = 0
        self.current_street_bet = 0
        self.street_action_log = []
        self.last_hand_summary = ""
        self.last_hand_winners = []
        self.won_by_fold_user_id = None
        self.chat_messages_since_table = 0
        self.is_all_in_runout = False

        self._ensure_seat_order()
        active_uids = self._active_seat_uids()

        for p in self.players.values():
            p.hole_cards = []
            p.current_bet = 0
            p.total_hand_bet = 0
            p.in_current_hand = p.user_id in active_uids
            p.folded = False
            p.all_in = False
            p.acted_this_street = False
            p.revealed_cards = False
            p.last_action_label = ""
            p.premove = None
            p.premove_street_bet = 0
            p.best_hand_desc = ""
            if p.in_current_hand:
                p.hands_played += 1
                p.total_bets += 1

        sb, bb, ante = self.get_blinds_and_ante()
        self.min_raise_increment = bb

        self.dealer_user_id = self._next_uid_in_list(active_uids, self.dealer_user_id)
        if len(active_uids) == 2:
            self.sb_user_id = self.dealer_user_id
            self.bb_user_id = self._next_uid_in_list(active_uids, self.dealer_user_id)
        else:
            self.sb_user_id = self._next_uid_in_list(active_uids, self.dealer_user_id)
            self.bb_user_id = self._next_uid_in_list(active_uids, self.sb_user_id)

        if ante > 0:
            for uid in active_uids:
                self._post_forced_bet(self.players[uid], ante, count_as_street_bet=False)
            self.street_action_log.append(f"Ante *{ante}* posted by all players")

        sb_player = self.players[self.sb_user_id]
        bb_player = self.players[self.bb_user_id]
        sb_posted = self._post_forced_bet(sb_player, sb, count_as_street_bet=True)
        bb_posted = self._post_forced_bet(bb_player, bb, count_as_street_bet=True)
        sb_player.last_action_label = f"SB {sb_posted}" + (" (All-In)" if sb_player.all_in else "")
        bb_player.last_action_label = f"BB {bb_posted}" + (" (All-In)" if bb_player.all_in else "")

        self.current_street_bet = max(sb_posted, bb_posted)
        self.street_action_log.append(f"*{sb_player.username}* posts SB *{sb_posted}* • *{bb_player.username}* posts BB *{bb_posted}*")

        self.deck = create_shuffled_deck(self._rng)
        for uid in active_uids:
            self.players[uid].hole_cards = [self.deck.pop(), self.deck.pop()]

        can_act = self._can_act_uids()
        if len(can_act) == 0 or (len(can_act) == 1 and self.players[can_act[0]].current_bet >= self.current_street_bet):
            self.current_turn_user_id = None
            self._run_out_remaining_streets()
        else:
            self.current_turn_user_id = self._next_uid_in_list(can_act, self.bb_user_id)
            self._process_afk_chain_if_needed()

        return self.format_table_caption()

    def get_to_call(self, user_id: int) -> int:
        player = self.players.get(user_id)
        if not player:
            return 0
        return max(0, self.current_street_bet - player.current_bet)

    def get_raise_presets(self, user_id: int) -> dict[str, int]:
        """Return valid raise target amounts ('min', 'half_pot', 'pot', 'all_in') for the player."""
        player = self.players.get(user_id)
        if not player or player.folded or player.all_in:
            return {}
        to_call = self.get_to_call(user_id)
        max_total_street_bet = player.current_bet + player.tournament_credits
        if player.tournament_credits <= to_call:
            return {}

        min_raise_to = self.current_street_bet + max(1, self.min_raise_increment)
        pot_after_call = self.pot + to_call
        half_pot_raise_to = self.current_street_bet + max(self.min_raise_increment, round(pot_after_call * 0.5))
        pot_raise_to = self.current_street_bet + max(self.min_raise_increment, pot_after_call)

        presets: dict[str, int] = {}
        seen_amounts: set[int] = set()

        for key, target in (("min", min_raise_to), ("half_pot", half_pot_raise_to), ("pot", pot_raise_to)):
            if target < max_total_street_bet and target not in seen_amounts:
                presets[key] = target
                seen_amounts.add(target)

        presets["all_in"] = max_total_street_bet
        return presets

    def process_action(
        self,
        user_id: int,
        action: PokerAction,
        raise_to_amount: int | None = None,
        is_timeout: bool = False,
        is_premove: bool = False,
    ) -> PokerActionResult:
        """Process a player's betting action on their turn."""
        if self.state != TournamentState.BETTING:
            return PokerActionResult(False, "No active betting round right now.")

        if user_id not in self.players:
            return PokerActionResult(False, "You are not seated at this poker table.")

        if user_id != self.current_turn_user_id:
            return PokerActionResult(False, "It is not your turn to act.")

        player = self.players[user_id]
        player.premove = None
        player.premove_street_bet = 0
        if not is_timeout:
            player.consecutive_timeouts = 0
            player.is_afk = False

        to_call = self.get_to_call(user_id)
        max_street_bet = player.current_bet + player.tournament_credits
        suffix = " (Timeout)" if is_timeout else (" (⚡ Premove)" if is_premove else "")

        if action == PokerAction.FOLD:
            player.folded = True
            player.acted_this_street = True
            player.last_action_label = f"Fold{suffix}"
            self.street_action_log.append(f"❌ *{player.username}* folds{suffix}")

        elif action == PokerAction.CHECK:
            if to_call > 0:
                return PokerActionResult(False, f"Cannot check — *{to_call}* credits needed to call.")
            player.acted_this_street = True
            player.last_action_label = f"Check{suffix}"
            self.street_action_log.append(f"✅ *{player.username}* checks{suffix}")

        elif action == PokerAction.CALL:
            if to_call == 0:
                return self.process_action(user_id, PokerAction.CHECK, is_timeout=is_timeout, is_premove=is_premove)
            added = self._post_forced_bet(player, to_call, count_as_street_bet=True)
            player.acted_this_street = True
            if player.all_in:
                player.last_action_label = f"All-In {player.current_bet}"
                self.street_action_log.append(f"🔥 *{player.username}* calls All-In (*{added}*){suffix}")
            else:
                player.last_action_label = f"Call {added}{suffix}"
                self.street_action_log.append(f"📞 *{player.username}* calls *{added}*{suffix}")

        elif action == PokerAction.ALL_IN:
            if player.tournament_credits <= 0:
                return PokerActionResult(False, "You have no credits left to bet.")
            added = self._post_forced_bet(player, player.tournament_credits, count_as_street_bet=True)
            player.acted_this_street = True
            player.last_action_label = f"All-In {player.current_bet}"
            if player.current_bet > self.current_street_bet:
                raise_diff = player.current_bet - self.current_street_bet
                if raise_diff >= self.min_raise_increment:
                    self.min_raise_increment = raise_diff
                self.current_street_bet = player.current_bet
                self._invalidate_other_players_on_raise(user_id)
                self.street_action_log.append(f"🔥 *{player.username}* goes All-In to *{player.current_bet}*!")
            else:
                self.street_action_log.append(f"🔥 *{player.username}* calls All-In (*{added}*)!")

        elif action == PokerAction.RAISE:
            if player.tournament_credits <= to_call:
                return self.process_action(user_id, PokerAction.ALL_IN, is_timeout=is_timeout, is_premove=is_premove)

            min_raise_to = self.current_street_bet + max(1, self.min_raise_increment)
            target = raise_to_amount if raise_to_amount is not None else min_raise_to
            if target >= max_street_bet:
                return self.process_action(user_id, PokerAction.ALL_IN, is_timeout=is_timeout, is_premove=is_premove)
            if target < min_raise_to:
                return PokerActionResult(False, f"Minimum raise is to *{min_raise_to}* credits.")

            needed = target - player.current_bet
            self._post_forced_bet(player, needed, count_as_street_bet=True)
            raise_diff = player.current_bet - self.current_street_bet
            self.min_raise_increment = max(self.min_raise_increment, raise_diff)
            self.current_street_bet = player.current_bet
            player.acted_this_street = True
            player.last_action_label = f"Raise {player.current_bet}"
            self._invalidate_other_players_on_raise(user_id)
            self.street_action_log.append(f"⬆️ *{player.username}* raises to *{player.current_bet}*")

        player.bet_history.append((self.round_number, self.street.value, action.value, player.current_bet))
        return self._advance_after_action()

    def handle_turn_timeout(self) -> PokerActionResult:
        """Auto-check (if free) or auto-fold the current player on turn timeout, marking AFK after 2 timeouts."""
        if self.state != TournamentState.BETTING or self.current_turn_user_id is None:
            return PokerActionResult(False, "No active turn to time out.")

        uid = self.current_turn_user_id
        player = self.players[uid]
        player.consecutive_timeouts += 1
        if player.consecutive_timeouts >= POKER_MAX_CONSECUTIVE_TIMEOUTS:
            player.is_afk = True

        to_call = self.get_to_call(uid)
        auto_action = PokerAction.CHECK if to_call == 0 else PokerAction.FOLD
        return self.process_action(uid, auto_action, is_timeout=True)

    def sit_back_in(self, user_id: int) -> tuple[str, bool]:
        player = self.players.get(user_id)
        if not player:
            return "You are not in this poker game.", False
        player.is_afk = False
        player.consecutive_timeouts = 0
        return f"{player.username} is back at the table!", True

    def _estimate_bot_hand_strength(self, bot: PokerPlayer) -> float:
        """Return a heuristic hand strength in [0.0, 1.0] for an AI bot."""
        if not bot.hole_cards:
            return 0.2
        hole_ranks = sorted((c.rank for c in bot.hole_cards), reverse=True)
        r1, r2 = hole_ranks[0], hole_ranks[1]
        suited = bot.hole_cards[0].suit == bot.hole_cards[1].suit

        if not self.community_cards:
            if r1 == r2:
                return min(0.95, 0.54 + (r1 - 2) * 0.034)
            base = ((r1 + r2 - 4) / 24.0) * 0.56
            if suited:
                base += 0.08
            if r1 - r2 <= 2:
                base += 0.06
            return max(0.15, min(0.85, base))

        all_cards = bot.hole_cards + self.community_cards
        evaluated = evaluate_best_hand(all_cards)
        cat = evaluated.rank_category

        if cat >= PokerHandRank.STRAIGHT:
            return 0.92
        if cat == PokerHandRank.THREE_OF_A_KIND:
            return 0.84
        if cat == PokerHandRank.TWO_PAIR:
            top_pair, second_pair = evaluated.tiebreakers[0], evaluated.tiebreakers[1]
            if r1 in (top_pair, second_pair) or r2 in (top_pair, second_pair):
                return 0.76
            return 0.36
        if cat == PokerHandRank.ONE_PAIR:
            pair_rank = evaluated.tiebreakers[0]
            if pair_rank in (r1, r2):
                return 0.52 + (pair_rank - 2) * 0.015
            return 0.30 + (r1 - 2) * 0.008

        suit_counts = Counter(c.suit for c in all_cards)
        has_flush_draw = len(self.community_cards) < 5 and max(suit_counts.values()) == 4
        draw_bonus = 0.14 if has_flush_draw else 0.0
        return min(0.55, 0.18 + (r1 - 2) * 0.012 + draw_bonus)

    def _decide_bot_action(self, bot: PokerPlayer) -> tuple[PokerAction, int | None]:
        """Choose an action and optional raise target for an AI bot player."""
        rng = self._rng if self._rng is not None else random
        strength = self._estimate_bot_hand_strength(bot) + (rng.random() - 0.5) * 0.10
        to_call = self.get_to_call(bot.user_id)
        presets = self.get_raise_presets(bot.user_id)

        if to_call == 0:
            if presets and (strength >= 0.65 or rng.random() < 0.10):
                if strength >= 0.85 and "pot" in presets:
                    return PokerAction.RAISE, presets["pot"]
                if "half_pot" in presets:
                    return PokerAction.RAISE, presets["half_pot"]
                if "min" in presets:
                    return PokerAction.RAISE, presets["min"]
            return PokerAction.CHECK, None

        pot_odds = to_call / max(1, self.pot + to_call)
        stack_ratio = to_call / max(1, bot.tournament_credits)
        call_threshold = min(0.75, 0.24 + pot_odds * 0.65 + stack_ratio * 0.18)

        if strength >= 0.78 and presets and stack_ratio < 0.6:
            if strength >= 0.90 and "pot" in presets:
                return PokerAction.RAISE, presets["pot"]
            if "min" in presets:
                return PokerAction.RAISE, presets["min"]

        if strength >= call_threshold or (to_call <= self.min_raise_increment and strength >= 0.34):
            return PokerAction.CALL, None

        return PokerAction.FOLD, None

    def _invalidate_other_players_on_raise(self, raiser_user_id: int) -> None:
        """Re-open street action and cancel any Check/Call premoves invalidated by a higher bet."""
        for other_uid in self._can_act_uids():
            if other_uid != raiser_user_id:
                other_p = self.players[other_uid]
                other_p.acted_this_street = False
                if other_p.premove in (PokerAction.CHECK, PokerAction.CALL) and other_p.premove_street_bet < self.current_street_bet:
                    other_p.premove = None
                    other_p.premove_street_bet = 0

    def set_premove(self, user_id: int, action: PokerAction) -> tuple[bool, str]:
        """Set or toggle off an out-of-turn premove for a human player."""
        if self.state != TournamentState.BETTING:
            return False, "No active betting round right now."
        if user_id not in self.players:
            return False, "You are not seated at this poker table."
        player = self.players[user_id]
        if not player.in_current_hand or player.folded or player.all_in:
            return False, "You cannot set a premove right now."

        to_call = self.get_to_call(user_id)
        normalized = PokerAction.CHECK if (action == PokerAction.CALL and to_call == 0) else action

        if player.premove == normalized:
            player.premove = None
            player.premove_street_bet = 0
            return False, "⚡ Premove cancelled."

        if normalized == PokerAction.CHECK:
            if to_call > 0:
                return False, f"⚠️ Cannot premove Check — {to_call} needed to call."
            player.premove = PokerAction.CHECK
            player.premove_street_bet = self.current_street_bet
            return True, "⚡ Premove set: Check (auto-cancels if someone bets)"

        if normalized == PokerAction.CALL:
            player.premove = PokerAction.CALL
            player.premove_street_bet = self.current_street_bet
            return True, f"⚡ Premove set: Call {to_call} (auto-cancels if raised)"

        if normalized == PokerAction.FOLD:
            player.premove = PokerAction.FOLD
            player.premove_street_bet = self.current_street_bet
            return True, "⚡ Premove set: Check / Fold"

        return False, "Unsupported premove action."

    def _peek_premove_action(self, player: PokerPlayer) -> PokerAction | None:
        if player.premove is None:
            return None
        to_call = self.get_to_call(player.user_id)
        if player.premove == PokerAction.CHECK:
            return PokerAction.CHECK if to_call == 0 else None
        if player.premove == PokerAction.CALL:
            if self.current_street_bet > player.premove_street_bet:
                return None
            return PokerAction.CHECK if to_call == 0 else PokerAction.CALL
        if player.premove == PokerAction.FOLD:
            return PokerAction.CHECK if to_call == 0 else PokerAction.FOLD
        return None

    def has_pending_auto_turn(self) -> bool:
        """Return True if the current turn belongs to an AI bot, AFK player, or player with a valid premove."""
        if self.state != TournamentState.BETTING or self.current_turn_user_id is None:
            return False
        player = self.players[self.current_turn_user_id]
        return player.is_bot or player.is_afk or self._peek_premove_action(player) is not None

    def step_auto_turn(self) -> PokerActionResult | None:
        """Execute a single bot, premove, or AFK action for the current turn player."""
        if not self.has_pending_auto_turn() or self.current_turn_user_id is None:
            return None
        uid = self.current_turn_user_id
        player = self.players[uid]
        if player.is_bot:
            bot_action, raise_amt = self._decide_bot_action(player)
            return self.process_action(uid, bot_action, raise_to_amount=raise_amt, is_timeout=False)
        pre_action = self._peek_premove_action(player)
        player.premove = None
        player.premove_street_bet = 0
        if pre_action is not None:
            return self.process_action(uid, pre_action, is_timeout=False, is_premove=True)
        if player.is_afk:
            to_call = self.get_to_call(uid)
            auto_action = PokerAction.CHECK if to_call == 0 else PokerAction.FOLD
            return self.process_action(uid, auto_action, is_timeout=True)
        return None

    def _process_afk_chain_if_needed(self) -> PokerActionResult | None:
        """If auto_bot_turns is enabled, immediately execute consecutive bot/AFK/premove turns."""
        if not self.auto_bot_turns:
            return None
        last_result: PokerActionResult | None = None
        safety = 0
        while self.has_pending_auto_turn() and safety < 32:
            safety += 1
            last_result = self.step_auto_turn()
        return last_result

    def _is_street_betting_complete(self) -> bool:
        can_act = self._can_act_uids()
        if not can_act:
            return True
        for uid in can_act:
            p = self.players[uid]
            if not p.acted_this_street or p.current_bet < self.current_street_bet:
                return False
        return True

    def _advance_after_action(self) -> PokerActionResult:
        non_folded = self._non_folded_uids()
        if len(non_folded) == 1:
            winner_uid = non_folded[0]
            self._resolve_fold_win(winner_uid)
            return PokerActionResult(
                valid=True,
                message=self.last_hand_summary,
                street_changed=False,
                hand_ended=True,
                won_by_fold=True,
            )

        if self._is_street_betting_complete():
            self._advance_to_next_street()
            afk_res = self._process_afk_chain_if_needed()
            if afk_res is not None:
                return PokerActionResult(
                    valid=True,
                    message=afk_res.message,
                    street_changed=True,
                    hand_ended=afk_res.hand_ended,
                    won_by_fold=afk_res.won_by_fold,
                )
            return PokerActionResult(
                valid=True,
                message=self.format_table_caption(),
                street_changed=True,
                hand_ended=(self.state == TournamentState.ROUND_RESULT),
                won_by_fold=False,
            )

        can_act = self._can_act_uids()
        self.current_turn_user_id = self._next_uid_in_list(can_act, self.current_turn_user_id)
        afk_res = self._process_afk_chain_if_needed()
        if afk_res is not None:
            return afk_res

        return PokerActionResult(
            valid=True,
            message=self.format_table_caption(),
            street_changed=False,
            hand_ended=False,
            won_by_fold=False,
        )

    def _advance_to_next_street(self) -> None:
        _, bb, _ = self.get_blinds_and_ante()
        for p in self.players.values():
            p.current_bet = 0
            p.acted_this_street = False
            p.premove = None
            p.premove_street_bet = 0
            if not p.folded and not p.all_in and p.in_current_hand:
                p.last_action_label = ""

        self.current_street_bet = 0
        self.min_raise_increment = bb

        can_act = self._can_act_uids()
        if len(can_act) <= 1 and not self.auto_runout:
            self._run_out_remaining_streets()
            return

        if self.street == PokerStreet.PRE_FLOP:
            self.street = PokerStreet.FLOP
            self.community_cards.extend([self.deck.pop(), self.deck.pop(), self.deck.pop()])
            board_str = " ".join(f"*{c.emoji_str}*" for c in self.community_cards)
            self.street_action_log.append(f"🃏 *Flop:* {board_str}")
        elif self.street == PokerStreet.FLOP:
            self.street = PokerStreet.TURN
            self.community_cards.append(self.deck.pop())
            board_str = " ".join(f"*{c.emoji_str}*" for c in self.community_cards)
            self.street_action_log.append(f"🃏 *Turn:* {board_str}")
        elif self.street == PokerStreet.TURN:
            self.street = PokerStreet.RIVER
            self.community_cards.append(self.deck.pop())
            board_str = " ".join(f"*{c.emoji_str}*" for c in self.community_cards)
            self.street_action_log.append(f"🃏 *River:* {board_str}")
        elif self.street == PokerStreet.RIVER:
            self._resolve_showdown()
            return

        if len(can_act) <= 1:
            self._run_out_remaining_streets()
        else:
            self.current_turn_user_id = self._next_uid_in_list(can_act, self.dealer_user_id)

    def reveal_all_in_hole_cards(self) -> None:
        """Flip all remaining non-folded players' hole cards face-up for an All-In runout."""
        for uid in self._non_folded_uids():
            p = self.players[uid]
            p.revealed_cards = True
            p.best_hand_desc = evaluate_best_hand(p.hole_cards + self.community_cards).description

    def step_all_in_runout(self) -> bool:
        """Advance an All-In runout by one street; return True when Showdown is reached."""
        if self.street == PokerStreet.PRE_FLOP:
            self.street = PokerStreet.FLOP
            self.community_cards.extend([self.deck.pop(), self.deck.pop(), self.deck.pop()])
            board_str = " ".join(f"*{c.emoji_str}*" for c in self.community_cards)
            self.street_action_log.append(f"🃏 *Flop:* {board_str}")
            self.reveal_all_in_hole_cards()
            return False
        if self.street == PokerStreet.FLOP:
            self.street = PokerStreet.TURN
            self.community_cards.append(self.deck.pop())
            board_str = " ".join(f"*{c.emoji_str}*" for c in self.community_cards)
            self.street_action_log.append(f"🃏 *Turn:* {board_str}")
            self.reveal_all_in_hole_cards()
            return False
        if self.street == PokerStreet.TURN:
            self.street = PokerStreet.RIVER
            self.community_cards.append(self.deck.pop())
            board_str = " ".join(f"*{c.emoji_str}*" for c in self.community_cards)
            self.street_action_log.append(f"🃏 *River:* {board_str}")
            self.reveal_all_in_hole_cards()
            return False

        self.is_all_in_runout = False
        self._resolve_showdown()
        return True

    def _run_out_remaining_streets(self) -> None:
        self.current_turn_user_id = None
        self.reveal_all_in_hole_cards()
        if not self.auto_runout:
            self.is_all_in_runout = True
            return
        while not self.step_all_in_runout():
            pass

    def calculate_side_pots(self) -> list[SidePot]:
        """Compute main pot and side pots from players' total_hand_bet contributions."""
        hand_players = [self.players[uid] for uid in self._hand_uids() if self.players[uid].total_hand_bet > 0]
        if not hand_players:
            return []

        levels = sorted({p.total_hand_bet for p in hand_players})
        pots: list[SidePot] = []
        prev_level = 0
        last_eligible: list[int] = [uid for uid in self._non_folded_uids()]

        for level in levels:
            slice_amount = sum(max(0, min(p.total_hand_bet, level) - prev_level) for p in hand_players)
            prev_level = level
            if slice_amount <= 0:
                continue

            eligible = [uid for uid in self._non_folded_uids() if self.players[uid].total_hand_bet >= level]
            if not eligible:
                eligible = last_eligible
            else:
                last_eligible = eligible

            if pots and pots[-1].eligible_user_ids == eligible:
                pots[-1].amount += slice_amount
            else:
                pots.append(SidePot(amount=slice_amount, eligible_user_ids=list(eligible)))

        return pots

    def _order_from_left_of_dealer(self, user_ids: list[int]) -> list[int]:
        if len(user_ids) <= 1 or self.dealer_user_id not in self.seat_order:
            return list(user_ids)
        dealer_idx = self.seat_order.index(self.dealer_user_id)
        n = len(self.seat_order)
        ordered = []
        for offset in range(1, n + 1):
            candidate = self.seat_order[(dealer_idx + offset) % n]
            if candidate in user_ids:
                ordered.append(candidate)
        return ordered

    def _resolve_fold_win(self, winner_uid: int) -> None:
        self.state = TournamentState.ROUND_RESULT
        self.current_turn_user_id = None
        self.won_by_fold_user_id = winner_uid if not self.players[winner_uid].is_bot else None
        self.last_hand_winners = [winner_uid]

        winner = self.players[winner_uid]
        winner.tournament_credits += self.pot
        winner.hands_won += 1
        winner.correct_bets += 1
        winner.biggest_pot_won = max(winner.biggest_pot_won, self.pot)
        winner.last_action_label = f"Won +{self.pot}"

        self.last_hand_summary = f"🏆 *{winner.username}* wins the pot of *{self.pot}* credits (all other players folded)!"

    def _resolve_showdown(self) -> None:
        self.state = TournamentState.ROUND_RESULT
        self.street = PokerStreet.SHOWDOWN
        self.current_turn_user_id = None
        self.won_by_fold_user_id = None

        non_folded = self._non_folded_uids()
        evaluated: dict[int, EvaluatedHand] = {}
        for uid in non_folded:
            p = self.players[uid]
            p.revealed_cards = True
            hand_eval = evaluate_best_hand(p.hole_cards + self.community_cards)
            evaluated[uid] = hand_eval
            p.best_hand_desc = hand_eval.description

        side_pots = self.calculate_side_pots()
        winnings_by_uid: dict[int, int] = {uid: 0 for uid in non_folded}

        for pot in side_pots:
            best_eval = max(evaluated[uid] for uid in pot.eligible_user_ids)
            winners = [uid for uid in pot.eligible_user_ids if evaluated[uid] == best_eval]
            ordered_winners = self._order_from_left_of_dealer(winners)
            share = pot.amount // len(ordered_winners)
            remainder = pot.amount % len(ordered_winners)
            for idx, w_uid in enumerate(ordered_winners):
                winnings_by_uid[w_uid] += share + (1 if idx < remainder else 0)

        self.last_hand_winners = [uid for uid, won in winnings_by_uid.items() if won > 0]
        summary_lines = ["🃏 *Showdown Results:*"]
        for uid in non_folded:
            p = self.players[uid]
            cards_str = " ".join(c.emoji_str for c in p.hole_cards)
            won = winnings_by_uid[uid]
            if won > 0:
                p.tournament_credits += won
                p.hands_won += 1
                p.correct_bets += 1
                p.biggest_pot_won = max(p.biggest_pot_won, won)
                p.last_action_label = f"Won +{won}"
                summary_lines.append(f"🏆 *{p.username}* ({cards_str}) — *{p.best_hand_desc}* → *+{won}* credits!")
            else:
                p.last_action_label = p.best_hand_desc
                summary_lines.append(f"• {p.username} ({cards_str}) — {p.best_hand_desc}")

        self.last_hand_summary = "\n".join(summary_lines)

    def reveal_winner_cards(self, user_id: int) -> tuple[str, bool]:
        """Allow the fold-win winner to flex and reveal their hole cards on the table image."""
        if self.state != TournamentState.ROUND_RESULT or self.won_by_fold_user_id != user_id:
            return "Only the hand winner can reveal cards right now.", False

        winner = self.players[user_id]
        if winner.revealed_cards:
            return "Your cards are already revealed!", False

        winner.revealed_cards = True
        hand_eval = evaluate_best_hand(winner.hole_cards + self.community_cards)
        winner.best_hand_desc = hand_eval.description
        cards_str = " ".join(c.emoji_str for c in winner.hole_cards)
        self.last_hand_summary += f"\n😎 *{winner.username}* flexed their cards: *{cards_str}* ({hand_eval.description})!"
        return f"Revealed {cards_str}!", True

    def get_player_cards_alert(self, user_id: int) -> str:
        """Build popup text (for show_alert=True) showing the player's hole cards and current hand."""
        player = self.players.get(user_id)
        if not player:
            return "You are not seated at this poker table."
        if not player.hole_cards:
            return "You have no cards in this hand."

        hole_str = "  ".join(c.emoji_str for c in player.hole_cards)
        all_cards = player.hole_cards + self.community_cards
        best = evaluate_best_hand(all_cards)
        board_str = " ".join(c.emoji_str for c in self.community_cards) if self.community_cards else "None (Pre-Flop)"
        return (
            f"🃏 Your Hole Cards:\n{hole_str}\n\n"
            f"🂠 Board: {board_str}\n"
            f"✨ Best Hand: {best.description}\n"
            f"💰 Stack: {player.tournament_credits} credits"
        )

    def format_table_caption(self) -> str:
        """Build the Markdown caption shown below the poker table photo."""
        sb, bb, ante = self.get_blinds_and_ante()
        ante_str = f" | Ante: *{ante}*" if ante > 0 else ""
        board_str = " ".join(f"*{c.emoji_str}*" for c in self.community_cards) if self.community_cards else "Waiting for Flop..."

        lines = [
            f"{self.format_header()} • *{self.street.value}*",
            f"💰 Pot: *{self.pot}* | Blinds: *{sb}/{bb}*{ante_str}",
            f"🂠 Board: {board_str}",
        ]

        if self.street_action_log:
            recent_actions = self.street_action_log[-POKER_ACTION_LOG_DISPLAY_COUNT:]
            lines.append("\n📜 *Recent Moves:*\n" + "\n".join(recent_actions))

        if self.is_all_in_runout:
            lines.append("\n🔥 *ALL-IN RUNOUT!* Hole cards revealed — dealing next cards...")
        elif self.state == TournamentState.BETTING and self.current_turn_user_id is not None:
            turn_player = self.players[self.current_turn_user_id]
            to_call = self.get_to_call(self.current_turn_user_id)
            call_info = f"*{to_call}* to call" if to_call > 0 else "Check or Bet"
            if turn_player.is_bot:
                lines.append(
                    f"\n🤖 *BOT TURN:* *{turn_player.username}* is thinking... ({call_info} | Stack: *{turn_player.tournament_credits}*)\n"
                    f"⚡ Use buttons below to queue a *Premove* for your turn!"
                )
            else:
                lines.append(
                    f"\n🎯 *YOUR TURN:* *{turn_player.username}* ({call_info} | Stack: *{turn_player.tournament_credits}*)\n"
                    f"⏱ {POKER_TURN_TIMEOUT_SECONDS}s timer — use buttons below or type a custom raise amount\n"
                    f"⚡ Not your turn? Tap Check / Call / Fold to queue a *Premove*!"
                )
        elif self.state == TournamentState.ROUND_RESULT:
            lines.append(f"\n{self.last_hand_summary}")
            lines.append(f"\n{self.get_standings()}")
            if not self.is_last_round():
                lines.append(f"\n⏳ Next hand starting in {POKER_NEXT_HAND_DELAY_SECONDS}s... (tap Next Hand to skip)")

        return "\n".join(lines)

    def build_lobby_keyboard(self) -> InlineKeyboardMarkup:
        """Build inline buttons for joining, adding AI bots, and starting the poker tournament."""
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        f"💺 Join Table ({self.buy_in})",
                        callback_data=f"{TOURNAMENT_LOBBY_CALLBACK_PREFIX}join",
                    ),
                    InlineKeyboardButton(
                        "🤖 Add Bot",
                        callback_data=f"{TOURNAMENT_LOBBY_CALLBACK_PREFIX}add_bot",
                    ),
                ],
                [
                    InlineKeyboardButton(
                        "▶️ Start Game",
                        callback_data=f"{TOURNAMENT_LOBBY_CALLBACK_PREFIX}start",
                    ),
                ],
            ]
        )

    def build_table_keyboard(self) -> InlineKeyboardMarkup:
        """Build the 1-click inline action grid for the poker table message."""
        rows: list[list[InlineKeyboardButton]] = [[InlineKeyboardButton("🃏 View My Cards", callback_data=f"{POKER_CALLBACK_PREFIX}cards")]]

        if self.state == TournamentState.BETTING and self.current_turn_user_id is not None:
            uid = self.current_turn_user_id
            to_call = self.get_to_call(uid)
            player = self.players[uid]

            if player.is_bot:
                if self.current_street_bet == 0:
                    premove_right_btn = InlineKeyboardButton("⚡ Premove: Check", callback_data=f"{POKER_CALLBACK_PREFIX}check")
                else:
                    premove_right_btn = InlineKeyboardButton(
                        f"⚡ Premove: Call ({self.current_street_bet})",
                        callback_data=f"{POKER_CALLBACK_PREFIX}call",
                    )
                rows.append(
                    [
                        InlineKeyboardButton("⚡ Premove: Check/Fold", callback_data=f"{POKER_CALLBACK_PREFIX}fold"),
                        premove_right_btn,
                    ]
                )
            else:
                if to_call == 0:
                    check_call_btn = InlineKeyboardButton("✅ Check", callback_data=f"{POKER_CALLBACK_PREFIX}check")
                elif to_call >= player.tournament_credits:
                    check_call_btn = InlineKeyboardButton(
                        f"🔥 Call All-In ({player.tournament_credits})",
                        callback_data=f"{POKER_CALLBACK_PREFIX}all_in",
                    )
                else:
                    check_call_btn = InlineKeyboardButton(f"📞 Call {to_call}", callback_data=f"{POKER_CALLBACK_PREFIX}call")

                rows.append(
                    [
                        InlineKeyboardButton("❌ Fold", callback_data=f"{POKER_CALLBACK_PREFIX}fold"),
                        check_call_btn,
                    ]
                )

                presets = self.get_raise_presets(uid)
                if presets:
                    raise_row: list[InlineKeyboardButton] = []
                    label_map = {
                        "min": "⬆️ Min",
                        "half_pot": "⬆️ ½ Pot",
                        "pot": "⬆️ Pot",
                    }
                    for key in ("min", "half_pot", "pot"):
                        if key in presets:
                            amt = presets[key]
                            raise_row.append(
                                InlineKeyboardButton(
                                    f"{label_map[key]} ({amt})",
                                    callback_data=f"{POKER_CALLBACK_PREFIX}raise:{amt}",
                                )
                            )
                    if "all_in" in presets:
                        amt = presets["all_in"]
                        raise_row.append(
                            InlineKeyboardButton(
                                f"🔥 All-In ({amt})",
                                callback_data=f"{POKER_CALLBACK_PREFIX}all_in",
                            )
                        )
                    if raise_row:
                        rows.append(raise_row)

            if any(p.is_afk and not p.is_bot for p in self.players.values()):
                rows.append([InlineKeyboardButton("👋 Sit Back In (Disable AFK)", callback_data=f"{POKER_CALLBACK_PREFIX}sit_in")])

        elif self.state == TournamentState.ROUND_RESULT:
            result_row: list[InlineKeyboardButton] = []
            if self.won_by_fold_user_id is not None and not self.players[self.won_by_fold_user_id].revealed_cards:
                result_row.append(InlineKeyboardButton("😎 Reveal Cards (Flex)", callback_data=f"{POKER_CALLBACK_PREFIX}flex"))
            if not self.is_last_round():
                result_row.append(InlineKeyboardButton("⏭️ Next Hand", callback_data=f"{POKER_CALLBACK_PREFIX}next_hand"))
            if result_row:
                rows.append(result_row)

        return InlineKeyboardMarkup(rows)

    def handle_game_message(self, user_id: int, text: str) -> str | None:
        """Allow the active turn player to type a custom integer raise amount in chat."""
        if self.state != TournamentState.BETTING or user_id != self.current_turn_user_id:
            return None
        cleaned = text.strip()
        if not cleaned.isdigit():
            return None
        amount = int(cleaned)
        res = self.process_action(user_id, PokerAction.RAISE, raise_to_amount=amount)
        return res.message

    def resolve_round(self) -> str:
        return self.last_hand_summary

    def is_last_round(self) -> bool:
        return self.round_number >= self.max_rounds or self.get_active_player_count() <= 1 or self.get_active_human_player_count() == 0

    def _get_multiplier(self, rank: int, total_players: int) -> float:
        if self.get_human_player_count() < 2:
            return 1.0
        if rank == 0:
            return 1.5
        if rank == 1 and total_players >= 3:
            return 1.2
        return 1.0

    def _get_penalized_players(self) -> tuple[set[int], set[int]]:
        return set(), set()

    def get_final_results(self) -> str:
        return self.finish()

    def get_tournament_stats(self) -> str:
        lines = ["*📊 Poker Table Statistics:*"]
        most_wins = max(self.players.values(), key=lambda p: p.hands_won, default=None)
        if most_wins and most_wins.hands_won > 0:
            lines.append(f"• Most hands won: *{most_wins.username}* ({most_wins.hands_won} hands)")
        biggest_pot_player = max(self.players.values(), key=lambda p: p.biggest_pot_won, default=None)
        if biggest_pot_player and biggest_pot_player.biggest_pot_won > 0:
            lines.append(f"• Biggest pot won: *{biggest_pot_player.username}* (*{biggest_pot_player.biggest_pot_won}* credits)")
        lines.append(f"• Total hands played: *{self.round_number}*")
        return "\n".join(lines)
