from abc import ABC, abstractmethod
from dataclasses import dataclass
from math import floor
from typing import Any

from src.config.constants import MIN_TOURNAMENT_PLAYERS
from src.config.enums import CreditActionType, TournamentState, TournamentType


@dataclass
class TournamentPlayer:
    user_id: int
    username: str
    is_host: bool
    tournament_credits: int
    correct_bets: int = 0
    total_bets: int = 0


@dataclass
class RoundResult:
    user_id: int
    username: str
    bet_type: Any
    bet_size: int
    won: bool
    credit_change: int
    tournament_credits_after: int


class BaseTournament(ABC):
    """Abstract base for multiplayer tournament games."""

    def __init__(self, chat_id: int, thread_id: int, host_user_id: int, host_username: str, credits_obj, buy_in: int, max_rounds: int):
        self.chat_id = chat_id
        self.thread_id = thread_id
        self.credits = credits_obj
        self.buy_in = buy_in
        self.max_rounds = max_rounds
        self.round_number = 0
        self.state = TournamentState.JOINING
        self.players: dict[int, TournamentPlayer] = {}
        self._add_host(host_user_id, host_username)

    @property
    def is_active(self) -> bool:
        return self.state != TournamentState.FINISHED

    @property
    @abstractmethod
    def tournament_type(self) -> TournamentType:
        ...

    @abstractmethod
    def handle_game_message(self, user_id: int, text: str) -> str | None:
        ...

    @abstractmethod
    def resolve_round(self) -> str:
        ...

    @abstractmethod
    def format_header(self) -> str:
        ...

    @abstractmethod
    def get_final_results(self) -> tuple[str, list[int]]:
        """Returns (final_results_message, list_of_zeroed_user_ids)."""
        ...

    @abstractmethod
    def get_tournament_stats(self) -> str:
        ...

    def _add_host(self, user_id: int, username: str):
        self._deduct_buy_in(user_id)
        self.players[user_id] = TournamentPlayer(user_id=user_id, username=username, is_host=True, tournament_credits=self.buy_in)

    def add_player(self, user_id: int, username: str) -> tuple[str, bool]:
        if self.state != TournamentState.JOINING:
            return "Tournament is not accepting players right now.", False

        if user_id in self.players:
            return "You already joined this tournament.", False

        if user_id not in self.credits.credits or self.credits.credits[user_id] < self.buy_in:
            current = self.credits.credits.get(user_id, 0)
            return f"Not enough credits. You have *{current}* but the buy-in is *{self.buy_in}*.", False

        self._deduct_buy_in(user_id)
        self.players[user_id] = TournamentPlayer(user_id=user_id, username=username, is_host=False, tournament_credits=self.buy_in)
        return f"*{username}* joined the tournament! [{len(self.players)} players]", True

    def _deduct_buy_in(self, user_id: int):
        self.credits.credits[user_id] -= self.buy_in
        self.credits.update_credit_history(user_id, -self.buy_in, CreditActionType.TOURNAMENT)

    def has_enough_players(self) -> bool:
        return len(self.players) >= MIN_TOURNAMENT_PLAYERS

    def cancel_and_refund(self) -> str:
        for player in self.players.values():
            self.credits.credits[player.user_id] += self.buy_in
            self.credits.update_credit_history(player.user_id, self.buy_in, CreditActionType.TOURNAMENT)
        self.state = TournamentState.FINISHED
        return "Not enough players joined. Tournament cancelled, buy-ins refunded."

    def start_betting_round(self) -> str:
        self.round_number += 1
        self.state = TournamentState.BETTING
        return self._format_round_start()

    def _format_round_start(self) -> str:
        lines = [f"*Round {self.round_number}/{self.max_rounds}*\n"]
        lines.append(self.get_standings())
        active = self.get_active_player_count()
        lines.append(f"\n_{active} player(s) can bet. Place your bets!_")
        return "\n".join(lines)

    def get_standings(self) -> str:
        sorted_players = sorted(self.players.values(), key=lambda p: p.tournament_credits, reverse=True)
        lines = ["*Standings:*"]
        for i, p in enumerate(sorted_players, 1):
            status = "" if p.tournament_credits > 0 else " 💀"
            lines.append(f"{i}. {p.username}: *{p.tournament_credits}* credits{status}")
        return "\n".join(lines)

    def get_active_player_count(self) -> int:
        return sum(1 for p in self.players.values() if p.tournament_credits > 0)

    def is_last_round(self) -> bool:
        return self.round_number >= self.max_rounds

    def get_ranking(self) -> list[TournamentPlayer]:
        return sorted(self.players.values(), key=lambda p: p.tournament_credits, reverse=True)

    def apply_final_multipliers(self) -> dict[int, int]:
        """Returns {user_id: real_credit_payout} after applying placement multipliers.
        Tied players share the highest multiplier among their tied positions.
        """
        ranking = self.get_ranking()
        n = len(ranking)
        payouts = {}

        place = 0
        for i, player in enumerate(ranking):
            if i == 0 or ranking[i - 1].tournament_credits != player.tournament_credits:
                place = i

            multiplier = self._get_multiplier(place, n)
            payout = floor(player.tournament_credits * multiplier)
            payouts[player.user_id] = payout

        return payouts

    @staticmethod
    def _get_multiplier(rank: int, total_players: int) -> float:
        if total_players == 2:
            return 2.0 if rank == 0 else 0.5
        if rank == 0:
            return 3.0
        elif rank == 1:
            return 1.5
        elif rank == total_players - 1:
            return 0.5
        return 1.0

    def distribute_payouts(self, payouts: dict[int, int]):
        for user_id, payout in payouts.items():
            self.credits.credits[user_id] += payout
            self.credits.update_credit_history(user_id, payout, CreditActionType.TOURNAMENT)

    def finish(self) -> tuple[str, list[int]]:
        self.state = TournamentState.FINISHED
        payouts = self.apply_final_multipliers()
        self.distribute_payouts(payouts)

        ranking = self.get_ranking()
        n = len(ranking)
        zeroed_user_ids = [p.user_id for p in ranking if p.tournament_credits == 0]

        lines = [f"{self.format_header()}\n", "*🏆 Final Results:*\n"]

        place = 0
        for i, player in enumerate(ranking):
            if i == 0 or ranking[i - 1].tournament_credits != player.tournament_credits:
                place = i

            multiplier = self._get_multiplier(place, n)
            payout = payouts[player.user_id]
            accuracy = f"{player.correct_bets}/{player.total_bets}" if player.total_bets > 0 else "0/0"
            medal = self._get_medal(place + 1)
            ban_note = " ⛔ BANNED" if player.tournament_credits == 0 else ""

            lines.append(
                f"{medal} *{place + 1}.* {player.username} — *{player.tournament_credits}* credits "
                f"(x{multiplier} → *{payout}*) | Accuracy: {accuracy}{ban_note}"
            )

        lines.append(f"\n{self.get_tournament_stats()}")

        return "\n".join(lines), zeroed_user_ids

    @staticmethod
    def _get_medal(place: int) -> str:
        return {1: "🥇", 2: "🥈", 3: "🥉"}.get(place, "▪️")
