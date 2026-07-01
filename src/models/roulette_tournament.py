import random
from collections import defaultdict

from src.config.constants import ROULETTE_COLORS, ROULETTE_NUMBERS
from src.config.enums import RouletteBetType, TournamentState, TournamentType
from src.models.base_tournament import BaseTournament, RoundResult

VALID_BET_TYPES = {bt.value for bt in RouletteBetType if bt != RouletteBetType.NONE and bt != RouletteBetType.SINGLE_NUMBER}


class RouletteTournament(BaseTournament):
    """Multiplayer roulette tournament — players bet against the casino on shared spins."""

    def __init__(self, chat_id: int, thread_id: int, host_user_id: int, host_username: str, credits_obj, buy_in: int, max_rounds: int):
        super().__init__(chat_id, thread_id, host_user_id, host_username, credits_obj, buy_in, max_rounds)
        self.bets: dict[int, tuple[RouletteBetType, int]] = {}
        self.round_history: list[dict] = []

    @property
    def tournament_type(self) -> TournamentType:
        return TournamentType.ROULETTE

    def format_header(self) -> str:
        return "🎰 *Roulette Tournament*"

    def handle_game_message(self, user_id: int, text: str) -> str | None:
        if self.state != TournamentState.BETTING:
            return None

        if user_id not in self.players:
            return None

        player = self.players[user_id]
        if player.tournament_credits <= 0:
            return "You have no tournament credits left. You're out of bets for this tournament."

        if user_id in self.bets:
            return "You already placed a bet this round."

        parts = text.lower().strip().split()
        if len(parts) != 3 or parts[0] != "bet":
            return None

        bet_type_str = parts[1]
        if bet_type_str not in VALID_BET_TYPES:
            valid = ", ".join(sorted(VALID_BET_TYPES))
            return f"Invalid bet type. Valid types: {valid}"

        try:
            bet_size = int(parts[2])
        except ValueError:
            return "Bet amount must be a number."

        if bet_size <= 0:
            return "Bet amount must be positive."

        if bet_size > player.tournament_credits:
            return f"Not enough tournament credits. You have *{player.tournament_credits}*."

        bet_type = RouletteBetType(bet_type_str)
        self.bets[user_id] = (bet_type, bet_size)
        return f"*{player.username}* bet *{bet_size}* on *{bet_type_str}* ✅"

    def all_bets_placed(self) -> bool:
        return all(not (player.tournament_credits > 0 and player.user_id not in self.bets) for player in self.players.values())

    def has_active_bets(self) -> bool:
        return len(self.bets) > 0

    def format_bets_summary(self) -> str:
        if not self.bets:
            return "No bets were placed this round."

        lines = ["*Bets:*"]
        for user_id, (bet_type, bet_size) in self.bets.items():
            username = self.players[user_id].username
            lines.append(f"• {username}: *{bet_size}* on *{bet_type.value}*")

        skipped = [p.username for p in self.players.values() if p.tournament_credits > 0 and p.user_id not in self.bets]
        if skipped:
            lines.append(f"\n_Skipped: {', '.join(skipped)}_")

        return "\n".join(lines)

    def resolve_round(self) -> str:
        self.state = TournamentState.ROUND_RESULT

        n = random.choice(ROULETTE_NUMBERS)
        color = RouletteBetType(ROULETTE_COLORS[n])
        is_even = n % 2 == 0 and n != 0
        is_high = n > 18

        self.round_history.append({"number": n, "color": color.value, "is_even": is_even, "is_high": is_high})

        results = []
        for user_id, (bet_type, bet_size) in self.bets.items():
            player = self.players[user_id]
            won, payout_multiplier = self._evaluate_bet(n, color, is_even, is_high, bet_type)

            if won:
                credit_change = bet_size * payout_multiplier
                player.correct_bets += 1
            else:
                credit_change = -bet_size

            player.tournament_credits += credit_change
            if player.tournament_credits < 0:
                player.tournament_credits = 0
            player.total_bets += 1

            results.append(
                RoundResult(
                    user_id=user_id,
                    username=player.username,
                    bet_type=bet_type,
                    bet_size=bet_size,
                    won=won,
                    credit_change=credit_change,
                    tournament_credits_after=player.tournament_credits,
                )
            )

        zeroed = [r.user_id for r in results if r.tournament_credits_after == 0]
        result_message = self._format_round_results(n, color, results, zeroed)

        self.bets = {}
        return result_message

    @staticmethod
    def _evaluate_bet(n: int, color: RouletteBetType, is_even: bool, is_high: bool, bet_type: RouletteBetType) -> tuple[bool, int]:
        """Returns (won, payout_multiplier). Payout multiplier is 1 for normal wins, 35 for green."""
        match bet_type:
            case RouletteBetType.RED:
                return color == RouletteBetType.RED, 1
            case RouletteBetType.BLACK:
                return color == RouletteBetType.BLACK, 1
            case RouletteBetType.GREEN:
                return n == 0, 35
            case RouletteBetType.ODD:
                return (not is_even and n != 0), 1
            case RouletteBetType.EVEN:
                return (is_even and n != 0), 1
            case RouletteBetType.HIGH:
                return is_high, 1
            case RouletteBetType.LOW:
                return (not is_high and n != 0), 1
            case _:
                return False, 1

    def _format_round_results(self, n: int, color: RouletteBetType, results: list[RoundResult], zeroed: list[int]) -> str:
        lines = [f"The ball fell on *{n}* (*{color.value}*)\n"]

        winners = [r for r in results if r.won]
        losers = [r for r in results if not r.won]

        if winners:
            lines.append("*Winners:* 🔥")
            for r in winners:
                lines.append(f"• {r.username}: +*{r.credit_change}* → *{r.tournament_credits_after}* credits")

        if losers:
            lines.append("*Losers:* 💀")
            for r in losers:
                lines.append(f"• {r.username}: *{r.credit_change}* → *{r.tournament_credits_after}* credits")

        bet_user_ids = {r.user_id for r in results}
        didnt_bet = [player for player in self.players.values() if player.user_id not in bet_user_ids and player.tournament_credits > 0]
        if didnt_bet:
            lines.append("*Didn't bet:* 😴")
            for p in didnt_bet:
                lines.append(f"• {p.username}: *{p.tournament_credits}* credits")

        if zeroed:
            zeroed_names = [self.players[uid].username for uid in zeroed]
            lines.append(f"\n⚠️ *Eliminated:* {', '.join(zeroed_names)}")

        return "\n".join(lines)

    def get_final_results(self) -> tuple[str, list[int]]:
        return self.finish()

    def get_tournament_stats(self) -> str:
        if not self.round_history:
            return ""

        color_counts: dict[str, int] = defaultdict(int)
        even_count = 0
        odd_count = 0
        high_count = 0
        low_count = 0
        zero_count = 0

        for rnd in self.round_history:
            color_counts[rnd["color"]] += 1
            if rnd["number"] == 0:
                zero_count += 1
            else:
                if rnd["is_even"]:
                    even_count += 1
                else:
                    odd_count += 1
                if rnd["is_high"]:
                    high_count += 1
                else:
                    low_count += 1

        total = len(self.round_history)
        lines = [
            "*📊 Tournament Stats:*",
            f"Rounds played: {total}",
            f"🔴 Red: {color_counts.get('red', 0)} | ⚫ Black: {color_counts.get('black', 0)} | 🟢 Green: {zero_count}",
            f"Odd: {odd_count} | Even: {even_count}",
            f"High: {high_count} | Low: {low_count}",
        ]
        return "\n".join(lines)
