import os
import pickle
from collections import defaultdict
import random

from definitions import CREDITS_PATH, LuckyScoreType, RouletteBetType
import src.core.utils as core_utils
import src.stats.utils as stats_utils


class Roulette:
    def __init__(self, users_df):
        self.credits = self.load_credits()
        self.all_numbers = range(37)
        self.roulette_colors = ["green", "red", "black", "red", "black", "red", "black", "red", "black", "red", "black", "black", "red", "black", "red", "black", "red", "black", "red", "red", "black",
                                "red", "black", "red", "black", "red", "black", "red", "black", "black", "red", "black", "red", "black", "red", "black", "red"]

    def save_credits(self):
        with open(CREDITS_PATH, 'wb') as f:
            pickle.dump(self.credits, f)

    def load_credits(self):
        if not os.path.exists(CREDITS_PATH):
            return defaultdict(int)

        with open(CREDITS_PATH, 'rb') as f:
            try:
                return pickle.load(f)
            except EOFError:
                return defaultdict(int)

    def update_credits(self, user_id):
        lucky_score_type, _ = core_utils.are_you_lucky(user_id, with_args=False)
        match lucky_score_type:
            case LuckyScoreType.VERY_UNLUCKY:
                new_credits = 5
            case LuckyScoreType.UNLUCKY:
                new_credits = 15
            case LuckyScoreType.NEUTRAL:
                new_credits = 25
            case LuckyScoreType.LUCKY:
                new_credits = 50
            case LuckyScoreType.VERY_LUCKY:
                new_credits = 100
            case _:
                new_credits = 0
        self.credits[user_id] += new_credits
        message = f"Your luck today is: *{lucky_score_type.value}*, StaraBaba gives you *{new_credits} credits* today :). Now in total you have *{self.credits[user_id]} credits*."
        message = stats_utils.escape_special_characters(message)
        self.save_credits()
        return message

    def show_credit_leaderboard(self, users_map) -> str:
        sorted_credits = sorted(self.credits.items(), key=lambda kv: kv[1], reverse=True)
        text = f'``` Credit score leaderboard: \n'
        if sorted_credits:
            max_len_username = max(len(users_map[user_id]) for user_id, _ in sorted_credits)
            for i, (user_id, credit) in enumerate(sorted_credits):
                username = users_map[user_id]
                text += f"\n{i + 1}.".ljust(4) + f" {username}:".ljust(max_len_username + 5) + f"{credit}"
        text += "```"
        text = stats_utils.escape_special_characters(text)
        return text

    def play(self, user_id, bet_size, bet_type_arg: str) -> str:
        if user_id not in self.credits or self.credits[user_id] < bet_size:
            return "You don't have enough credits for that bet, fuck off."

        bet_type = self.parse_bet(bet_type_arg)
        if bet_type == RouletteBetType.NONE:
            return "Invalid bet type."

        message = ''
        match bet_type:
            case RouletteBetType.RED | RouletteBetType.BLACK | RouletteBetType.GREEN:
                message = self.play_red_black(user_id, bet_size, bet_type)
            case RouletteBetType.ODD | RouletteBetType.EVEN:
                message = self.play_odd_even(user_id, bet_size, bet_type)
            case RouletteBetType.HIGH | RouletteBetType.LOW:
                message = self.play_high_low(user_id, bet_size, bet_type)
            case RouletteNumbers():
                message = self.play_number(user_id, bet_size, bet_type)
            case RouletteBetType.NONE:
                message = "Invalid bet type."
        self.save_credits()
        return message

    def play_number(self, user_id, bet_size, number_enum) -> str:
        n = random.choice(self.all_numbers)
        result = RouletteBetType(n)
        rule = number_enum == int(RouletteNumbers(n))
        return self.apply_bet(n, result, user_id, bet_size, rule, payout_multiplier=35)

    def play_red_black(self, user_id, bet_size, bet_type) -> str:
        n = random.choice(self.all_numbers)
        result = RouletteBetType(self.roulette_colors[n])

        rule = bet_type == result
        if RouletteBetType.GREEN == result:
            return self.apply_bet(n, result, user_id, bet_size, rule, payout_multiplier=35)
        return self.apply_bet(n, result, user_id, bet_size, rule)

    def play_odd_even(self, user_id, bet_size, bet_type) -> str:
        n = random.choice(self.all_numbers)
        is_even = n % 2 == 0
        result = RouletteBetType.EVEN if is_even else RouletteBetType.ODD
        winning_rule = bet_type == result and n != 0
        return self.apply_bet(n, result, user_id, bet_size, winning_rule)

    def play_high_low(self, user_id, bet_size, bet_type) -> str:
        n = random.choice(self.all_numbers)
        result = RouletteBetType.HIGH if n > 18 else RouletteBetType.LOW
        if n == 0:
            result = RouletteBetType.NONE
        winning_rule = bet_type == result
        return self.apply_bet(n, result, user_id, bet_size, winning_rule)

    def apply_bet(self, n, result, user_id, bet_size, winning_rule, payout_multiplier=1) -> str:
        if winning_rule:
            self.credits[user_id] += bet_size * payout_multiplier
            return f"The ball fell on *{n}*, which is *{result.value}*. Congrats! You won *{bet_size * payout_multiplier} credits* 🔥"
        else:
            self.credits[user_id] -= bet_size
            return f"The ball fell on *{n}*, which is *{result.value}*. You lose your *{bet_size} credits* 🖕"

    def parse_bet(self, bet_type_arg: str) -> RouletteBetType:
        exists = bet_type_arg in [bet_type.value for bet_type in list(RouletteBetType)]
        return RouletteBetType(bet_type_arg) if exists else RouletteBetType.NONE
