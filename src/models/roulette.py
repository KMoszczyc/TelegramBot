import random

import pandas as pd

from definitions import CreditActionType, RouletteBetType

pd.options.mode.chained_assignment = None
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', 1000)


class Roulette:
    def __init__(self, credits_obj):
        self.all_numbers = range(37)
        self.roulette_colors = ["green", "red", "black", "red", "black", "red", "black", "red", "black", "red", "black", "black", "red", "black", "red", "black", "red", "black", "red", "red", "black",
                                "red", "black", "red", "black", "red", "black", "red", "black", "black", "red", "black", "red", "black", "red", "black", "red"]
        self.credits = credits_obj

    def play(self, user_id, bet_size, bet_type_arg: str) -> tuple[str, str]:
        if user_id not in self.credits.credits or self.credits.credits[user_id] < bet_size:
            return "You don't have enough credits for that bet, fuck off.", False

        bet_type = self.parse_bet(bet_type_arg)
        if bet_type == RouletteBetType.NONE:
            return "Invalid bet type.", False

        message = ''
        success = False
        match bet_type:
            case RouletteBetType.RED | RouletteBetType.BLACK | RouletteBetType.GREEN:
                message, success = self.play_red_black(user_id, bet_size, bet_type)
            case RouletteBetType.ODD | RouletteBetType.EVEN:
                message, success = self.play_odd_even(user_id, bet_size, bet_type)
            case RouletteBetType.HIGH | RouletteBetType.LOW:
                message, success = self.play_high_low(user_id, bet_size, bet_type)
            case RouletteBetType.NONE | _:
                message, success = "Invalid bet type.", False
        return message, success

    def play_red_black(self, user_id, bet_size, bet_type) -> str:
        n = random.choice(self.all_numbers)
        result = RouletteBetType(self.roulette_colors[n])

        rule = bet_type == result
        special_rule = bet_type == result and result == RouletteBetType.GREEN
        return self.apply_bet(n, result, user_id, bet_size, rule, bet_type, special_rule, payout_multiplier=35)

    def play_odd_even(self, user_id, bet_size, bet_type) -> str:
        n = random.choice(self.all_numbers)
        is_even = n % 2 == 0
        result = RouletteBetType.EVEN if is_even else RouletteBetType.ODD
        winning_rule = bet_type == result and n != 0
        return self.apply_bet(n, result, user_id, bet_size, winning_rule, bet_type)

    def play_high_low(self, user_id, bet_size, bet_type) -> str:
        n = random.choice(self.all_numbers)
        result = RouletteBetType.HIGH if n > 18 else RouletteBetType.LOW
        if n == 0:
            result = RouletteBetType.NONE
        winning_rule = bet_type == result
        return self.apply_bet(n, result, user_id, bet_size, winning_rule, bet_type)

    def apply_bet(self, n, result, user_id, bet_size, winning_rule, bet_type, special_rule=False, payout_multiplier=1) -> str:
        if special_rule:
            credit_change = bet_size * payout_multiplier
            message = f"The ball fell on *{n}*, which is *{result.value}*. Congrats! You won *{bet_size * payout_multiplier} credits* 🔥"
        elif winning_rule:
            credit_change = bet_size
            message = f"The ball fell on *{n}*, which is *{result.value}*. Congrats! You won *{bet_size} credits* 🔥"
        else:
            credit_change = -bet_size
            message = f"The ball fell on *{n}*, which is *{result.value}*. You lose your *{bet_size} credits* 🖕"

        self.credits.credits[user_id] += credit_change
        self.credits.update_credit_history(user_id, credit_change, CreditActionType.BET, bet_type, winning_rule)
        success = credit_change > 0
        return message, success

    def parse_bet(self, bet_type_arg: str) -> RouletteBetType:
        exists = bet_type_arg in [bet_type.value for bet_type in list(RouletteBetType)]
        return RouletteBetType(bet_type_arg) if exists else RouletteBetType.NONE
