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
        print(self.credits)
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
            case LuckyScoreType.NEUTRAL:
                new_credits = 10
            case LuckyScoreType.LUCKY:
                new_credits = 25
            case LuckyScoreType.VERY_LUCKY:
                new_credits = 50
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
            case RouletteBetType.RED | RouletteBetType.BLACK:
                message = self.play_red_black(user_id, bet_size, bet_type)
            case RouletteBetType.ODD | RouletteBetType.EVEN:
                message = self.play_odd_even(user_id, bet_size, bet_type)
            case RouletteBetType.NONE:
                message = "Invalid bet type."
        self.save_credits()
        return message

    def play_red_black(self, user_id, bet_size, bet_type) -> str:
        n = random.choice(self.all_numbers)
        result = self.roulette_colors[n]
        rule = (bet_type == RouletteBetType.RED and result == 'red') or (bet_type == RouletteBetType.BLACK and result == 'black')
        return self.apply_bet(n, result, user_id, bet_size, rule)

    def play_odd_even(self, user_id, bet_size, bet_type) -> str:
        n = random.choice(self.all_numbers)
        is_even = n % 2 == 0
        result = 'even' if is_even else 'odd'
        rule = (bet_type == RouletteBetType.ODD and not is_even) or (bet_type == RouletteBetType.EVEN and is_even)
        return self.apply_bet(n, result, user_id, bet_size, rule)

    def apply_bet(self, n, result, user_id, bet_size, rule) -> str:
        if rule:
            self.credits[user_id] += bet_size
            return f"The ball fell on *{n}*, which is *{result}*. Congrats! You won *{bet_size} credits* 🔥"
        else:
            self.credits[user_id] -= bet_size
            return f"The ball fell on *{n}*, which is *{result}*. You lose your *{bet_size} credits* 🖕"

    def parse_bet(self, bet_type_arg: str) -> RouletteBetType:
        print(list(RouletteBetType))
        exists = bet_type_arg in [bet_type.value for bet_type in list(RouletteBetType)]
        return RouletteBetType(bet_type_arg) if exists else RouletteBetType.NONE
