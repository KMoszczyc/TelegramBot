import os
import pickle
from collections import defaultdict
import random

import pandas as pd

from definitions import CREDITS_PATH, LuckyScoreType, RouletteBetType, CREDIT_HISTORY_PATH, CreditActionType
import src.core.utils as core_utils
import src.stats.utils as stats_utils
from src.models.command_args import CommandArgs

pd.options.mode.chained_assignment = None
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', 1000)

CREDIT_HISTORY_COLUMNS = ['timestamp', 'user_id', 'robbed_user_id', 'credit_change', 'action_type', 'bet_type', 'success']


class Roulette:
    def __init__(self):
        self.credits = self.load_credits()
        self.credit_history_df = self.load_credit_history()
        self.all_numbers = range(37)
        self.roulette_colors = ["green", "red", "black", "red", "black", "red", "black", "red", "black", "red", "black", "black", "red", "black", "red", "black", "red", "black", "red", "red", "black",
                                "red", "black", "red", "black", "red", "black", "red", "black", "black", "red", "black", "red", "black", "red", "black", "red"]

    def save_credits(self):
        with open(CREDITS_PATH, 'wb') as f:
            pickle.dump(self.credits, f)

        core_utils.save_df(self.credit_history_df, CREDIT_HISTORY_PATH)

    def load_credits(self):
        if not os.path.exists(CREDITS_PATH):
            return defaultdict(int)

        with open(CREDITS_PATH, 'rb') as f:
            try:
                return pickle.load(f)
            except EOFError:
                return defaultdict(int)

    def load_credit_history(self):
        if not os.path.exists(CREDIT_HISTORY_PATH):
            credits = self.load_credits()
            data = [[stats_utils.get_dt_now(), user_id, None, credit, CreditActionType.GET.value, None, True] for user_id, credit in credits.items()]
            df = pd.DataFrame(columns=CREDIT_HISTORY_COLUMNS, data=data)
            df.to_parquet(CREDIT_HISTORY_PATH)

            print(df.head(15))

        return pd.read_parquet(CREDIT_HISTORY_PATH)

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
        self.update_credit_history(user_id, new_credits, CreditActionType.GET, None, True, None)
        message = f"Your luck today is: *{lucky_score_type.value}*, StaraBaba gives you *{new_credits} credits* today :). Now in total you have *{self.credits[user_id]} credits*."
        return stats_utils.escape_special_characters(message)

    def show_credit_leaderboard(self, users_map) -> str:
        sorted_credits = sorted(self.credits.items(), key=lambda kv: kv[1], reverse=True)
        text = f'``` Credit score leaderboard: \n'
        if sorted_credits:
            max_len_username = max(len(users_map[user_id]) for user_id, _ in sorted_credits)
            for i, (user_id, credit) in enumerate(sorted_credits):
                username = users_map[user_id]
                text += f"\n{i + 1}.".ljust(4) + f" {username}:".ljust(max_len_username + 5) + f"{credit}"
        text += "```"
        return stats_utils.escape_special_characters(text)

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
            case RouletteBetType.NONE:
                message = "Invalid bet type."
        return message

    def play_red_black(self, user_id, bet_size, bet_type) -> str:
        n = random.choice(self.all_numbers)
        result = RouletteBetType(self.roulette_colors[n])

        rule = bet_type == result
        special_rule = bet_type == result and RouletteBetType.GREEN == result
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

        self.credits[user_id] += credit_change
        self.update_credit_history(user_id, credit_change, CreditActionType.BET, bet_type, winning_rule)

        return message

    def update_credit_history(self, user_id: int, credit_change: int, action_type: CreditActionType | None, bet_type: RouletteBetType | None, success: bool, robbed_user_id=None):
        bet_type = bet_type.value if bet_type is not None else None
        data = [stats_utils.get_dt_now(), user_id, robbed_user_id, credit_change, action_type.value, bet_type, success]
        new_entry = pd.DataFrame(columns=CREDIT_HISTORY_COLUMNS, data=[data])
        self.credit_history_df = pd.concat([self.credit_history_df, new_entry], ignore_index=True)
        self.save_credits()

    def parse_bet(self, bet_type_arg: str) -> RouletteBetType:
        exists = bet_type_arg in [bet_type.value for bet_type in list(RouletteBetType)]
        return RouletteBetType(bet_type_arg) if exists else RouletteBetType.NONE

    def preprocess_credit_history(self, command_args):
        filtered_credit_history_df = self.credit_history_df.copy(deep=True)
        filtered_credit_history_df = stats_utils.filter_by_time_df(filtered_credit_history_df, command_args, 'timestamp')
        if command_args.user_id is not None:
            filtered_credit_history_df = filtered_credit_history_df[filtered_credit_history_df['user_id'] == command_args.user_id]

        return filtered_credit_history_df

    def show_top_bet_leaderboard(self, users_map, command_args: CommandArgs):
        filtered_credit_history_df = self.preprocess_credit_history(command_args)
        filtered_credit_history_df = filtered_credit_history_df[filtered_credit_history_df['action_type'] == CreditActionType.BET.value]
        filtered_credit_history_df['credit_change'] = filtered_credit_history_df['credit_change'].abs()
        filtered_credit_history_df = filtered_credit_history_df.sort_values(by='credit_change', ascending=False)
        text = f'``` TOP bet leaderboard: \n'
        max_len_username = core_utils.max_str_length_in_list(users_map.values())
        for i, (index, row) in enumerate(filtered_credit_history_df.head(10).iterrows()):
            username = users_map[row['user_id']]
            text += f"\n{i + 1}.".ljust(4) + f" {username}:".ljust(max_len_username + 5) + f"{row['credit_change']}"
        text += "```"
        return stats_utils.escape_special_characters(text)

    def steal_credits(self, user_id, robbed_user_id, amount, users_map) -> str:
        robbed_username = users_map[robbed_user_id]
        if amount > self.credits[robbed_user_id]:
            return f"*{robbed_username}* doesn't have that much credits. Steal less!"
        p = self.calculate_steal_chance(robbed_user_id, amount)
        if random.random() < p:
            self.credits[user_id] += amount
            self.credits[robbed_user_id] -= amount
            self.update_credit_history(user_id, amount, CreditActionType.STEAL, None, True, robbed_user_id)
            return f"You've successfully stolen *{amount}* credits from *{robbed_username}*!!"
        else:
            self.update_credit_history(user_id, amount, CreditActionType.STEAL, None, False, robbed_user_id)
            return f"You've failed to steal *{amount}* credits from *{robbed_username}*."
        
    def calculate_steal_chance(self, robbed_user_id, amount):
        target_credits = self.credits[robbed_user_id]
        return core_utils.calculate_skewed_probability(amount, target_credits)
