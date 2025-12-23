import os
import pickle
import random
from collections import defaultdict
import pandas as pd

from definitions import LuckyScoreType, CreditActionType, CREDITS_PATH, CREDIT_HISTORY_PATH, RouletteBetType, CREDIT_HISTORY_COLUMNS
import src.core.utils as core_utils
import src.stats.utils as stats_utils
from src.models.command_args import CommandArgs


class Credits:
    def __init__(self):
        self.credits = self.load_credits()
        self.credit_history_df = self.load_credit_history()

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

    def get_daily_credits(self, user_id):
        lucky_score_type, _ = core_utils.are_you_lucky(user_id, with_args=False)
        match lucky_score_type:
            case LuckyScoreType.VERY_UNLUCKY:
                new_credits = 15
            case LuckyScoreType.UNLUCKY:
                new_credits = 25
            case LuckyScoreType.NEUTRAL:
                new_credits = 50
            case LuckyScoreType.LUCKY:
                new_credits = 100
            case LuckyScoreType.VERY_LUCKY:
                new_credits = 200
            case _:
                new_credits = 0
        self.credits[user_id] += new_credits
        self.update_credit_history(user_id, new_credits, CreditActionType.GET, None, True, None)
        message = f"Your luck today is: *{lucky_score_type.value}*, StaraBaba gives you *{new_credits} credits* today :). Now in total you have *{self.credits[user_id]} credits*."

        return stats_utils.escape_special_characters(message)

    def update_credits(self, user_id, credit_change, action_type):
        if self.credits[user_id] + credit_change < 0:
            return 0, False
        self.credits[user_id] += credit_change
        self.update_credit_history(user_id, credit_change, action_type, None, True, None)

        return self.credits[user_id], True

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

    def update_credit_history(self, user_id: int, credit_change: int, action_type: CreditActionType | None, bet_type: RouletteBetType | None, success: bool, target_user_id=None):
        bet_type = bet_type.value if bet_type is not None else None
        data = [stats_utils.get_dt_now(), user_id, target_user_id, credit_change, action_type.value, bet_type, success]
        new_entry = pd.DataFrame(columns=CREDIT_HISTORY_COLUMNS, data=[data])
        self.credit_history_df = pd.concat([self.credit_history_df, new_entry], ignore_index=True)
        self.save_credits()

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

    def show_steal_leaderboard(self, users_map, command_args: CommandArgs):
        filtered_credit_history_df = self.preprocess_credit_history(command_args)
        steal_history_df = filtered_credit_history_df[filtered_credit_history_df['action_type'] == CreditActionType.STEAL.value]
        steal_history_df = steal_history_df[filtered_credit_history_df['success'] == True]
        steal_history_df['steal_amount'] = steal_history_df['credit_change'].abs()
        steal_history_grouped_df = steal_history_df.groupby('user_id').agg({'steal_amount': 'sum'}).reset_index()
        steal_history_grouped_df = steal_history_grouped_df.sort_values(by='steal_amount', ascending=False)
        text = f'``` TOTAL steal leaderboard: \n'
        max_len_username = core_utils.max_str_length_in_list(users_map.values())
        for i, (index, row) in enumerate(steal_history_grouped_df.head(10).iterrows()):
            username = users_map[row['user_id']]
            text += f"\n{i + 1}.".ljust(4) + f" {username}:".ljust(max_len_username + 5) + f"{row['steal_amount']}"
        text += "```"
        return stats_utils.escape_special_characters(text)

    def gift_credits(self, source_user_id, target_user_id, amount, users_map):
        if self.credits[source_user_id] - amount < 0:
            return "You don't have enough credits to gift this amount."

        self.credits[source_user_id] -= amount
        self.credits[target_user_id] += amount
        self.update_credit_history(source_user_id, amount, CreditActionType.GIFT, None, True, target_user_id)

        gifter_username = users_map[source_user_id]
        target_username = users_map[target_user_id]
        if amount > 1000:
            return f"{gifter_username} gifted *{amount} credits* to *{target_username}*! A HUUUGE gift!"
        elif amount > 300:
            return f"{gifter_username} gifted *{amount} credits* to *{target_username}*! A really nice gift."
        elif amount > 100:
            return f"{gifter_username} gifted *{amount} credits* to *{target_username}*! A decent gift, could be better though."
        elif amount > 20:
            return f"{gifter_username} gifted only *{amount} credits* to *{target_username}*. This much credits lie on the street in Berlin and nobody picks it up."
        else:
            return f"A pathetic amount. You call that a gift? You should be ashamed of yourself."

    def steal_credits(self, user_id, target_user_id, amount, users_map) -> str:
        robbed_username = users_map[target_user_id]

        p = self.calculate_steal_chance(target_user_id, amount)
        if random.random() < p:
            self.credits[user_id] += amount
            self.credits[target_user_id] -= amount
            self.update_credit_history(user_id, amount, CreditActionType.STEAL, None, True, target_user_id)
            return f"You've *successfully* stolen {amount} credits from *{robbed_username}*!!"
        else:
            self.update_credit_history(user_id, amount, CreditActionType.STEAL, None, False, target_user_id)
            return f"You've *failed* to steal {amount} credits from *{robbed_username}*."

    def validate_steal(self, target_user_id, amount, users_map):
        robbed_username = users_map[target_user_id]
        if self.credits[target_user_id] == 0:
            return False, f"*{robbed_username}* doesn't have any credits left."

        if amount > self.credits[target_user_id]:
            return False, f"*{robbed_username}* doesn't have that much credits. Steal less!"

        return True, ""

    def calculate_steal_chance(self, target_user_id, amount):
        target_credits = self.credits[target_user_id]
        return core_utils.calculate_skewed_probability(amount, target_credits)
