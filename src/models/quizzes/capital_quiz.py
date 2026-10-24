import logging

import pandas as pd

from src.models.quizzes.base_guess_quiz import BaseGuessQuiz
from src.models.quizzes.flag_quiz import FlagQuiz

log = logging.getLogger(__name__)


class CapitalQuiz(BaseGuessQuiz):
    QUIZ_TYPE = "capital"
    FILTER_LABEL = "Continent"
    FILTER_KEY = "continent"
    ITEM_KEY = "country"
    REWARD_LEVELS = {"easy": 2500, "medium": 6000, "hard": 20000, "crazy": 50000}

    def get_display_name(self, item: dict | pd.Series) -> str:
        capital = item.get("capital", "") if isinstance(item, dict) else item["capital"]
        return str(capital)

    def get_description(self, item: dict | pd.Series) -> str:
        return FlagQuiz().get_description(item)

    def get_tips(self, item: dict | pd.Series, continent_specified: bool = False) -> list[str]:
        if not isinstance(item, dict):
            item = item.to_dict()

        continent = str(item.get("continent", "N/A")).strip()
        capital = str(item.get("capital", "")).strip()

        first_letter = capital[0].upper() if len(capital) >= 1 else "?"
        second_letter = capital[1] if len(capital) >= 2 else ""

        next_letters = capital[2:4]

        tip_continent = f"Continent: {continent}"
        tip_first_letter = f"First letter: {first_letter}"
        tip_second_letter = f"Second letter: {second_letter}" if second_letter else "Second letter: (none)"
        tip_next_letters = f"Next two letters: {next_letters}" if next_letters else "Next two letters: (none)"

        letter_tips = [tip_first_letter, tip_second_letter, tip_next_letters]
        if continent_specified:
            return letter_tips
        return [tip_continent, *letter_tips]

    def get_valid_answers(self, item: dict | pd.Series) -> set[str]:
        if not isinstance(item, dict):
            item = item.to_dict()

        valid_answers = set()
        capital = str(item.get("capital", "")).lower().strip()
        if capital and capital != "nan":
            valid_answers.add(capital)
            for part in capital.split():
                if len(part) >= 3:
                    valid_answers.add(part)

        return valid_answers

    def get_prompt(self, item: dict | pd.Series) -> str:
        return f"Country: *{FlagQuiz().get_display_name(item)}*"

    def format_timeout_message(self, item: dict | pd.Series, **kwargs) -> str:
        display_name = FlagQuiz().get_display_name(item)
        target_name = self.get_display_name(item)
        desc = self.get_description(item)
        return f"Time's up! The capital of *{display_name}* was *{target_name}*.\n\n{desc}"

    def format_correct_message(self, item: dict | pd.Series, reward: int, total_credits: int | None = None, **kwargs) -> str:
        display_name = FlagQuiz().get_display_name(item)
        target_name = self.get_display_name(item)
        desc = self.get_description(item)
        credits_part = f"You receive *{reward}* credits!"
        if total_credits is not None:
            credits_part += f" [*{total_credits}* in total]"
        return f"Correct! The capital of *{display_name}* is *{target_name}*.\n{credits_part}\n\n{desc}"

    def format_wrong_message(self, item: dict | pd.Series, **kwargs) -> str:
        display_name = FlagQuiz().get_display_name(item)
        target_name = self.get_display_name(item)
        desc = self.get_description(item)
        return f"Wrong! The capital of *{display_name}* was *{target_name}*.\n\n{desc}"
