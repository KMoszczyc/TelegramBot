import logging

import pandas as pd

from src.models.quizzes.base_guess_quiz import BaseGuessQuiz
from src.models.quizzes.flag_quiz import FlagQuiz

log = logging.getLogger(__name__)


class CapitalQuiz(BaseGuessQuiz):
    REWARD_LEVELS = {"easy": 2500, "medium": 6000, "hard": 20000, "crazy": 50000}

    def get_display_name(self, item: dict | pd.Series) -> str:
        capital = item.get("capital", "") if isinstance(item, dict) else item["capital"]
        return str(capital)

    def get_description(self, item: dict | pd.Series) -> str:
        return FlagQuiz().get_description(item)

    def get_tips(self, item: dict | pd.Series, continent_specified: bool = False) -> list[str]:
        if not isinstance(item, dict):
            item = item.to_dict()

        country_name = str(item.get("country_name", "")).strip()
        capital = str(item.get("capital", "")).strip()

        first_letter = capital[0].upper() if len(capital) >= 1 else "?"
        second_letter = capital[1] if len(capital) >= 2 else ""

        tip_country = f"Country: {country_name}"
        tip_first_letter = f"First letter: {first_letter}"
        tip_second_letter = f"Second letter: {second_letter}" if second_letter else "Second letter: (none)"

        return [tip_country, tip_first_letter, tip_second_letter]

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

    def get_image_path(self, item: dict | pd.Series) -> str:
        return FlagQuiz().get_image_path(item)
