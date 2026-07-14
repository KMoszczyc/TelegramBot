import logging
import os

import pandas as pd

from src.config.paths import FLAGS_DIR_PATH
from src.models.map_quiz import MapQuiz

log = logging.getLogger(__name__)

REWARD_LEVELS = {
    "easy": 1000,
    "medium": 3000,
    "hard": 10000,
    "crazy": 25000,
}


class FlagQuiz:
    REWARD_LEVELS = REWARD_LEVELS

    @staticmethod
    def get_reward(difficulty: str, continent_specified: bool, tips_given: int = 0) -> tuple[int, int]:
        base_reward = FlagQuiz.REWARD_LEVELS.get(difficulty, FlagQuiz.REWARD_LEVELS["crazy"])
        if continent_specified:
            base_reward = base_reward // 2

        if tips_given == 0:
            return base_reward, 0

        current_reward = int(base_reward * 0.5 * (0.75 ** (tips_given - 1)))
        decrease_pct = int(round((base_reward - current_reward) / base_reward * 100)) if base_reward > 0 else 0
        return current_reward, decrease_pct

    @staticmethod
    def get_country_display_name(country: dict | pd.Series) -> str:
        name = country.get("country_name", "") if isinstance(country, dict) else country["country_name"]
        return str(name)

    @staticmethod
    def get_country_description(country: dict | pd.Series) -> str:
        if not isinstance(country, dict):
            country = country.to_dict()
        capital = country.get("capital", "N/A")
        continent = country.get("continent", "N/A")
        pop = country.get("population", 0)
        try:
            pop_str = f"{int(pop):,}"
        except (ValueError, TypeError):
            pop_str = str(pop)
        return f"Capital: {capital} | Continent: {continent} | Population: {pop_str}"

    @staticmethod
    def get_tips(country: dict | pd.Series, continent_specified: bool = False) -> list[str]:
        if not isinstance(country, dict):
            country = country.to_dict()

        name = str(country.get("country_name", "")).strip()
        continent = str(country.get("continent", "N/A")).strip()

        first_letter = name[0].upper() if len(name) >= 1 else "?"
        second_letter = name[1] if len(name) >= 2 else ""

        tip_first_letter = f"First letter: {first_letter}"
        tip_second_letter = f"Second letter: {second_letter}" if second_letter else "Second letter: (none)"

        if continent_specified:
            return [tip_first_letter, tip_second_letter]
        else:
            return [f"Continent: {continent}", tip_first_letter, tip_second_letter]

    @staticmethod
    def get_valid_answers(country: dict | pd.Series) -> set[str]:
        if not isinstance(country, dict):
            country = country.to_dict()

        valid_answers = set()
        name = str(country.get("country_name", "")).lower().strip()
        if name and name != "nan":
            valid_answers.add(name)
            for part in name.split():
                if len(part) >= 3:
                    valid_answers.add(part)

        code = str(country.get("country_code", "")).lower().strip()
        if code and code != "nan":
            valid_answers.add(code)

        return valid_answers

    @staticmethod
    def is_answer_correct(user_answer: str, valid_answers: set[str]) -> bool:
        return MapQuiz.is_answer_correct(user_answer, valid_answers)

    @staticmethod
    def get_flag_filename_for_country(country: dict | pd.Series) -> str:
        code = country.get("country_code", "") if isinstance(country, dict) else country["country_code"]
        code_str = str(code).strip().upper()
        return f"{code_str}.jpg"

    def guess_random_flag(self, countries_df: pd.DataFrame) -> tuple[str, dict]:
        country = countries_df.sample(n=1).iloc[0].to_dict()
        filename = FlagQuiz.get_flag_filename_for_country(country)
        image_path = os.path.join(FLAGS_DIR_PATH, filename)
        return image_path, country
