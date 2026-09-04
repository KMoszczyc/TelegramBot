import logging
import os

import pandas as pd

from src.config.paths import FLAGS_DIR_PATH
from src.models.quizzes.base_guess_quiz import BaseGuessQuiz

log = logging.getLogger(__name__)


class FlagQuiz(BaseGuessQuiz):
    REWARD_LEVELS = {
        "easy": 1000,
        "medium": 3000,
        "hard": 10000,
        "crazy": 25000,
    }

    def get_display_name(self, item: dict | pd.Series) -> str:
        name = item.get("country_name", "") if isinstance(item, dict) else item["country_name"]
        return str(name)

    def get_description(self, item: dict | pd.Series) -> str:
        if not isinstance(item, dict):
            item = item.to_dict()
        capital = item.get("capital", "N/A")
        continent = item.get("continent", "N/A")
        pop = item.get("population", 0)
        try:
            pop_str = f"{int(pop):,}"
        except (ValueError, TypeError):
            pop_str = str(pop)
        return f"Capital: {capital} | Continent: {continent} | Population: {pop_str}"

    def get_tips(self, item: dict | pd.Series, continent_specified: bool = False) -> list[str]:
        if not isinstance(item, dict):
            item = item.to_dict()

        name = str(item.get("country_name", "")).strip()
        continent = str(item.get("continent", "N/A")).strip()

        first_letter = name[0].upper() if len(name) >= 1 else "?"
        second_letter = name[1] if len(name) >= 2 else ""

        tip_first_letter = f"First letter: {first_letter}"
        tip_second_letter = f"Second letter: {second_letter}" if second_letter else "Second letter: (none)"

        if continent_specified:
            return [tip_first_letter, tip_second_letter]
        else:
            return [f"Continent: {continent}", tip_first_letter, tip_second_letter]

    def get_valid_answers(self, item: dict | pd.Series) -> set[str]:
        if not isinstance(item, dict):
            item = item.to_dict()

        valid_answers = set()
        name = str(item.get("country_name", "")).lower().strip()
        if name and name != "nan":
            valid_answers.add(name)
            for part in name.split():
                if len(part) >= 3:
                    valid_answers.add(part)

        code = str(item.get("country_code", "")).lower().strip()
        if code and code != "nan":
            valid_answers.add(code)

        return valid_answers

    @staticmethod
    def get_flag_filename_for_country(country: dict | pd.Series) -> str:
        code = country.get("country_code", "") if isinstance(country, dict) else country["country_code"]
        code_str = str(code).strip().upper()
        return f"{code_str}.jpg"

    def guess_random_flag(self, countries_df: pd.DataFrame) -> tuple[str, dict]:
        country = countries_df.sample(n=1).iloc[0].to_dict()
        image_path = self.get_image_path(country)
        return image_path, country

    def get_image_path(self, item: dict | pd.Series) -> str:
        """Return the absolute path to the flag image for *item*."""
        filename = self.get_flag_filename_for_country(item)
        return os.path.join(FLAGS_DIR_PATH, filename)
