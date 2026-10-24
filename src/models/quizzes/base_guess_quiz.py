import difflib
import re
from abc import ABC, abstractmethod

import pandas as pd


class BaseGuessQuiz(ABC):
    REWARD_LEVELS: dict[str, int]
    QUIZ_TYPE: str = ""
    FILTER_LABEL: str = "Continent"
    FILTER_KEY: str = "continent"
    ITEM_KEY: str = "country"

    @classmethod
    def get_reward(cls, difficulty: str, continent_specified: bool = False, tips_given: int = 0) -> tuple[int, int]:
        base_reward = cls.REWARD_LEVELS.get(difficulty, cls.REWARD_LEVELS.get("crazy", 0))
        if continent_specified:
            base_reward = base_reward // 2

        if tips_given == 0:
            return base_reward, 0

        current_reward = int(base_reward * 0.5 * (0.75 ** (tips_given - 1)))
        decrease_pct = int(round((base_reward - current_reward) / base_reward * 100)) if base_reward > 0 else 0
        return current_reward, decrease_pct

    @staticmethod
    def is_answer_correct(user_answer: str, valid_answers: set[str]) -> bool:
        if user_answer in valid_answers:
            return True

        user_words = user_answer.split()
        if not user_words:
            return False

        valid_parts = {w for w in valid_answers if len(w.split()) == 1}
        matched_words = 0
        for word in user_words:
            matches = difflib.get_close_matches(word, valid_parts, n=1, cutoff=0.75)
            if word in valid_answers or (
                len(word) >= 3
                and not re.match(r"^m{0,4}(cm|cd|d?c{0,3})(xc|xl|l?x{0,3})(ix|iv|v?i{0,3})$", word)
                and matches
                and abs(len(word) - len(matches[0])) <= (1 if len(word) <= 6 else 2)
            ):
                matched_words += 1

        return matched_words == len(user_words)

    @abstractmethod
    def get_display_name(self, item: dict | pd.Series) -> str:
        pass

    @abstractmethod
    def get_description(self, item: dict | pd.Series) -> str:
        pass

    @abstractmethod
    def get_tips(self, item: dict | pd.Series, continent_specified: bool = False) -> list[str]:
        pass

    @abstractmethod
    def get_valid_answers(self, item: dict | pd.Series) -> set[str]:
        pass

    def get_image_path(self, item: dict | pd.Series) -> str | None:
        """Return the image path for *item*, or None when the quiz is text-only."""
        return None

    def get_prompt(self, item: dict | pd.Series) -> str:
        """Return the text shown to the user for text-only quizzes."""
        return ""

    def build_quiz_prompt_text(
        self,
        item: dict | pd.Series,
        difficulty: str,
        timeout_seconds: int,
        filter_specified: bool = False,
        title: str | None = None,
    ) -> str:
        """Format the initial prompt/caption displayed when the quiz starts."""
        reward, _ = self.get_reward(difficulty, filter_specified, 0)
        base_text = f"Difficulty: {difficulty.replace('_', ' ').title()}\nTime to answer: {timeout_seconds}s\nReward: {reward} credits"
        if title:
            base_text = f"{title}\n{base_text}"

        if filter_specified:
            filter_val = item.get(self.FILTER_KEY) if isinstance(item, dict) else item[self.FILTER_KEY]
            base_text += f"\n{self.FILTER_LABEL}: {filter_val}"

        if not self.get_image_path(item):
            prompt = self.get_prompt(item)
            if prompt:
                base_text += f"\n\n{prompt}"

        return base_text

    @abstractmethod
    def format_timeout_message(self, item: dict | pd.Series, **kwargs) -> str:
        """Format the unescaped markdown message displayed when time runs out."""
        pass

    @abstractmethod
    def format_correct_message(self, item: dict | pd.Series, reward: int, total_credits: int | None = None, **kwargs) -> str:
        """Format the unescaped markdown message displayed when a user gives a correct answer."""
        pass

    @abstractmethod
    def format_wrong_message(self, item: dict | pd.Series, **kwargs) -> str:
        """Format the unescaped markdown message displayed when a user gives an incorrect answer."""
        pass
