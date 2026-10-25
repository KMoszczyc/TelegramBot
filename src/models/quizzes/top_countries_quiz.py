import math
import random
import re
from dataclasses import dataclass, field
from enum import Enum

import pandas as pd
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from src.config.constants import (
    TOP_COUNTRIES_BASE_REWARDS,
    TOP_COUNTRIES_COMPLETION_EXPONENT,
    TOP_COUNTRIES_MAX_STRIKES,
    TOP_COUNTRIES_MAX_TIPS,
    TOP_COUNTRIES_MIN_BUFFER,
    TOP_COUNTRIES_STRIKE_MULTIPLIER,
    TOP_COUNTRIES_TIP_CALLBACK_PREFIX,
    TOP_COUNTRIES_TIP_MULTIPLIER,
)
from src.models.quizzes.base_guess_quiz import BaseGuessQuiz
from src.models.quizzes.flag_quiz import FlagQuiz

QUIZ_TYPE = "topcountries"
LIST_SIZES = (10, 5, 3)
UNKNOWN_SLOT = "???"
STRIKE_ICON = "❌"
EMPTY_STRIKE_ICON = "✖️"
MAX_LAST_GUESS_LENGTH = 30
_MARKDOWN_UNSAFE_CHARS = re.compile(r"[*`\\]")


class Metric(Enum):
    POPULATION = ("population", "population", "population")
    AREA = ("area_sq_km", "area (km²)", "area")

    def __init__(self, column: str, label: str, short_name: str):
        self.column = column
        self.label = label
        self.short_name = short_name


METRIC_ALIASES = {
    "population": Metric.POPULATION,
    "pop": Metric.POPULATION,
    "area": Metric.AREA,
    "size": Metric.AREA,
    "area_sq_km": Metric.AREA,
}


class Language(Enum):
    EN = "en"
    PL = "pl"

    @property
    def column(self) -> str:
        return "country_name_en" if self == Language.EN else "country_name"


LANGUAGE_ALIASES = {
    "en": Language.EN,
    "english": Language.EN,
    "pl": Language.PL,
    "polish": Language.PL,
}


def parse_language(raw: str | None) -> Language | None:
    """Map a user-supplied `--language` value onto a Language, or None when not recognised."""
    if raw is None:
        return None
    return LANGUAGE_ALIASES.get(str(raw).strip().lower())


class PoolKind(Enum):
    CONTINENT = "continent"
    LETTER = "letter"


class GuessResult(Enum):
    CORRECT = "correct"
    WRONG = "wrong"
    DUPLICATE = "duplicate"


class GameStatus(Enum):
    ACTIVE = "active"
    WON = "won"
    LOST = "lost"
    TIMEOUT = "timeout"


@dataclass(frozen=True)
class Pool:
    kind: PoolKind
    key: str
    size: int

    @property
    def description(self) -> str:
        """Return the human readable pool filter line, e.g. 'Continent: Europa'."""
        return f"Continent: {self.key}" if self.kind == PoolKind.CONTINENT else f"Starting letter: {self.key}"


@dataclass
class Entry:
    name: str
    value: float
    valid_answers: set[str]


def parse_metric(raw: str | None) -> Metric | None:
    """Map a user supplied `--by` value onto a Metric, or None when it is not recognised."""
    if raw is None:
        return None
    return METRIC_ALIASES.get(str(raw).strip().lower())


def get_list_size(pool_count: int) -> int | None:
    """Return the biggest top-N list that leaves at least the required buffer of extra countries, or None."""
    for size in LIST_SIZES:
        if pool_count >= size + TOP_COUNTRIES_MIN_BUFFER:
            return size
    return None


def parse_guesses(text: str) -> list[str]:
    """Split comma-separated guess text into trimmed, non-empty candidate guesses."""
    return [part.strip() for part in text.split(",") if part.strip()]


def _first_letters(df: pd.DataFrame, language: Language = Language.EN) -> pd.Series:
    return df[language.column].astype(str).str.strip().str[0].str.upper()


def get_eligible_pools(df: pd.DataFrame, language: Language = Language.EN) -> list[Pool]:
    """List every continent and starting letter that has enough countries for at least a Top 3."""
    pools: list[Pool] = []
    for kind, counts in (
        (PoolKind.CONTINENT, df["continent"].value_counts().sort_index()),
        (PoolKind.LETTER, _first_letters(df, language).value_counts().sort_index()),
    ):
        for key, count in counts.items():
            size = get_list_size(int(count))
            if size is not None:
                pools.append(Pool(kind, str(key), size))
    return pools


def pick_pool(
    df: pd.DataFrame,
    rng: random.Random | None = None,
    language: Language = Language.EN,
) -> Pool:
    """Pick one eligible pool uniformly at random."""
    return (rng or random).choice(get_eligible_pools(df, language))


def get_pool_df(df: pd.DataFrame, pool: Pool, language: Language = Language.EN) -> pd.DataFrame:
    """Return the countries belonging to *pool*."""
    if pool.kind == PoolKind.CONTINENT:
        return df[df["continent"] == pool.key]
    return df[_first_letters(df, language) == pool.key]


def build_valid_answers(row: dict | pd.Series) -> set[str]:
    """Build valid answers from both Polish and English country names and code."""
    valid = FlagQuiz().get_valid_answers(row)
    if not isinstance(row, dict):
        row = row.to_dict()
    en_name = str(row.get("country_name_en", "")).lower().strip()
    if en_name and en_name != "nan":
        valid.add(en_name)
        for part in en_name.split():
            if len(part) >= 3:
                valid.add(part)
    return valid


def calculate_reward(list_size: int, correct: int, tips_given: int, strikes: int) -> int:
    """Credits for *correct* countries out of *list_size*, after the tip and strike penalties."""
    min_correct = math.ceil(list_size / 2)
    if correct < min_correct:
        return 0
    completion = ((correct - min_correct + 1) / (list_size - min_correct + 1)) ** TOP_COUNTRIES_COMPLETION_EXPONENT
    base = TOP_COUNTRIES_BASE_REWARDS[list_size]
    return int(round(base * completion * TOP_COUNTRIES_TIP_MULTIPLIER**tips_given * TOP_COUNTRIES_STRIKE_MULTIPLIER**strikes))


def build_tip_callback_data(user_id: int) -> str:
    """Return the callback data carried by the tip button of *user_id*'s game."""
    return f"{TOP_COUNTRIES_TIP_CALLBACK_PREFIX}{user_id}"


def parse_tip_callback_data(data: str | None) -> int | None:
    """Extract the game owner's user id from tip button callback data, or None if it is malformed."""
    if not data or not data.startswith(TOP_COUNTRIES_TIP_CALLBACK_PREFIX):
        return None
    try:
        return int(data[len(TOP_COUNTRIES_TIP_CALLBACK_PREFIX) :])
    except ValueError:
        return None


def format_value(value: float, metric: Metric) -> str:
    """Format a metric value with thousands separators (and km² for area)."""
    formatted = f"{int(value):,}"
    return f"{formatted} km²" if metric == Metric.AREA else formatted


def sanitize_guess(text: str) -> str:
    """Strip markdown control characters from user text and truncate it so it is safe to echo."""
    return _MARKDOWN_UNSAFE_CHARS.sub("", text).strip()[:MAX_LAST_GUESS_LENGTH]


@dataclass
class TopCountriesGame:
    metric: Metric
    pool: Pool
    entries: list[Entry]
    guessed: set[int] = field(default_factory=set)
    strikes: int = 0
    tip_letters: dict[int, int] = field(default_factory=dict)
    tips_given: int = 0
    last_guess: str | None = None
    timed_out: bool = False
    language: Language = Language.EN

    @classmethod
    def create(
        cls,
        countries_df: pd.DataFrame,
        metric: Metric | None = None,
        pool: Pool | None = None,
        language: Language = Language.EN,
        rng: random.Random | None = None,
    ) -> "TopCountriesGame":
        """Build a new game from the countries dataframe, choosing the metric and pool at random when not given."""
        rng = rng or random
        metric = metric or rng.choice(list(Metric))
        pool = pool or pick_pool(countries_df, rng=rng, language=language)
        top_df = get_pool_df(countries_df, pool, language).sort_values(metric.column, ascending=False, kind="stable").head(pool.size)
        col = language.column
        entries = [Entry(str(row[col]), float(row[metric.column]), build_valid_answers(row)) for row in top_df.to_dict("records")]
        return cls(metric=metric, pool=pool, entries=entries, language=language)

    @property
    def list_size(self) -> int:
        return len(self.entries)

    @property
    def correct_count(self) -> int:
        return len(self.guessed)

    @property
    def min_correct(self) -> int:
        return math.ceil(self.list_size / 2)

    @property
    def status(self) -> GameStatus:
        if self.correct_count == self.list_size:
            return GameStatus.WON
        if self.strikes >= TOP_COUNTRIES_MAX_STRIKES:
            return GameStatus.LOST
        if self.timed_out:
            return GameStatus.TIMEOUT
        return GameStatus.ACTIVE

    @property
    def is_over(self) -> bool:
        return self.status != GameStatus.ACTIVE

    @property
    def reward(self) -> int:
        return calculate_reward(self.list_size, self.correct_count, self.tips_given, self.strikes)

    def _missing_indices(self) -> list[int]:
        return [i for i in range(self.list_size) if i not in self.guessed]

    def _find_match(self, text: str, indices: list[int]) -> int | None:
        for i in indices:
            if text in self.entries[i].valid_answers:
                return i
        for i in indices:
            if BaseGuessQuiz.is_answer_correct(text, self.entries[i].valid_answers):
                return i
        return None

    def submit_guess(self, text: str) -> GuessResult:
        """Apply a guess: a remaining country is placed, a repeat of a placed country is ignored, anything else is a strike."""
        normalized = text.lower().strip()
        match = self._find_match(normalized, self._missing_indices())
        if match is not None:
            self.guessed.add(match)
            self.last_guess = f"*{self.entries[match].name}* ✅"
            return GuessResult.CORRECT

        if self._find_match(normalized, sorted(self.guessed)) is not None:
            return GuessResult.DUPLICATE

        self.strikes += 1
        clean_guess = sanitize_guess(text)
        self.last_guess = f"*{clean_guess}* {STRIKE_ICON}" if clean_guess else STRIKE_ICON
        return GuessResult.WRONG

    def submit_guesses(self, text: str) -> list[GuessResult]:
        """Apply comma-separated guesses in order until all are evaluated or the game ends."""
        guesses = parse_guesses(text)
        if not guesses:
            return []

        results = []
        batch_feedback = []
        for guess in guesses:
            if self.is_over:
                break
            res = self.submit_guess(guess)
            results.append(res)
            if res != GuessResult.DUPLICATE and self.last_guess:
                batch_feedback.append(self.last_guess)

        if len(batch_feedback) > 1:
            self.last_guess = ", ".join(batch_feedback)

        return results

    def _tip_candidates(self) -> list[int]:
        missing = self._missing_indices()
        if not missing:
            return []

        start_pos = 1 if self.pool.kind == PoolKind.LETTER else 0
        eligible = [i for i in missing if start_pos + self.tip_letters.get(i, 0) < len(self.entries[i].name)]
        if not eligible:
            return []

        zero_tips = [i for i in eligible if self.tip_letters.get(i, 0) == 0]
        if zero_tips:
            return zero_tips

        min_count = min(self.tip_letters.get(i, 0) for i in eligible)
        return [i for i in eligible if self.tip_letters.get(i, 0) == min_count]

    def can_give_tip(self) -> bool:
        return not self.is_over and self.tips_given < TOP_COUNTRIES_MAX_TIPS and bool(self._tip_candidates())

    def reveal_tip(self, rng: random.Random | None = None) -> int | None:
        """Reveal one letter of a missing country; return its index or None when no tip is possible."""
        if not self.can_give_tip():
            return None
        index = (rng or random).choice(self._tip_candidates())
        self.tip_letters[index] = self.tip_letters.get(index, 0) + 1
        self.tips_given += 1
        return index

    def _render_slot(self, index: int) -> str:
        entry = self.entries[index]
        number = f"{index + 1}."
        if index in self.guessed:
            return f"{number} *{entry.name}* - {format_value(entry.value, self.metric)}"
        if self.is_over:
            return f"{number} {STRIKE_ICON} *{entry.name}* - {format_value(entry.value, self.metric)}"
        if index in self.tip_letters and self.tip_letters[index] > 0:
            count = self.tip_letters[index]
            if self.pool.kind == PoolKind.LETTER:
                letters = entry.name[1 : 1 + count]
                return f"{number} ?{letters}{UNKNOWN_SLOT}"
            else:
                letters = entry.name[:count]
                letters = letters[0].upper() + letters[1:] if letters else letters
                return f"{number} {letters}{UNKNOWN_SLOT}"
        return f"{number} {UNKNOWN_SLOT}"

    def format_title(self) -> str:
        """Return the formatted header sentence for the quiz, e.g. Guess Top 5 most populated countries that start with letter *\"S\"*."""
        metric_phrase = "most populated countries" if self.metric == Metric.POPULATION else "largest countries"
        if self.pool.kind == PoolKind.LETTER:
            return f'Guess Top {self.list_size} {metric_phrase} that start with letter *"{self.pool.key}"*'
        return f"Guess Top {self.list_size} {metric_phrase} in *{self.pool.key}*"

    def render_strikes(self) -> str:
        """Return the strikes row: one red cross per strike, grey crosses for the strikes left."""
        return STRIKE_ICON * self.strikes + EMPTY_STRIKE_ICON * (TOP_COUNTRIES_MAX_STRIKES - self.strikes)

    def render(self, result_line: str | None = None) -> str:
        """Return the unescaped MarkdownV2-ready message text for the current game state."""
        lines = [
            self.format_title(),
            f"At least {self.min_correct} correct answers are needed for a reward",
            "",
            *(self._render_slot(i) for i in range(self.list_size)),
            "",
            f"Strikes: {self.render_strikes()}",
            f"Tips: {self.tips_given}/{TOP_COUNTRIES_MAX_TIPS}",
            f"Reward: {self.reward} credits",
        ]
        if self.last_guess:
            lines.append(f"Last guess: {self.last_guess}")
        if result_line:
            lines.extend(["", result_line])
        elif not self.is_over:
            lines.extend(["", "Type a country name to guess (or !tip)."])
        return "\n".join(lines)

    def format_result_line(self, total_credits: int | None = None) -> str:
        """Return the end-of-game summary line including the credits received."""
        headlines = {
            GameStatus.WON: f"🎉 Perfect! You got all {self.list_size}!",
            GameStatus.LOST: f"💀 {TOP_COUNTRIES_MAX_STRIKES} strikes, game over. You got {self.correct_count}/{self.list_size}.",
            GameStatus.TIMEOUT: f"⏰ Time's up! You got {self.correct_count}/{self.list_size}.",
        }
        reward = self.reward
        if reward <= 0:
            return f"{headlines[self.status]} No reward this time."
        credits_part = f"You receive *{reward}* credits!"
        if total_credits is not None:
            credits_part += f" [*{total_credits}* in total]"
        return f"{headlines[self.status]} {credits_part}"

    def build_tip_label(self) -> str | None:
        """Return the tip button label showing the reward after the next tip, or None when no tip can be taken."""
        if not self.can_give_tip():
            return None
        next_reward = calculate_reward(self.list_size, self.correct_count, self.tips_given + 1, self.strikes)
        return f"💡 Tip {self.tips_given + 1}/{TOP_COUNTRIES_MAX_TIPS} (reward → {next_reward})"

    def build_keyboard(self, user_id: int) -> InlineKeyboardMarkup:
        """Build the tip button keyboard, empty (which removes the button) when no tip is available."""
        label = self.build_tip_label()
        if label is None:
            return InlineKeyboardMarkup([])
        return InlineKeyboardMarkup([[InlineKeyboardButton(label, callback_data=build_tip_callback_data(user_id))]])
