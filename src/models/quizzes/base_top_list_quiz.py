"""Shared base structures, utilities, and abstract game class for Top List Quizzes."""

from __future__ import annotations

import math
import random
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

UNKNOWN_SLOT = "???"
STRIKE_ICON = "❌"
EMPTY_STRIKE_ICON = "✖️"
MAX_LAST_GUESS_LENGTH = 30
_MARKDOWN_UNSAFE_CHARS = re.compile(r"[*`\\]")


class PoolKind(Enum):
    WORLD = "world"
    CONTINENT = "continent"
    COUNTRY = "country"
    DISTRICT = "district"
    LETTER = "letter"
    CUSTOM = "custom"


class GuessResult(Enum):
    CORRECT = "correct"
    WRONG = "wrong"
    DUPLICATE = "duplicate"


class GameStatus(Enum):
    IN_PROGRESS = "in_progress"
    ACTIVE = "active"
    WON = "won"
    LOST = "lost"
    TIMEOUT = "timeout"


@dataclass(frozen=True)
class Pool:
    kind: PoolKind
    target: str
    size: int
    extra: tuple[tuple[str, str], ...] = ()

    @property
    def key(self) -> str:
        return self.target

    @property
    def extra_dict(self) -> dict[str, str]:
        """Return `extra` as a plain dict for convenient key lookup."""
        return dict(self.extra)

    @property
    def description(self) -> str:
        """Return the human readable pool filter line, e.g. 'Continent: Europa'."""
        if self.kind == PoolKind.CONTINENT:
            return f"Continent: {self.target}"
        if self.kind == PoolKind.LETTER:
            return f"Starting letter: {self.target}"
        if self.kind == PoolKind.COUNTRY:
            return f"Country: {self.target}"
        if self.kind == PoolKind.DISTRICT:
            return f"District: {self.target}"
        return f"Scope: {self.target}"


def parse_guesses(text: str) -> list[str]:
    """Split comma-separated guess text into trimmed, non-empty candidate guesses."""
    return [part.strip() for part in text.split(",") if part.strip()]


def sanitize_guess(text: str) -> str:
    """Strip markdown control characters from user text and truncate it so it is safe to echo."""
    return _MARKDOWN_UNSAFE_CHARS.sub("", text).strip()[:MAX_LAST_GUESS_LENGTH]


def render_strikes(strikes: int, max_strikes: int = 3, separator: str = "") -> str:
    """Render strikes display e.g. ❌❌✖️ or ❌ ❌ ✖️."""
    icons = [STRIKE_ICON] * strikes
    icons += [EMPTY_STRIKE_ICON] * max(0, max_strikes - strikes)
    return separator.join(icons)


def calculate_top_reward(
    list_size: int,
    correct: int,
    tips_given: int,
    strikes: int,
    base_rewards: dict[int, int],
    tip_multiplier: float,
    strike_multiplier: float,
    completion_exponent: float,
) -> int:
    """Generic formula for Top List quiz rewards with strike and tip penalties."""
    min_correct = math.ceil(list_size / 2)
    if correct < min_correct:
        return 0
    base = base_rewards.get(list_size, 0)
    completion = (correct / list_size) ** completion_exponent
    tip_mult = tip_multiplier**tips_given
    strike_mult = strike_multiplier**strikes
    return int(round(base * completion * tip_mult * strike_mult))


def build_tip_callback_data(prefix: str, user_id: int) -> str:
    """Build callback_data string for a tip button."""
    return f"{prefix}{user_id}"


def parse_tip_callback_data(prefix: str, data: str) -> int | None:
    """Extract owner user_id from the tip callback data or None if invalid."""
    if not data or not data.startswith(prefix):
        return None
    try:
        return int(data[len(prefix) :])
    except ValueError:
        return None


class BaseTopListGame(ABC):
    """Abstract base class managing state, batching, and rendering for Top List quizzes."""

    def __init__(
        self,
        pool: Pool,
        entries: list[Any],
        max_strikes: int = 3,
        max_tips: int = 5,
        tip_callback_prefix: str = "",
        strike_separator: str = "",
    ):
        self.pool = pool
        self.entries = entries
        self.guessed: set[int] = set()
        self.strikes: int = 0
        self.tips_given: int = 0
        self.last_guess: str | None = None
        self.timed_out: bool = False
        self.max_strikes = max_strikes
        self.max_tips = max_tips
        self.tip_callback_prefix = tip_callback_prefix
        self.strike_separator = strike_separator

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
        if self.strikes >= self.max_strikes:
            return GameStatus.LOST
        if self.timed_out:
            return GameStatus.TIMEOUT
        return GameStatus.IN_PROGRESS

    @property
    def is_over(self) -> bool:
        return self.status not in (GameStatus.IN_PROGRESS, GameStatus.ACTIVE)

    @property
    @abstractmethod
    def reward(self) -> int:
        """Calculate current credit reward."""

    @property
    def can_give_tip(self) -> bool:
        """Whether a new tip can still be given."""
        return not self.is_over and self.tips_given < self.max_tips and len(self._missing_indices()) > 0

    def _missing_indices(self) -> list[int]:
        return [i for i in range(self.list_size) if i not in self.guessed]

    @abstractmethod
    def submit_guess(self, text: str) -> GuessResult:
        """Evaluate a single guess."""

    @abstractmethod
    def reveal_tip(self, rng: random.Random | None = None) -> Any:
        """Reveal one tip and return its slot index or None."""

    @abstractmethod
    def format_title(self) -> str:
        """Generate quiz header text."""

    @abstractmethod
    def _render_slot(self, index: int) -> str:
        """Render a single slot line."""

    def submit_guesses(self, text: str) -> list[GuessResult]:
        """Apply comma-separated guesses in order until all are evaluated or game ends."""
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

    def render_strikes(self) -> str:
        """Render strikes display e.g. ❌❌✖️."""
        return render_strikes(self.strikes, self.max_strikes, separator=self.strike_separator)

    def render(self, result_line: str | None = None) -> str:
        """Render full Markdown V2 quiz message."""
        lines = [self.format_title(), ""]
        for i in range(self.list_size):
            lines.append(self._render_slot(i))

        lines.append("")
        if not self.is_over:
            lines.append(f"Strikes: {self.render_strikes()}")
            if self.last_guess:
                lines.append(f"Last guess: {self.last_guess}")
        elif result_line:
            lines.append(result_line)

        return "\n".join(lines)

    def format_result_line(self, total_credits: int | None = None) -> str:
        """Summary message shown when the quiz finishes."""
        credits_str = f" [Total: *{total_credits:,}* credits]" if total_credits is not None else ""
        match self.status:
            case GameStatus.WON:
                return f"🎉 *Congratulations!* You guessed all {self.list_size} items! Received *{self.reward:,}* credits!{credits_str}"
            case GameStatus.LOST:
                reward_str = f" Received *{self.reward:,}* credits!{credits_str}" if self.reward > 0 else ""
                return f"💀 *Game Over!* {self.max_strikes} strikes reached.{reward_str}"
            case GameStatus.TIMEOUT:
                reward_str = f" Received *{self.reward:,}* credits!{credits_str}" if self.reward > 0 else ""
                return f"⏰ *Time's up!*{reward_str}"
            case _:
                return ""

    def build_keyboard(self, user_id: int) -> InlineKeyboardMarkup | None:
        """Build inline keyboard with the tip button if tips are still available."""
        if self.is_over or not self.can_give_tip:
            return None
        remaining = max(0, self.max_tips - self.tips_given)
        data = build_tip_callback_data(self.tip_callback_prefix, user_id)
        return InlineKeyboardMarkup([[InlineKeyboardButton(f"💡 Tip ({remaining} left)", callback_data=data)]])
