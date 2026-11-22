"""Presenter component for Wordle Telegram messages and inline keyboards.

Handles MarkdownV2 caption building, quote block formatting with safe length clamping,
and interactive inline keyboard state transitions.
"""

import re
from typing import ClassVar

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

import src.stats.utils as stats_utils
from src.config.constants import (
    TELEGRAM_CAPTION_MAX_LENGTH,
    WORDLE_SURRENDER_CALLBACK_PREFIX,
    WORDLE_TIP_CALLBACK_PREFIX,
)
from src.models.quizzes.wordle_game import WordleGame


class WordlePresenter:
    """Formats Wordle text captions and keyboards for Telegram."""

    MAX_CAPTION_LEN: ClassVar[int] = TELEGRAM_CAPTION_MAX_LENGTH
    MAX_QUOTE_CHARS: ClassVar[int] = 350

    @classmethod
    def build_keyboard(cls, user_id: int, game: WordleGame) -> InlineKeyboardMarkup:
        """Build the dynamic Wordle keyboard with surrender and tip buttons."""
        surrender_button = InlineKeyboardButton("🏳️ Surrender", callback_data=f"{WORDLE_SURRENDER_CALLBACK_PREFIX}{user_id}")
        if game.revealed_tip_tier == 2:
            return InlineKeyboardMarkup([[surrender_button]])

        if game.revealed_tip_tier == 1:
            if game.remaining_tries == 1:
                tip_button = InlineKeyboardButton("💡 Final Tip", callback_data=f"{WORDLE_TIP_CALLBACK_PREFIX}{user_id}")
            else:
                tip_button = InlineKeyboardButton("🔒 Final Tip (last guess)", callback_data=f"{WORDLE_TIP_CALLBACK_PREFIX}{user_id}")
            return InlineKeyboardMarkup([[tip_button, surrender_button]])

        # revealed_tip_tier == 0
        if len(game.guesses) >= (game.max_tries // 2):
            tip_button = InlineKeyboardButton("💡 Tip", callback_data=f"{WORDLE_TIP_CALLBACK_PREFIX}{user_id}")
        else:
            unlock_guess = (game.max_tries // 2) + 1
            tip_button = InlineKeyboardButton(f"🔒 Tip (guess {unlock_guess})", callback_data=f"{WORDLE_TIP_CALLBACK_PREFIX}{user_id}")
        return InlineKeyboardMarkup([[tip_button, surrender_button]])

    @classmethod
    def build_surrender_keyboard(cls, user_id: int) -> InlineKeyboardMarkup:
        """Build standalone surrender button keyboard."""
        return InlineKeyboardMarkup([[InlineKeyboardButton("🏳️ Surrender", callback_data=f"{WORDLE_SURRENDER_CALLBACK_PREFIX}{user_id}")]])

    @classmethod
    def build_initial_caption(cls, game: WordleGame) -> str:
        """Build the starting caption for a new Wordle game."""
        caption = f"🟩 *WORDLE* — {game.dataset_display_name} 🟨\n\nType your guess in chat!"
        return stats_utils.escape_special_characters(caption)

    @classmethod
    def build_in_progress_caption(cls, game: WordleGame) -> str:
        """Build the in-progress caption with active tip (if any) and guess prompt."""
        base_caption = f"🟩 *WORDLE* — {game.dataset_display_name} 🟨"
        tip_block = cls.build_tip_block(game)
        prompt = "Type your next guess!"
        escaped_caption = (
            stats_utils.escape_special_characters(base_caption) + tip_block + "\n\n" + stats_utils.escape_special_characters(prompt)
        )
        return cls._clamp_caption(escaped_caption)

    @classmethod
    def build_win_caption(cls, game: WordleGame, reward: int, user_credits: int) -> str:
        """Build caption for a won game with credit summary and quote."""
        base_caption = (
            f"🎉 *CONGRATULATIONS! You guessed the word:* *{game.target_word.upper()}*!\n\n"
            f"🎯 Guesses used: *{len(game.guesses)}/{game.max_tries}*\n"
            f"💰 Reward: *+{reward}* credits (Total: *{user_credits}*)"
        )
        quote_block = cls.build_quote_block(game)
        return cls._clamp_caption(stats_utils.escape_special_characters(base_caption) + quote_block)

    @classmethod
    def build_game_over_caption(cls, game: WordleGame) -> str:
        """Build caption when a player runs out of tries."""
        base_caption = (
            f"💀 *GAME OVER! You ran out of tries.*\n\n"
            f"🔑 The word was: *{game.target_word.upper()}*\n"
            f"🎯 Guesses used: *{len(game.guesses)}/{game.max_tries}*"
        )
        quote_block = cls.build_quote_block(game)
        return cls._clamp_caption(stats_utils.escape_special_characters(base_caption) + quote_block)

    @classmethod
    def build_surrender_caption(cls, game: WordleGame) -> str:
        """Build caption when a player surrenders."""
        base_caption = (
            f"🏳️ *YOU SURRENDERED!*\n\n🔑 The word was: *{game.target_word.upper()}*\n🎯 Guesses: *{len(game.guesses)}/{game.max_tries}*"
        )
        quote_block = cls.build_quote_block(game)
        return cls._clamp_caption(stats_utils.escape_special_characters(base_caption) + quote_block)

    @classmethod
    def build_timeout_caption(cls, game: WordleGame) -> str:
        """Build caption when the game expires on timeout."""
        base_caption = f"⏰ *TIME'S UP!*\n\n🔑 The word was: *{game.target_word.upper()}*"
        quote_block = cls.build_quote_block(game)
        return cls._clamp_caption(stats_utils.escape_special_characters(base_caption) + quote_block)

    @classmethod
    def build_tip_block(cls, game: WordleGame) -> str:
        """Return pre-escaped MarkdownV2 tip block, or empty string if no active tip."""
        active_tip = game.get_active_tip()
        if not active_tip:
            return ""
        escaped_tip = stats_utils.escape_special_characters(active_tip)
        tip_label = "Final Tip" if game.revealed_tip_tier == 2 else "Tip"
        return f"\n\n💡 *{tip_label}:* _{escaped_tip}_"

    @classmethod
    def build_quote_block(cls, game: WordleGame, max_chars: int | None = None) -> str:
        """Return a pre-escaped MarkdownV2 blockquote block safely clamped to avoid caption overflow."""
        if not game.source_quote:
            return ""
        quote = game.source_quote
        target = game.target_word
        limit = max_chars if max_chars is not None else cls.MAX_QUOTE_CHARS

        if len(quote) > limit:
            quote = cls._extract_snippet(quote, target, limit)

        escaped_quote = stats_utils.escape_special_characters(quote)
        escaped_name = stats_utils.escape_special_characters(game.source_attribution)
        escaped_word = re.escape(stats_utils.escape_special_characters(target))
        bolded = re.sub(
            rf"\b({escaped_word})\b",
            lambda m: f"*{m.group(1)}*",
            escaped_quote,
            flags=re.IGNORECASE,
        )
        return f"\n> From *{escaped_name}*:\n> _{bolded}_"

    @classmethod
    def _extract_snippet(cls, quote: str, target: str, max_chars: int) -> str:
        """Extract a centered window around target word preserving word boundaries."""
        pattern = re.compile(rf"\b{re.escape(target)}\b", re.IGNORECASE)
        m = pattern.search(quote)
        if not m:
            return quote[:max_chars].rstrip() + " ..."

        t_start, t_end = m.span()
        target_len = t_end - t_start
        half = max(0, (max_chars - target_len) // 2)
        start_char = max(0, t_start - half)
        end_char = min(len(quote), t_end + half)

        if start_char > 0:
            prev_space = quote.find(" ", start_char)
            if prev_space != -1 and prev_space < t_start:
                start_char = prev_space + 1
        if end_char < len(quote):
            next_space = quote.rfind(" ", t_end, end_char)
            if next_space != -1 and next_space > t_start:
                end_char = next_space

        snippet = quote[start_char:end_char]
        if start_char > 0:
            snippet = f"... {snippet}"
        if end_char < len(quote):
            snippet = f"{snippet} ..."
        return snippet

    @classmethod
    def _clamp_caption(cls, caption: str) -> str:
        """Hard safety clamp guaranteeing the caption never exceeds Telegram's limit."""
        if len(caption) <= cls.MAX_CAPTION_LEN:
            return caption
        return caption[: cls.MAX_CAPTION_LEN - 3] + "..."
