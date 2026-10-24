from dataclasses import dataclass

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

import src.stats.utils as stats_utils
from src.config.constants import (
    QUIZ_TIP_CALLBACK_PREFIX,
    TELEGRAM_CAPTION_MAX_LENGTH,
    TELEGRAM_MESSAGE_MAX_LENGTH,
)


@dataclass
class TipDisplayPlan:
    """Carries layout and routing decisions for displaying a freshly revealed tip."""

    keyboard: InlineKeyboardMarkup
    can_edit: bool
    edit_text: str | None
    overflow_text: str
    has_photo: bool
    chat_id: int
    message_id: int | None
    thread_id: int | None


def build_tip_callback_data(user_id: int) -> str:
    """Return the callback data carried by the tip button of *user_id*'s quiz."""
    return f"{QUIZ_TIP_CALLBACK_PREFIX}{user_id}"


def parse_tip_callback_data(data: str | None) -> int | None:
    """Extract the quiz owner's user id from tip button callback data, or None if it is malformed."""
    if not data or not data.startswith(QUIZ_TIP_CALLBACK_PREFIX):
        return None
    try:
        return int(data[len(QUIZ_TIP_CALLBACK_PREFIX) :])
    except ValueError:
        return None


def format_tip_line(tip_number: int, total_tips: int, reward: int, decrease_pct: int, tip_text: str) -> str:
    """Format a revealed tip together with the reward left after taking it."""
    return f"💡 Tip {tip_number}/{total_tips} - [{reward} credits, -{decrease_pct}%]: {tip_text}"


def build_tip_label(quiz_class, difficulty: str, continent_specified: bool, tips_given: int, total_tips: int) -> str | None:
    """Return the tip button label showing the reward after the next tip, or None when no tips are left."""
    if tips_given >= total_tips:
        return None
    next_reward, _ = quiz_class.get_reward(difficulty, continent_specified, tips_given + 1)
    return f"💡 Tip {tips_given + 1}/{total_tips} (reward → {next_reward})"


def build_tip_keyboard(user_id: int, label: str | None) -> InlineKeyboardMarkup:
    """Build the single-button keyboard, or an empty keyboard (which removes the button) when *label* is None."""
    if label is None:
        return InlineKeyboardMarkup([])
    return InlineKeyboardMarkup([[InlineKeyboardButton(label, callback_data=build_tip_callback_data(user_id))]])


def build_initial_tip_keyboard(
    quiz_class,
    user_id: int,
    difficulty: str,
    filter_specified: bool,
    total_tips: int,
) -> InlineKeyboardMarkup:
    """Build the initial tip button keyboard for a newly started quiz session."""
    label = build_tip_label(quiz_class, difficulty, filter_specified, 0, total_tips)
    return build_tip_keyboard(user_id, label)


def compose_quiz_text(base_text: str, tip_lines: list[str], limit: int) -> str | None:
    """Join the quiz text with its revealed tips and escape it for MarkdownV2, or return None if it exceeds *limit*."""
    raw_text = "\n\n".join([base_text, "\n".join(tip_lines)]) if tip_lines else base_text
    escaped_text = stats_utils.escape_special_characters(raw_text)
    return escaped_text if len(escaped_text) <= limit else None


def reveal_next_tip(cached_quiz: dict, quiz, quiz_class, item) -> str | None:
    """Advance the quiz to its next tip, record it in the cache and return its formatted line, or None if none are left."""
    continent_specified = cached_quiz.get("continent_specified", cached_quiz.get("category_specified", False))
    tips = quiz.get_tips(item, continent_specified=continent_specified)
    tips_given = cached_quiz.get("tips_given", 0)
    if tips_given >= len(tips):
        return None

    cached_quiz["tips_given"] = tips_given + 1
    reward, decrease = quiz_class.get_reward(cached_quiz.get("difficulty", "crazy"), continent_specified, tips_given + 1)
    tip_line = format_tip_line(tips_given + 1, len(tips), reward, decrease, tips[tips_given])
    cached_quiz.setdefault("tip_lines", []).append(tip_line)
    return tip_line


def plan_tip_display(
    cached_quiz: dict,
    quiz,
    quiz_class,
    user_id: int,
    tip_line: str,
) -> TipDisplayPlan:
    """Compute whether to edit the quiz message in-place or reply with overflow, and prepare texts/keyboards."""
    has_photo = bool(cached_quiz.get("has_photo", False))
    limit = TELEGRAM_CAPTION_MAX_LENGTH if has_photo else TELEGRAM_MESSAGE_MAX_LENGTH
    message_id = cached_quiz.get("message_id")
    chat_id = cached_quiz["chat_id"]
    thread_id = cached_quiz.get("thread_id")

    item = cached_quiz.get("country") or cached_quiz.get("person")
    filter_specified = cached_quiz.get("continent_specified", cached_quiz.get("category_specified", False))
    total_tips = len(quiz.get_tips(item, continent_specified=filter_specified))

    label = build_tip_label(
        quiz_class,
        cached_quiz.get("difficulty", "crazy"),
        filter_specified,
        cached_quiz.get("tips_given", 0),
        total_tips,
    )
    keyboard = build_tip_keyboard(user_id, label)

    edit_text = None
    if message_id is not None and cached_quiz.get("base_text") is not None:
        edit_text = compose_quiz_text(cached_quiz["base_text"], cached_quiz.get("tip_lines", []), limit)

    can_edit = edit_text is not None
    overflow_text = stats_utils.escape_special_characters(tip_line)

    return TipDisplayPlan(
        keyboard=keyboard,
        can_edit=can_edit,
        edit_text=edit_text,
        overflow_text=overflow_text,
        has_photo=has_photo,
        chat_id=chat_id,
        message_id=message_id,
        thread_id=thread_id,
    )
