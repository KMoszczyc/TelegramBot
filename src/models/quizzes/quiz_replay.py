from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update

from src.config.constants import QUIZ_REPLAY_BUTTON_TEXT, QUIZ_REPLAY_CALLBACK_PREFIX

COMMAND_NAME_BY_QUIZ_TYPE: dict[str, str] = {
    "quiz": "quiz",
    "map": "guessperson",
    "flag": "guessflag",
    "capital": "guesscapital",
}


def build_replay_callback_data(quiz_type: str, user_id: int) -> str:
    """Build callback data string for quiz replay button."""
    return f"{QUIZ_REPLAY_CALLBACK_PREFIX}{quiz_type}:{user_id}"


def parse_replay_callback_data(data: str | None) -> tuple[str, int] | None:
    """Parse callback data string returning (quiz_type, user_id) or None if invalid."""
    if not data or not data.startswith(QUIZ_REPLAY_CALLBACK_PREFIX):
        return None

    payload = data[len(QUIZ_REPLAY_CALLBACK_PREFIX) :]
    parts = payload.split(":", 1)
    if len(parts) != 2:
        return None

    quiz_type, user_id_str = parts
    try:
        return quiz_type, int(user_id_str)
    except ValueError:
        return None


def format_replay_command_name(quiz_type: str) -> str:
    """Format user-facing slash command name for the given quiz type."""
    return COMMAND_NAME_BY_QUIZ_TYPE.get(quiz_type, quiz_type)


def build_replay_keyboard(
    quiz_type: str,
    user_id: int,
    button_text: str = QUIZ_REPLAY_BUTTON_TEXT,
) -> InlineKeyboardMarkup:
    """Build an inline keyboard with a single Play Again button."""
    callback_data = build_replay_callback_data(quiz_type, user_id)
    return InlineKeyboardMarkup([[InlineKeyboardButton(button_text, callback_data=callback_data)]])


class ReplayUpdateProxy:
    """Proxy around Update for replay callbacks where update.message and effective properties are needed."""

    def __init__(self, original_update: Update, query_message: Any, user: Any):
        self._original = original_update
        self.message = query_message
        self.effective_message = query_message
        self.effective_user = user
        self.effective_chat = getattr(query_message, "chat", None) or getattr(original_update, "effective_chat", None)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._original, name)
