from unittest.mock import MagicMock

import pytest
from telegram import InlineKeyboardMarkup

from src.config.constants import QUIZ_REPLAY_BUTTON_TEXT, QUIZ_REPLAY_CALLBACK_PREFIX
from src.models.quizzes.quiz_replay import (
    ReplayUpdateProxy,
    build_replay_callback_data,
    build_replay_keyboard,
    format_replay_command_name,
    parse_replay_callback_data,
)


@pytest.mark.parametrize(
    "quiz_type, user_id, expected",
    [
        ("quiz", 12345, f"{QUIZ_REPLAY_CALLBACK_PREFIX}quiz:12345"),
        ("map", 987654321, f"{QUIZ_REPLAY_CALLBACK_PREFIX}map:987654321"),
        ("flag", 1, f"{QUIZ_REPLAY_CALLBACK_PREFIX}flag:1"),
        ("capital", 42, f"{QUIZ_REPLAY_CALLBACK_PREFIX}capital:42"),
    ],
)
def test_build_replay_callback_data(quiz_type: str, user_id: int, expected: str):
    assert build_replay_callback_data(quiz_type, user_id) == expected


@pytest.mark.parametrize(
    "callback_data, expected",
    [
        (f"{QUIZ_REPLAY_CALLBACK_PREFIX}quiz:12345", ("quiz", 12345)),
        (f"{QUIZ_REPLAY_CALLBACK_PREFIX}map:999", ("map", 999)),
        (f"{QUIZ_REPLAY_CALLBACK_PREFIX}flag:1", ("flag", 1)),
        (f"{QUIZ_REPLAY_CALLBACK_PREFIX}capital:42", ("capital", 42)),
        ("invalid_prefix:quiz:123", None),
        ("quiztip:123", None),
        ("", None),
        (None, None),
        (f"{QUIZ_REPLAY_CALLBACK_PREFIX}quiz", None),
        (f"{QUIZ_REPLAY_CALLBACK_PREFIX}quiz:not_an_int", None),
        (f"{QUIZ_REPLAY_CALLBACK_PREFIX}", None),
    ],
)
def test_parse_replay_callback_data(callback_data: str | None, expected: tuple[str, int] | None):
    assert parse_replay_callback_data(callback_data) == expected


@pytest.mark.parametrize(
    "quiz_type, expected_cmd",
    [
        ("quiz", "quiz"),
        ("map", "guessperson"),
        ("flag", "guessflag"),
        ("capital", "guesscapital"),
        ("unknown_game", "unknown_game"),
    ],
)
def test_format_replay_command_name(quiz_type: str, expected_cmd: str):
    assert format_replay_command_name(quiz_type) == expected_cmd


@pytest.mark.parametrize("quiz_type", ["quiz", "map", "flag", "capital"])
def test_build_replay_keyboard(quiz_type: str):
    keyboard = build_replay_keyboard(quiz_type, 12345)
    assert isinstance(keyboard, InlineKeyboardMarkup)
    assert len(keyboard.inline_keyboard) == 1
    assert len(keyboard.inline_keyboard[0]) == 1

    button = keyboard.inline_keyboard[0][0]
    assert button.text == QUIZ_REPLAY_BUTTON_TEXT
    assert button.callback_data == f"{QUIZ_REPLAY_CALLBACK_PREFIX}{quiz_type}:12345"


def test_build_replay_keyboard_custom_text():
    keyboard = build_replay_keyboard("quiz", 999, button_text="Play Once More")
    button = keyboard.inline_keyboard[0][0]
    assert button.text == "Play Once More"
    assert button.callback_data == f"{QUIZ_REPLAY_CALLBACK_PREFIX}quiz:999"


@pytest.mark.parametrize("has_chat", [True, False])
def test_replay_update_proxy_properties(has_chat: bool):
    original_update = MagicMock()
    original_update.update_id = 42
    original_update.custom_attr = "hello"

    query_message = MagicMock()
    if has_chat:
        mock_chat = MagicMock()
        mock_chat.id = 999
        query_message.chat = mock_chat
    else:
        query_message.chat = None
        mock_fallback_chat = MagicMock()
        mock_fallback_chat.id = 777
        original_update.effective_chat = mock_fallback_chat

    mock_user = MagicMock()
    mock_user.id = 111

    proxy = ReplayUpdateProxy(original_update, query_message, mock_user)

    assert proxy.message is query_message
    assert proxy.effective_message is query_message
    assert proxy.effective_user is mock_user
    if has_chat:
        assert proxy.effective_chat.id == 999
    else:
        assert proxy.effective_chat.id == 777
    assert proxy.update_id == 42
    assert proxy.custom_attr == "hello"


@pytest.mark.parametrize(
    "quiz_type, expected",
    [("topcountries", "guesstopcountries"), ("capital", "guesscapital"), ("unknown", "unknown")],
)
def test_format_replay_command_name_top_countries(quiz_type: str, expected: str):
    assert format_replay_command_name(quiz_type) == expected
