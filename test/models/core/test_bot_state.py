from unittest.mock import MagicMock

import pytest

from src.models.core.bot_state import BotState
from src.models.quizzes.capital_quiz import CapitalQuiz
from src.models.quizzes.flag_quiz import FlagQuiz
from src.models.quizzes.map_quiz import MapQuiz


@pytest.fixture
def bot_state():
    return BotState(job_queue=MagicMock(), assets=MagicMock())


@pytest.mark.parametrize(
    "quiz_type, args",
    [
        ("quiz", ["easy", "--category", "history"]),
        ("flag", ["hard", "--continent", "Europe"]),
        ("capital", []),
        ("map", None),
    ],
)
def test_quiz_replay_args_roundtrip(bot_state, quiz_type, args):
    user_id = 123
    assert bot_state.get_quiz_replay_args(user_id, quiz_type) == []

    bot_state.set_quiz_replay_args(user_id, quiz_type, args)
    expected = list(args) if args else []
    assert bot_state.get_quiz_replay_args(user_id, quiz_type) == expected

    # Verify mutations on returned list don't mutate cache
    retrieved = bot_state.get_quiz_replay_args(user_id, quiz_type)
    retrieved.append("extra")
    assert bot_state.get_quiz_replay_args(user_id, quiz_type) == expected


@pytest.mark.parametrize(
    "cache_attr, expected_quiz_class",
    [
        ("map_quiz_cache", MapQuiz),
        ("flag_quiz_cache", FlagQuiz),
        ("capital_quiz_cache", CapitalQuiz),
    ],
)
def test_has_and_find_active_guess_quiz(bot_state, cache_attr, expected_quiz_class):
    user_id = 456
    assert not bot_state.has_active_guess_quiz(user_id)
    assert bot_state.find_active_guess_quiz(user_id) is None

    cache = getattr(bot_state, cache_attr)
    session_data = {"country": {"name": "Test"}}
    cache[user_id] = session_data

    assert bot_state.has_active_guess_quiz(user_id)
    found = bot_state.find_active_guess_quiz(user_id)
    assert found is not None
    found_cache, found_class = found
    assert found_cache is cache
    assert found_class is expected_quiz_class
