from collections import defaultdict
from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest
import telegram

from src.commands.quiz_commands import QuizCommands
from src.models.quizzes.city_quiz import CityQuiz


@pytest.fixture()
def users_df():
    data = [("A", None, "a", "user_a"), ("B", None, "b", "user_b")]
    return pd.DataFrame(data, columns=["first_name", "last_name", "username", "final_username"], index=[111, 222])


@pytest.fixture()
def db(users_df):
    d = MagicMock()
    d.load_table.return_value = users_df
    return d


@pytest.fixture()
def command_logger():
    return MagicMock()


@pytest.fixture()
def job_persistance():
    return MagicMock()


@pytest.fixture()
def credits():
    c = MagicMock()
    c.update_credits.return_value = (2000, True)
    return c


@pytest.fixture()
def bot_state():
    bs = MagicMock()
    bs.city_quiz_cache = {}
    bs.flag_quiz_cache = {}
    bs.map_quiz_cache = {}
    bs.capital_quiz_cache = {}
    bs.top_countries_quiz_cache = {}
    bs.quiz_replay_cache = defaultdict(dict)
    return bs


@pytest.fixture()
def assets():
    a = MagicMock()
    mock_mystery = {
        "id": 1,
        "city": "Krakow",
        "city_ascii": "Krakow",
        "lat": 50.06,
        "lng": 19.94,
        "country": "Poland",
        "iso2": "PL",
        "continent": "Europa",
        "admin_name": "Malopolskie",
        "population": 780000,
        "difficulty": "easy",
    }
    mock_hints = [
        {
            "id": 2,
            "city": "Warsaw",
            "lng": 21.01,
            "lat": 52.23,
            "iso2": "PL",
            "population": 1800000,
            "capital": "primary",
            "_dist_km": 250.0,
            "_bearing": 15.0,
        },
        {
            "id": 3,
            "city": "Katowice",
            "lng": 19.02,
            "lat": 50.26,
            "iso2": "PL",
            "population": 300000,
            "capital": "admin",
            "_dist_km": 70.0,
            "_bearing": 290.0,
        },
        {
            "id": 4,
            "city": "Tarnow",
            "lng": 20.98,
            "lat": 50.01,
            "iso2": "PL",
            "population": 110000,
            "capital": "minor",
            "_dist_km": 80.0,
            "_bearing": 95.0,
        },
        {
            "id": 5,
            "city": "Rzeszow",
            "lng": 22.00,
            "lat": 50.04,
            "iso2": "PL",
            "population": 190000,
            "capital": "admin",
            "_dist_km": 150.0,
            "_bearing": 92.0,
        },
        {
            "id": 6,
            "city": "Kielce",
            "lng": 20.63,
            "lat": 50.87,
            "iso2": "PL",
            "population": 200000,
            "capital": "admin",
            "_dist_km": 115.0,
            "_bearing": 18.0,
        },
    ]
    a.cities.find_country_for_district.return_value = "PL"
    a.cities.get_districts_for_country.return_value = ["Malopolskie"]
    a.cities.get_available_difficulties.return_value = ["easy", "medium"]
    a.cities.sample_mystery_city.return_value = (mock_mystery, "easy")
    a.cities.get_hint_pool.return_value = pd.DataFrame(mock_hints)
    return a


@pytest.fixture()
def commands(command_logger, job_persistance, bot_state, credits, db, assets):
    return QuizCommands(command_logger, job_persistance, bot_state, credits, db, assets)


@pytest.fixture()
def update():
    u = MagicMock()
    u.effective_chat.id = 999
    u.effective_user.id = 111
    u.message.message_id = 123
    u.message.message_thread_id = 42
    u.message.text = ""
    u.message.reply_photo = AsyncMock()
    return u


@pytest.fixture()
def context():
    ctx = MagicMock()
    ctx.args = []
    ctx.bot = MagicMock()
    ctx.bot.send_message = AsyncMock()
    ctx.job_queue = MagicMock()
    job_mock = MagicMock()
    ctx.job_queue.run_once.return_value = job_mock
    return ctx


@pytest.mark.asyncio
async def test_cmd_guess_city_success_parameters(commands, update, context):
    sent_mock = MagicMock()
    sent_mock.message_id = 777
    update.message.reply_photo.return_value = sent_mock

    await commands.cmd_guess_city(update, context)

    update.message.reply_photo.assert_awaited_once()
    kwargs = update.message.reply_photo.call_args.kwargs
    assert kwargs["read_timeout"] == 60
    assert kwargs["write_timeout"] == 60
    assert kwargs["filename"] == "city_quiz.jpg"

    assert 111 in commands.bot_state.city_quiz_cache
    cached = commands.bot_state.city_quiz_cache[111]
    assert cached["message_id"] == 777
    assert cached["city"]["city"] == "Krakow"
    assert commands.bot_state.quiz_replay_cache[111][CityQuiz.QUIZ_TYPE] == context.args


@pytest.mark.asyncio
async def test_cmd_guess_city_blocked_when_active(commands, update, context):
    commands.bot_state.city_quiz_cache[111] = {"active": True}

    await commands.cmd_guess_city(update, context)

    update.message.reply_photo.assert_not_called()
    assert commands.bot_state.city_quiz_cache[111] == {"active": True}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "transient_error",
    [
        telegram.error.TimedOut("Timed out"),
        telegram.error.NetworkError("Network down"),
    ],
)
async def test_cmd_guess_city_timeout_raises_and_does_not_cache(commands, update, context, transient_error):
    update.message.reply_photo.side_effect = transient_error

    with pytest.raises(type(transient_error)):
        await commands.cmd_guess_city(update, context)

    assert 111 not in commands.bot_state.city_quiz_cache


@pytest.mark.asyncio
async def test_cmd_guess_city_no_cities_found(commands, update, context):
    commands.assets.cities.get_available_difficulties.return_value = []

    await commands.cmd_guess_city(update, context)

    update.message.reply_photo.assert_not_called()
    assert 111 not in commands.bot_state.city_quiz_cache
