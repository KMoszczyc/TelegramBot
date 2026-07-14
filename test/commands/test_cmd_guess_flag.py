from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest

from src.commands.credit_commands import CreditCommands
from src.config.enums import CreditActionType


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
    bs.map_quiz_cache = {}
    bs.flag_quiz_cache = {}
    return bs


@pytest.fixture()
def assets(users_df):
    a = MagicMock()
    countries_df = pd.DataFrame(
        [
            {
                "country_name": "Polska",
                "country_code": "PL",
                "continent": "Europa",
                "population": 38000000,
                "capital": "Warszawa",
                "difficulty": "easy",
                "easiness_score": 1900.0,
            },
            {
                "country_name": "Niemcy",
                "country_code": "DE",
                "continent": "Europa",
                "population": 83000000,
                "capital": "Berlin",
                "difficulty": "medium",
                "easiness_score": 4150.0,
            },
            {
                "country_name": "Chiny",
                "country_code": "CN",
                "continent": "Azja",
                "population": 1400000000,
                "capital": "Pekin",
                "difficulty": "hard",
                "easiness_score": 14000.0,
            },
        ]
    )
    a.countries = MagicMock()
    a.countries.df = countries_df

    def mock_get_countries(difficulty=None, continent=None):
        df = countries_df
        if continent:
            df = df[df["continent"].str.contains(continent, case=False, na=False)]
        if difficulty:
            df = df[df["difficulty"] == difficulty.lower()]
        return df

    a.countries.get_countries.side_effect = mock_get_countries
    return a


@pytest.fixture()
def commands(command_logger, job_persistance, bot_state, credits, db, assets):
    return CreditCommands(command_logger, job_persistance, bot_state, credits, db, assets)


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
async def test_cmd_guess_flag_already_active(mocker, commands, update, context):
    commands.bot_state.flag_quiz_cache[111] = {"thread_id": 42}
    mock_send = mocker.patch("src.commands.credit_commands.core_utils.send_message", new_callable=AsyncMock)

    await commands.cmd_guess_flag(update, context)

    mock_send.assert_called_once()
    assert "already have an active quiz" in mock_send.call_args[0][3]


@pytest.mark.asyncio
async def test_cmd_guess_flag_invalid_difficulty(mocker, commands, update, context):
    context.args = ["--difficulty", "supercrazy"]
    mock_send = mocker.patch("src.commands.credit_commands.core_utils.send_message", new_callable=AsyncMock)

    await commands.cmd_guess_flag(update, context)

    mock_send.assert_called_once()
    assert "Invalid difficulty" in mock_send.call_args[0][3]


@pytest.mark.asyncio
async def test_cmd_guess_flag_valid(mocker, commands, update, context):
    context.args = ["--difficulty", "easy"]
    await commands.cmd_guess_flag(update, context)

    update.message.reply_photo.assert_called_once()
    assert 111 in commands.bot_state.flag_quiz_cache
    cached = commands.bot_state.flag_quiz_cache[111]
    assert cached["difficulty"] == "easy"
    assert cached["country"]["country_code"] == "PL"
    assert not cached["continent_specified"]
    context.job_queue.run_once.assert_called_once()


@pytest.mark.asyncio
async def test_cmd_guess_flag_with_continent(mocker, commands, update, context):
    context.args = ["--continent", "Europa", "--difficulty", "medium"]
    await commands.cmd_guess_flag(update, context)

    update.message.reply_photo.assert_called_once()
    assert 111 in commands.bot_state.flag_quiz_cache
    cached = commands.bot_state.flag_quiz_cache[111]
    assert cached["continent_specified"] is True
    assert cached["country"]["country_code"] == "DE"


@pytest.mark.asyncio
async def test_cmd_guess_flag_no_args(mocker, commands, update, context):
    context.args = []
    await commands.cmd_guess_flag(update, context)

    update.message.reply_photo.assert_called_once()
    assert 111 in commands.bot_state.flag_quiz_cache
    cached = commands.bot_state.flag_quiz_cache[111]
    assert cached["difficulty"] in ["easy", "medium", "hard"]
    assert not cached["continent_specified"]


@pytest.mark.asyncio
async def test_cmd_guess_flag_continent_only(mocker, commands, update, context):
    context.args = ["--continent", "Europa"]
    await commands.cmd_guess_flag(update, context)

    update.message.reply_photo.assert_called_once()
    assert 111 in commands.bot_state.flag_quiz_cache
    cached = commands.bot_state.flag_quiz_cache[111]
    assert cached["difficulty"] in ["easy", "medium", "hard"]
    assert cached["continent_specified"] is True


@pytest.mark.asyncio
async def test_handle_flag_quiz_answer_ignores_not_in_cache(commands, update, context):
    update.message.text = "Polska"
    await commands.handle_flag_quiz_answer(update, context)
    context.bot.send_message.assert_not_called()
    commands.credits.update_credits.assert_not_called()


@pytest.mark.asyncio
async def test_handle_flag_quiz_answer_wrong_thread(commands, update, context):
    commands.bot_state.flag_quiz_cache[111] = {"thread_id": 99, "country": {}, "job": MagicMock()}
    update.message.text = "Polska"
    await commands.handle_flag_quiz_answer(update, context)
    context.bot.send_message.assert_not_called()
    commands.credits.update_credits.assert_not_called()


@pytest.mark.asyncio
async def test_handle_flag_quiz_answer_tip_without_continent(commands, update, context):
    country = {"country_name": "Polska", "country_code": "PL", "continent": "Europa", "population": 38000000, "capital": "Warszawa"}
    commands.bot_state.flag_quiz_cache[111] = {
        "chat_id": 999,
        "thread_id": 42,
        "country": country,
        "difficulty": "easy",
        "continent_specified": False,
        "tips_given": 0,
        "job": MagicMock(),
    }
    update.message.text = "!tip"

    # Tip 1 (Continent)
    await commands.handle_flag_quiz_answer(update, context)
    assert commands.bot_state.flag_quiz_cache[111]["tips_given"] == 1
    text1 = context.bot.send_message.call_args[1]["text"]
    assert "Tip 1/3" in text1
    assert "Continent\\: Europa" in text1

    # Tip 2 (First letter)
    await commands.handle_flag_quiz_answer(update, context)
    assert commands.bot_state.flag_quiz_cache[111]["tips_given"] == 2
    text2 = context.bot.send_message.call_args[1]["text"]
    assert "Tip 2/3" in text2
    assert "First letter\\: P" in text2


@pytest.mark.asyncio
async def test_handle_flag_quiz_answer_tip_with_continent(commands, update, context):
    country = {"country_name": "Polska", "country_code": "PL", "continent": "Europa", "population": 38000000, "capital": "Warszawa"}
    commands.bot_state.flag_quiz_cache[111] = {
        "chat_id": 999,
        "thread_id": 42,
        "country": country,
        "difficulty": "easy",
        "continent_specified": True,
        "tips_given": 0,
        "job": MagicMock(),
    }
    update.message.text = "!tip"

    # Tip 1 (First letter - because continent skipped)
    await commands.handle_flag_quiz_answer(update, context)
    assert commands.bot_state.flag_quiz_cache[111]["tips_given"] == 1
    text1 = context.bot.send_message.call_args[1]["text"]
    assert "Tip 1/2" in text1
    assert "First letter\\: P" in text1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "user_answer, is_correct",
    [
        ("polska", True),
        ("PL", True),
        ("polsk", True),  # Fuzzy 1
        ("niemcy", False),
    ],
)
async def test_handle_flag_quiz_answer_logic(commands, update, context, user_answer, is_correct):
    job_mock = MagicMock()
    country = {"country_name": "Polska", "country_code": "PL", "continent": "Europa", "population": 38000000, "capital": "Warszawa"}
    commands.bot_state.flag_quiz_cache[111] = {
        "chat_id": 999,
        "thread_id": 42,
        "country": country,
        "difficulty": "easy",
        "continent_specified": False,
        "tips_given": 0,
        "job": job_mock,
    }
    update.message.text = user_answer

    await commands.handle_flag_quiz_answer(update, context)

    job_mock.schedule_removal.assert_called_once()
    assert 111 not in commands.bot_state.flag_quiz_cache

    if is_correct:
        commands.credits.update_credits.assert_called_once_with(user_id=111, credit_change=1000, action_type=CreditActionType.QUIZ)
        assert "Correct\\! The country is *Polska*" in context.bot.send_message.call_args[1]["text"]
    else:
        commands.credits.update_credits.assert_not_called()
        assert "Wrong\\! The correct answer was *Polska*" in context.bot.send_message.call_args[1]["text"]


@pytest.mark.asyncio
async def test_flag_quiz_timeout(commands, context):
    country = {"country_name": "Polska", "country_code": "PL", "continent": "Europa", "population": 38000000, "capital": "Warszawa"}
    commands.bot_state.flag_quiz_cache[111] = {
        "chat_id": 999,
        "thread_id": 42,
        "country": country,
        "difficulty": "easy",
        "continent_specified": False,
        "tips_given": 0,
        "job": MagicMock(),
    }
    context.job = MagicMock()
    context.job.data = 111

    await commands.flag_quiz_timeout(context)

    assert 111 not in commands.bot_state.flag_quiz_cache
    context.bot.send_message.assert_called_once()
    assert "Time's up\\! The country was *Polska*" in context.bot.send_message.call_args[1]["text"]
