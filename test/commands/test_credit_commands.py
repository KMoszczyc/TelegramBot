from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest

from src.commands.credit_commands import CreditCommands


@pytest.fixture()
def users_df():
    data = [("A", None, "a", "user_a"), ("B", None, "b", "user_b")]
    return pd.DataFrame(data, columns=["first_name", "last_name", "username", "final_username"], index=[111, 222])


@pytest.fixture()
def db():
    return MagicMock()


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
    return bs


@pytest.fixture()
def assets():
    a = MagicMock()
    a.famous_people_trivia_df = pd.DataFrame([{"name_pl": "Jan Kowalski", "description": "some desc"}])
    return a


@pytest.fixture()
def commands(command_logger, job_persistance, bot_state, credits, db, assets):
    return CreditCommands(command_logger, job_persistance, bot_state, credits, db, assets)


@pytest.fixture()
def update():
    u = MagicMock()
    u.effective_chat.id = 999
    u.effective_user.id = 111
    u.message.message_thread_id = 42
    u.message.text = ""
    return u


@pytest.fixture()
def context():
    ctx = MagicMock()
    ctx.args = []
    ctx.bot = MagicMock()
    ctx.bot.send_message = AsyncMock()
    ctx.job_queue = MagicMock()
    return ctx


@pytest.mark.asyncio
async def test_handle_map_quiz_answer_ignores_user_not_in_cache(commands, update, context):
    await commands.handle_map_quiz_answer(update, context)
    commands.credits.update_credits.assert_not_called()


@pytest.mark.asyncio
async def test_handle_map_quiz_answer_ignores_wrong_thread(commands, update, context):
    commands.bot_state.map_quiz_cache[111] = {"thread_id": 99, "person": {}, "job": MagicMock()}
    await commands.handle_map_quiz_answer(update, context)
    commands.credits.update_credits.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("user_answer,is_correct", [("jan kowalski", True), ("kowalski", True), ("kowal", False), ("jan", True)])
async def test_handle_map_quiz_answer_logic(mocker, commands, update, context, user_answer, is_correct):
    job_mock = MagicMock()
    person = {"name_pl": "Jan Kowalski", "description": "A brave man."}
    commands.bot_state.map_quiz_cache[111] = {"thread_id": 42, "person": person, "job": job_mock}
    update.message.text = user_answer

    mocker.patch("src.commands.credit_commands.core_utils.send_message", new_callable=AsyncMock)

    await commands.handle_map_quiz_answer(update, context)

    job_mock.schedule_removal.assert_called_once()
    assert 111 not in commands.bot_state.map_quiz_cache

    if is_correct:
        commands.credits.update_credits.assert_called_once()
    else:
        commands.credits.update_credits.assert_not_called()


@pytest.mark.asyncio
async def test_cmd_tournament_missing_buyin_arg(mocker, commands, update, context):
    context.args = ["roulette"]
    send_mock = mocker.patch("src.commands.credit_commands.core_utils.send_message", new_callable=AsyncMock)

    await commands.cmd_tournament(update, context)

    send_mock.assert_called_once()
    message = send_mock.call_args[0][3]
    assert "Missing required argument" in message


@pytest.mark.asyncio
async def test_cmd_tournament_valid_args(mocker, commands, update, context):
    context.args = ["roulette", "100"]
    commands.bot_state.is_tournament_banned.return_value = False
    commands.credits.credits = {111: 500}
    send_mock = mocker.patch("src.commands.credit_commands.core_utils.send_message", new_callable=AsyncMock)

    await commands.cmd_tournament(update, context)

    send_mock.assert_called_once()
    assert 999 in commands.active_tournaments
