from collections import defaultdict
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.commands.quiz_commands import QuizCommands
from src.config.constants import WORDLE_SURRENDER_CALLBACK_PREFIX
from src.config.enums import CreditActionType
from src.models.quizzes.wordle_game import WordleGame


@pytest.fixture()
def bot_state():
    state = MagicMock()
    state.map_quiz_cache = {}
    state.flag_quiz_cache = {}
    state.capital_quiz_cache = {}
    state.top_countries_quiz_cache = {}
    state.top_cities_quiz_cache = {}
    state.city_quiz_cache = {}
    state.wordle_cache = {}
    state.quiz_replay_cache = defaultdict(dict)
    state.has_active_guess_quiz.side_effect = lambda uid: uid in state.wordle_cache
    return state


@pytest.fixture()
def credits():
    c = MagicMock()
    c.update_credits.return_value = (5000, 1000)
    return c


@pytest.fixture()
def assets():
    a = MagicMock()
    a.polish_stopwords = ["i", "w"]
    a.bible_df = None
    a.quran_df = None
    a.ozjasz_phrases = ["wolny rynek podatki"]
    a.bartosiak_phrases = ["lewar teatr wojny drabina"]
    a.europejskafirma_phrases = ["firma kapitał"]
    return a


@pytest.fixture()
def commands(bot_state, credits, assets):
    cmd = QuizCommands(MagicMock(), MagicMock(), bot_state, credits, MagicMock(), assets)
    cmd.users_df = MagicMock()
    return cmd


@pytest.fixture()
def update():
    u = MagicMock()
    u.effective_chat.id = 999
    u.effective_user.id = 111
    u.message.message_id = 123
    u.message.message_thread_id = 42
    u.message.text = ""
    u.message.reply_photo = AsyncMock()
    u.message.reply_photo.return_value.message_id = 555
    u.message.delete = AsyncMock()
    return u


@pytest.fixture()
def context():
    ctx = MagicMock()
    ctx.args = []
    ctx.bot = MagicMock()
    ctx.bot.send_message = AsyncMock()
    ctx.bot.edit_message_media = AsyncMock()
    ctx.job_queue = MagicMock()
    ctx.job_queue.run_once.return_value = MagicMock()
    return ctx


@pytest.mark.asyncio
async def test_cmd_wordle_success(commands, update, context, bot_state):
    await commands.cmd_wordle(update, context)

    assert 111 in bot_state.wordle_cache
    update.message.reply_photo.assert_awaited_once()
    context.job_queue.run_once.assert_called_once()
    cached = bot_state.wordle_cache[111]
    assert cached["chat_id"] == 999
    assert cached["message_id"] == 555
    assert cached["thread_id"] == 42
    # Default word length is 5
    assert cached["game"].word_length == 5


@pytest.mark.asyncio
async def test_cmd_wordle_with_args(commands, update, context, bot_state):
    context.args = ["bartosiak", "--length", "7"]
    await commands.cmd_wordle(update, context)

    cached = bot_state.wordle_cache[111]
    assert cached["game"].dataset_key == "bartosiak"
    assert cached["game"].word_length == 7


@pytest.mark.asyncio
async def test_cmd_wordle_with_name_arg(commands, update, context, bot_state):
    context.args = ["--name", "7", "bartosiak"]
    await commands.cmd_wordle(update, context)

    cached = bot_state.wordle_cache[111]
    assert cached["game"].dataset_key == "bartosiak"
    assert cached["game"].word_length == 7


@pytest.mark.asyncio
async def test_cmd_wordle_invalid_length(commands, update, context, bot_state):
    context.args = ["--length", "10"]
    await commands.cmd_wordle(update, context)

    assert 111 not in bot_state.wordle_cache
    update.message.reply_photo.assert_not_awaited()
    context.bot.send_message.assert_awaited_once()
    assert "between 4 and 8" in context.bot.send_message.call_args[1]["text"]


@pytest.mark.asyncio
async def test_cmd_wordle_blocked_if_active(commands, update, context, bot_state):
    bot_state.wordle_cache[111] = {"dummy": True}

    await commands.cmd_wordle(update, context)

    update.message.reply_photo.assert_not_awaited()


@pytest.mark.asyncio
async def test_handle_wordle_answer_wrong_length(commands, update, context, bot_state):
    game = WordleGame(target_word="świat", dataset_key="bible")
    bot_state.wordle_cache[111] = {
        "game": game,
        "chat_id": 999,
        "message_id": 555,
        "thread_id": 42,
        "job": MagicMock(),
    }
    update.message.text = "kot"  # 3 chars instead of 5

    await commands.handle_wordle_answer(update, context)

    update.message.delete.assert_not_awaited()
    context.bot.edit_message_media.assert_not_awaited()
    assert len(game.guesses) == 0


@pytest.mark.asyncio
async def test_handle_wordle_answer_progress(commands, update, context, bot_state):
    game = WordleGame(target_word="świat", dataset_key="bible")
    mock_job = MagicMock()
    bot_state.wordle_cache[111] = {
        "game": game,
        "chat_id": 999,
        "message_id": 555,
        "thread_id": 42,
        "job": mock_job,
    }
    update.message.text = "kwiat"

    await commands.handle_wordle_answer(update, context)

    update.message.delete.assert_awaited_once()
    mock_job.schedule_removal.assert_called_once()
    context.bot.edit_message_media.assert_awaited_once()
    assert len(game.guesses) == 1
    assert 111 in bot_state.wordle_cache


@pytest.mark.asyncio
async def test_handle_wordle_answer_win(commands, update, context, bot_state, credits):
    game = WordleGame(target_word="świat", dataset_key="bible")
    mock_job = MagicMock()
    bot_state.wordle_cache[111] = {
        "game": game,
        "chat_id": 999,
        "message_id": 555,
        "thread_id": 42,
        "job": mock_job,
    }
    update.message.text = "świat"

    await commands.handle_wordle_answer(update, context)

    assert 111 not in bot_state.wordle_cache
    credits.update_credits.assert_called_once()
    assert credits.update_credits.call_args[1]["action_type"] == CreditActionType.QUIZ
    context.bot.edit_message_media.assert_awaited_once()


@pytest.mark.asyncio
async def test_handle_wordle_answer_loss(commands, update, context, bot_state, credits):
    game = WordleGame(target_word="koty", dataset_key="bartosiak", max_tries=1)
    mock_job = MagicMock()
    bot_state.wordle_cache[111] = {
        "game": game,
        "chat_id": 999,
        "message_id": 555,
        "thread_id": 42,
        "job": mock_job,
    }
    update.message.text = "domy"

    await commands.handle_wordle_answer(update, context)

    assert 111 not in bot_state.wordle_cache
    credits.update_credits.assert_not_called()
    context.bot.edit_message_media.assert_awaited_once()


@pytest.mark.asyncio
async def test_btn_wordle_surrender_not_owner(commands, update, context, bot_state):
    query = MagicMock()
    query.data = f"{WORDLE_SURRENDER_CALLBACK_PREFIX}111"
    query.from_user.id = 222  # not the owner
    query.answer = AsyncMock()
    update.callback_query = query

    await commands.btn_wordle_surrender_callback(update, context)

    query.answer.assert_awaited_once_with("This is not your game!", show_alert=True)
    assert not query.edit_message_media.called


@pytest.mark.asyncio
async def test_btn_wordle_surrender_owner(commands, update, context, bot_state):
    game = WordleGame(target_word="świat", dataset_key="bible")
    mock_job = MagicMock()
    bot_state.wordle_cache[111] = {
        "game": game,
        "chat_id": 999,
        "message_id": 555,
        "job": mock_job,
    }

    query = MagicMock()
    query.data = f"{WORDLE_SURRENDER_CALLBACK_PREFIX}111"
    query.from_user.id = 111
    query.answer = AsyncMock()
    query.edit_message_media = AsyncMock()
    update.callback_query = query

    await commands.btn_wordle_surrender_callback(update, context)

    assert 111 not in bot_state.wordle_cache
    mock_job.schedule_removal.assert_called_once()
    assert game.is_over
    query.edit_message_media.assert_awaited_once()


@pytest.mark.asyncio
async def test_wordle_timeout(commands, context, bot_state):
    game = WordleGame(target_word="świat", dataset_key="bible")
    bot_state.wordle_cache[111] = {
        "game": game,
        "chat_id": 999,
        "message_id": 555,
    }
    mock_job = MagicMock()
    mock_job.data = {"user_id": 111, "chat_id": 999, "message_id": 555}
    context.job = mock_job

    await commands.wordle_timeout(context)

    assert 111 not in bot_state.wordle_cache
    assert game.is_over
    context.bot.edit_message_media.assert_awaited_once()


@pytest.mark.parametrize(
    ("quote", "word", "expected_contains"),
    [
        (
            "Na początku było słowo i słowo było u Boga.",
            "słowo",
            ["*słowo*", "Na początku było"],
        ),
        (
            "Drabina eskalacji jest straszna.",
            "drabina",
            ["*Drabina*", "eskalacji"],
        ),
        ("", "pies", []),
    ],
)
def test_build_quote_block(commands, quote: str, word: str, expected_contains: list[str]):
    game = WordleGame(target_word=word, dataset_key="bible", source_quote=quote)
    result = commands._build_quote_block(game)
    if not quote:
        assert result == ""
    else:
        for fragment in expected_contains:
            assert fragment in result
        assert result.startswith("\nFrom *Bible*:\n_")
