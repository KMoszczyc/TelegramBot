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
    ("quote", "word", "attr", "expected_contains", "expected_header"),
    [
        (
            "Na początku było słowo i słowo było u Boga.",
            "słowo",
            "Biblia (J 1, 1)",
            ["*słowo*", "Na początku było"],
            "\n> From *Biblia \\(J 1, 1\\)*:\n> _",
        ),
        (
            "Drabina eskalacji jest straszna.",
            "drabina",
            "Bartosiak",
            ["*Drabina*", "eskalacji"],
            "\n> From *Bartosiak*:\n> _",
        ),
        (
            "Przedsiębiorca zarabia kapitał.",
            "kapitał",
            "Jerzy Krajewski",
            ["*kapitał*"],
            "\n> From *Jerzy Krajewski*:\n> _",
        ),
        ("", "pies", "Biblia", [], ""),
    ],
)
def test_build_quote_block(commands, quote: str, word: str, attr: str, expected_contains: list[str], expected_header: str):
    game = WordleGame(target_word=word, dataset_key="bible", source_quote=quote, source_attribution=attr)
    result = commands._build_quote_block(game)
    if not quote:
        assert result == ""
    else:
        for fragment in expected_contains:
            assert fragment in result
        assert result.startswith(expected_header)


def test_build_wordle_keyboard_states(commands):
    user_id = 12345
    game = WordleGame(target_word="robot", dataset_key="bible", source_quote="sample quote with robot in it")
    # Initial state (guesses = 0, max_tries = 6) -> midpoint is 3, unlock at guess 4
    kb0 = commands._build_wordle_keyboard(user_id, game)
    assert len(kb0.inline_keyboard) == 1
    assert len(kb0.inline_keyboard[0]) == 2
    assert "🔒 Tip (guess 4)" in kb0.inline_keyboard[0][0].text
    assert "🏳️ Surrender" in kb0.inline_keyboard[0][1].text

    # After 3 guesses -> Tier 1 unlocked
    for _ in range(3):
        game.submit_guess("tests")
    kb1 = commands._build_wordle_keyboard(user_id, game)
    assert kb1.inline_keyboard[0][0].text == "💡 Tip"

    # Reveal tier 1 -> Tier 2 is locked until last guess
    game.reveal_tip()
    kb2 = commands._build_wordle_keyboard(user_id, game)
    assert "🔒 Final Tip (last guess)" in kb2.inline_keyboard[0][0].text

    # 4th guess (guesses = 4, remaining_tries = 2) -> still locked
    game.submit_guess("tests")
    kb3 = commands._build_wordle_keyboard(user_id, game)
    assert "🔒 Final Tip (last guess)" in kb3.inline_keyboard[0][0].text

    # 5th guess (guesses = 5, remaining_tries = 1) -> Final Tip unlocked!
    game.submit_guess("tests")
    kb4 = commands._build_wordle_keyboard(user_id, game)
    assert kb4.inline_keyboard[0][0].text == "💡 Final Tip"

    # Reveal final tip -> tip button disappears
    game.reveal_tip()
    kb5 = commands._build_wordle_keyboard(user_id, game)
    assert len(kb5.inline_keyboard[0]) == 1
    assert "🏳️ Surrender" in kb5.inline_keyboard[0][0].text


def test_build_tip_block(commands):
    game = WordleGame(target_word="robot", dataset_key="bible", source_quote="sample quote with robot in it")
    assert commands._build_tip_block(game) == ""

    game.revealed_tip_tier = 1
    block1 = commands._build_tip_block(game)
    assert "💡 *Tip:*" in block1
    assert "\\[\\_\\_\\_\\_\\_\\]" in block1

    game.revealed_tip_tier = 2
    block2 = commands._build_tip_block(game)
    assert "💡 *Final Tip:*" in block2
    assert "\\[\\_\\_\\_\\_\\_\\]" in block2


@pytest.mark.asyncio
async def test_btn_wordle_tip_callback_unauthorized(commands):
    update = AsyncMock()
    context = AsyncMock()
    query = AsyncMock()
    query.data = "wordletip:12345"
    query.from_user.id = 99999  # unauthorized user
    update.callback_query = query

    await commands.btn_wordle_tip_callback(update, context)
    query.answer.assert_called_once_with("This is not your Wordle game!", show_alert=True)


@pytest.mark.asyncio
async def test_btn_wordle_tip_callback_locked_toast(commands):
    user_id = 12345
    game = WordleGame(target_word="robot", dataset_key="bible", source_quote="sample quote with robot in it")
    commands.bot_state.wordle_cache[user_id] = {
        "game": game,
        "chat_id": 100,
        "message_id": 200,
    }

    update = AsyncMock()
    context = AsyncMock()
    query = AsyncMock()
    query.data = f"wordletip:{user_id}"
    query.from_user.id = user_id
    update.callback_query = query

    # Game has 0 guesses -> locked
    await commands.btn_wordle_tip_callback(update, context)
    query.answer.assert_called_once_with("🔒 Tip unlocks after guess 3!", show_alert=False)
    query.edit_message_caption.assert_not_called()


@pytest.mark.asyncio
async def test_btn_wordle_tip_callback_unlock_and_reveal(commands):
    user_id = 12345
    game = WordleGame(target_word="robot", dataset_key="bible", source_quote="sample quote with robot in it")
    for _ in range(3):
        game.submit_guess("tests")

    commands.bot_state.wordle_cache[user_id] = {
        "game": game,
        "chat_id": 100,
        "message_id": 200,
    }

    update = AsyncMock()
    context = AsyncMock()
    query = AsyncMock()
    query.data = f"wordletip:{user_id}"
    query.from_user.id = user_id
    update.callback_query = query

    await commands.btn_wordle_tip_callback(update, context)
    assert game.revealed_tip_tier == 1
    query.answer.assert_called_once_with("💡 Tip revealed!", show_alert=False)
    query.edit_message_caption.assert_called_once()
