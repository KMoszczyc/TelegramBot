"""Integration tests for the /guesstopcities command handler and quiz lifecycle."""

from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest

from src.commands.quiz_commands import QuizCommands
from src.config.enums import CreditActionType
from src.models.quizzes import top_cities_quiz as tcq
from src.models.quizzes.top_cities_quiz import Pool, PoolKind, TopCitiesGame


def make_cities_df() -> pd.DataFrame:
    cities = []
    # 15 cities for Poland (Europe)
    for i in range(1, 16):
        pop = 2_000_000 - i * 100_000
        cities.append(
            {
                "city": f"PolishCity{i}",
                "city_ascii": f"PolishCity{i}",
                "country": "Poland",
                "iso2": "PL",
                "admin_name": f"Voivodeship{i}",
                "population": pop,
                "continent": "Europa",
            }
        )
    # 15 cities for Brazil (South America)
    for i in range(1, 16):
        pop = 3_000_000 - i * 150_000
        cities.append(
            {
                "city": f"BrazilianCity{i}",
                "city_ascii": f"BrazilianCity{i}",
                "country": "Brazil",
                "iso2": "BR",
                "admin_name": f"State{i}",
                "population": pop,
                "continent": "Ameryka Południowa",
            }
        )
    return pd.DataFrame(cities)


def make_game() -> TopCitiesGame:
    return TopCitiesGame.create(make_cities_df(), Pool(PoolKind.COUNTRY, "Poland", 10))


def make_cached(game: TopCitiesGame | None = None, **overrides) -> dict:
    cached = {
        "chat_id": 999,
        "thread_id": 42,
        "message_id": 555,
        "has_photo": False,
        "game": game or make_game(),
        "job": MagicMock(),
    }
    cached.update(overrides)
    return cached


@pytest.fixture()
def users_df():
    data = [("A", None, "a", "user_a"), ("B", None, "b", "user_b")]
    return pd.DataFrame(data, columns=["first_name", "last_name", "username", "final_username"], index=[111, 222])


@pytest.fixture()
def db(users_df):
    mock_db = MagicMock()
    mock_db.load_table.return_value = users_df
    return mock_db


@pytest.fixture()
def credits():
    mock_credits = MagicMock()
    mock_credits.update_credits.return_value = (50000, True)
    return mock_credits


@pytest.fixture()
def bot_state():
    mock_state = MagicMock()
    mock_state.top_cities_quiz_cache = {}
    mock_state.quiz_replay_cache = {111: {}}
    mock_state.has_active_guess_quiz = lambda user_id: user_id in mock_state.top_cities_quiz_cache
    return mock_state


@pytest.fixture()
def assets():
    mock_assets = MagicMock()
    df = make_cities_df()
    mock_cities = MagicMock()
    mock_cities.df = df

    def mock_resolve(q):
        ql = q.strip().lower()
        if ql in ("poland", "pl"):
            return ("PL", "Poland")
        if ql in ("brazil", "br"):
            return ("BR", "Brazil")
        return None

    def mock_norm_cont(q):
        ql = q.strip().lower()
        if ql in ("europe", "europa"):
            return "Europa"
        if ql in ("south america", "ameryka południowa", "ameryka poludniowa"):
            return "Ameryka Południowa"
        return None

    mock_cities.resolve_country.side_effect = mock_resolve
    mock_cities._normalize_continent.side_effect = mock_norm_cont
    mock_assets.cities = mock_cities
    return mock_assets


@pytest.fixture()
def commands(bot_state, credits, db, assets):
    return QuizCommands(MagicMock(), MagicMock(), bot_state, credits, db, assets)


@pytest.fixture()
def update():
    u = MagicMock()
    u.effective_chat.id = 999
    u.effective_user.id = 111
    u.message.message_id = 123
    u.message.message_thread_id = 42
    u.message.text = ""
    u.message.reply_to_message = None
    u.message.reply_text = AsyncMock()
    u.message.reply_text.return_value.message_id = 555
    u.message.delete = AsyncMock()
    return u


@pytest.fixture()
def context():
    ctx = MagicMock()
    ctx.args = []
    ctx.bot = MagicMock()
    ctx.bot.send_message = AsyncMock()
    ctx.bot.edit_message_text = AsyncMock()
    ctx.bot.edit_message_reply_markup = AsyncMock()
    ctx.job_queue = MagicMock()
    ctx.job_queue.run_once.return_value = MagicMock()
    return ctx


def make_tip_update(user_id=111, message_id=555, data="topcitytip:111"):
    u = MagicMock()
    u.callback_query.data = data
    u.callback_query.from_user.id = user_id
    u.callback_query.message.message_id = message_id
    u.callback_query.answer = AsyncMock()
    return u


# ── Command Invocation & Validation Tests ─────────────────────────────────────


@pytest.mark.asyncio
async def test_cmd_blocked_by_active_top_cities_game(commands, update, context):
    commands.bot_state.top_cities_quiz_cache[111] = make_cached()
    await commands.cmd_guess_top_cities(update, context)

    update.message.reply_text.assert_not_called()
    assert context.bot.send_message.call_count == 1
    call_text = context.bot.send_message.call_args[1]["text"]
    assert "already have an active quiz" in call_text


@pytest.mark.asyncio
async def test_cmd_starts_game_without_args(commands, update, context):
    await commands.cmd_guess_top_cities(update, context)

    assert 111 in commands.bot_state.top_cities_quiz_cache
    cached = commands.bot_state.top_cities_quiz_cache[111]
    assert isinstance(cached["game"], TopCitiesGame)
    assert update.message.reply_text.call_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "args,expected_kind,expected_target",
    [
        (["--continent", "Europe"], PoolKind.CONTINENT, "Europa"),
        (["--country", "Poland"], PoolKind.COUNTRY, "Poland"),
        (["--letter", "P"], PoolKind.LETTER, "P"),
        (["--continent", "Europe", "--letter", "P"], PoolKind.CUSTOM, "Europa (P)"),
        (["--scope", "world"], PoolKind.WORLD, "World"),
    ],
)
async def test_cmd_starts_game_with_valid_args(commands, update, context, args, expected_kind, expected_target):
    context.args = args
    await commands.cmd_guess_top_cities(update, context)

    assert 111 in commands.bot_state.top_cities_quiz_cache
    game = commands.bot_state.top_cities_quiz_cache[111]["game"]
    assert game.pool.kind == expected_kind
    assert game.pool.target == expected_target


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "args,error_substring",
    [
        (["--letter", "AB"], "Invalid --letter"),
        (["--letter", "1"], "Invalid --letter"),
        (["--continent", "Atlantis"], "Invalid --continent"),
        (["--country", "Mordor"], "Unknown country"),
        (["--scope", "galaxy"], "Invalid --scope"),
    ],
)
async def test_cmd_invalid_args_returns_error(commands, update, context, args, error_substring):
    context.args = args
    await commands.cmd_guess_top_cities(update, context)

    assert 111 not in commands.bot_state.top_cities_quiz_cache
    assert context.bot.send_message.call_count == 1
    call_text = context.bot.send_message.call_args[1]["text"]
    assert error_substring in call_text


# ── Answering & Lifecycle Tests ───────────────────────────────────────────────


@pytest.mark.asyncio
async def test_answer_ignored_when_no_game(commands, update, context):
    update.message.text = "Warsaw"
    await commands.handle_top_cities_answer(update, context)
    context.bot.edit_message_text.assert_not_called()


@pytest.mark.asyncio
async def test_answer_ignored_in_other_thread(commands, update, context):
    commands.bot_state.top_cities_quiz_cache[111] = make_cached(thread_id=99)
    update.message.message_thread_id = 42
    update.message.text = "PolishCity1"
    await commands.handle_top_cities_answer(update, context)
    context.bot.edit_message_text.assert_not_called()


@pytest.mark.asyncio
async def test_correct_answer_updates_message(commands, update, context):
    cached = make_cached()
    commands.bot_state.top_cities_quiz_cache[111] = cached
    update.message.text = "PolishCity1"

    await commands.handle_top_cities_answer(update, context)

    assert 0 in cached["game"].guessed
    assert context.bot.edit_message_text.call_count == 1
    assert update.message.delete.call_count == 1


@pytest.mark.asyncio
async def test_wrong_answer_adds_strike(commands, update, context):
    cached = make_cached()
    commands.bot_state.top_cities_quiz_cache[111] = cached
    update.message.text = "WrongCity"

    await commands.handle_top_cities_answer(update, context)

    assert cached["game"].strikes == 1
    assert context.bot.edit_message_text.call_count == 1


@pytest.mark.asyncio
async def test_duplicate_answer_ignored(commands, update, context):
    cached = make_cached()
    cached["game"].submit_guess("PolishCity1")
    commands.bot_state.top_cities_quiz_cache[111] = cached
    update.message.text = "PolishCity1"

    await commands.handle_top_cities_answer(update, context)
    context.bot.edit_message_text.assert_not_called()


@pytest.mark.asyncio
async def test_third_strike_finishes_game(commands, update, context):
    cached = make_cached()
    cached["game"].strikes = 2
    commands.bot_state.top_cities_quiz_cache[111] = cached
    update.message.text = "WrongCity3"

    await commands.handle_top_cities_answer(update, context)

    assert 111 not in commands.bot_state.top_cities_quiz_cache
    assert cached["game"].is_over
    assert context.bot.edit_message_text.call_count == 1


@pytest.mark.asyncio
async def test_completing_list_pays_reward_and_replays(commands, update, context, credits):
    cached = make_cached()
    # Guess 9 cities
    for i in range(1, 10):
        cached["game"].submit_guess(f"PolishCity{i}")
    commands.bot_state.top_cities_quiz_cache[111] = cached

    update.message.text = "PolishCity10"
    await commands.handle_top_cities_answer(update, context)

    assert 111 not in commands.bot_state.top_cities_quiz_cache
    assert cached["game"].status == tcq.GameStatus.WON
    credits.update_credits.assert_called_once_with(user_id=111, credit_change=cached["game"].reward, action_type=CreditActionType.QUIZ)


# ── Tip & Timeout Tests ───────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_typed_tip_reveals_slot(commands, update, context):
    cached = make_cached()
    commands.bot_state.top_cities_quiz_cache[111] = cached
    update.message.text = "!tip"

    await commands.handle_top_cities_answer(update, context)

    assert cached["game"].tips_given == 1
    assert context.bot.edit_message_text.call_count == 1


@pytest.mark.asyncio
async def test_tip_button_reveals_slot_and_answers(commands, context):
    cached = make_cached()
    commands.bot_state.top_cities_quiz_cache[111] = cached
    tip_update = make_tip_update()

    await commands.btn_top_cities_tip_callback(tip_update, context)

    assert cached["game"].tips_given == 1
    tip_update.callback_query.answer.assert_called_once()
    assert context.bot.edit_message_text.call_count == 1


@pytest.mark.asyncio
async def test_tip_button_rejected_for_non_owner(commands, context):
    commands.bot_state.top_cities_quiz_cache[111] = make_cached()
    tip_update = make_tip_update(user_id=999)

    await commands.btn_top_cities_tip_callback(tip_update, context)

    tip_update.callback_query.answer.assert_called_once_with("This isn't your quiz!")


@pytest.mark.asyncio
async def test_timeout_pays_and_finishes_game(commands, context, credits):
    cached = make_cached()
    # Guess 6 cities (>= min_correct 5)
    for i in range(1, 7):
        cached["game"].submit_guess(f"PolishCity{i}")
    commands.bot_state.top_cities_quiz_cache[111] = cached

    context.job = MagicMock()
    context.job.data = 111

    await commands.top_cities_timeout(context)

    assert 111 not in commands.bot_state.top_cities_quiz_cache
    assert cached["game"].timed_out
    assert cached["game"].is_over
    credits.update_credits.assert_called_once()
