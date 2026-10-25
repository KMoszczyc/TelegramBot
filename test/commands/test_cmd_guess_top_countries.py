from collections import defaultdict
from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest
import telegram

from src.commands.credit_commands import CreditCommands
from src.config.enums import CreditActionType
from src.models.quizzes import top_countries_quiz as tcq
from src.models.quizzes.top_countries_quiz import Metric, Pool, PoolKind, TopCountriesGame

NAMES = ["Alfa", "Bravo", "Charlie", "Delta", "Echo", "Foxtrot", "Golf", "Hotel", "India", "Juliet", "Kilo", "Lima"]


def make_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "country_name": f"{NAMES[i]}{i}",
                "country_name_en": f"{NAMES[i]}{i}",
                "country_code": f"x{i}",
                "continent": "Europa",
                "population": 1000 * (12 - i),
                "area_sq_km": 10 * (i + 1),
            }
            for i in range(12)
        ]
    )


def make_game() -> TopCountriesGame:
    return TopCountriesGame.create(make_df(), Metric.POPULATION, Pool(PoolKind.CONTINENT, "Europa", 10))


def make_cached(game: TopCountriesGame | None = None, **overrides) -> dict:
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
    d = MagicMock()
    d.load_table.return_value = users_df
    return d


@pytest.fixture()
def credits():
    c = MagicMock()
    c.update_credits.return_value = (4000, True)
    return c


@pytest.fixture()
def bot_state():
    bs = MagicMock()
    bs.map_quiz_cache = {}
    bs.flag_quiz_cache = {}
    bs.capital_quiz_cache = {}
    bs.top_countries_quiz_cache = {}
    bs.quiz_replay_cache = defaultdict(dict)
    return bs


@pytest.fixture()
def assets():
    a = MagicMock()
    a.countries.df = make_df()
    return a


@pytest.fixture()
def commands(bot_state, credits, db, assets):
    return CreditCommands(MagicMock(), MagicMock(), bot_state, credits, db, assets)


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


def make_tip_update(user_id=111, message_id=555, data="toptip:111"):
    u = MagicMock()
    u.callback_query.data = data
    u.callback_query.from_user.id = user_id
    u.callback_query.message.message_id = message_id
    u.callback_query.answer = AsyncMock()
    return u


def last_edit(context) -> dict:
    return context.bot.edit_message_text.call_args.kwargs


@pytest.mark.asyncio
async def test_cmd_blocked_by_active_top_countries_game(mocker, commands, update, context):
    commands.bot_state.top_countries_quiz_cache[111] = make_cached()
    mock_send = mocker.patch("src.commands.quiz_commands.core_utils.send_message", new_callable=AsyncMock)

    await commands.cmd_guess_top_countries(update, context)

    assert "already have an active quiz" in mock_send.call_args[0][3]
    update.message.reply_text.assert_not_called()


@pytest.mark.asyncio
async def test_other_quiz_blocked_by_active_top_countries_game(mocker, commands, update, context):
    commands.bot_state.top_countries_quiz_cache[111] = make_cached()
    mock_send = mocker.patch("src.commands.quiz_commands.core_utils.send_message", new_callable=AsyncMock)

    await commands.cmd_guess_capital(update, context)

    assert "already have an active quiz" in mock_send.call_args[0][3]


@pytest.mark.asyncio
async def test_cmd_invalid_by_value(mocker, commands, update, context):
    context.args = ["--by", "gdp"]
    mock_send = mocker.patch("src.commands.quiz_commands.core_utils.send_message", new_callable=AsyncMock)

    await commands.cmd_guess_top_countries(update, context)

    assert "Invalid --by value" in mock_send.call_args[0][3]
    assert 111 not in commands.bot_state.top_countries_quiz_cache


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "args, expected_phrase",
    [(["--by", "population"], "most populated countries"), (["--by", "area"], "largest countries")],
)
async def test_cmd_starts_game_with_metric(commands, update, context, args, expected_phrase):
    context.args = args

    await commands.cmd_guess_top_countries(update, context)

    kwargs = update.message.reply_text.call_args.kwargs
    assert f"Guess Top 10 {expected_phrase} in *Europa*" in kwargs["text"]
    assert kwargs["parse_mode"] == "MarkdownV2"
    assert kwargs["reply_markup"].inline_keyboard[0][0].callback_data == "toptip:111"
    cached = commands.bot_state.top_countries_quiz_cache[111]
    assert cached["message_id"] == 555
    assert cached["thread_id"] == 42
    assert cached["chat_id"] == 999
    assert isinstance(cached["game"], TopCountriesGame)
    context.job_queue.run_once.assert_called_once()
    assert commands.bot_state.quiz_replay_cache[111]["topcountries"] == args


@pytest.mark.asyncio
async def test_cmd_without_args_picks_random_metric(commands, update, context):
    await commands.cmd_guess_top_countries(update, context)

    game = commands.bot_state.top_countries_quiz_cache[111]["game"]
    assert game.metric in list(Metric)
    assert game.list_size == 10


@pytest.mark.asyncio
async def test_answer_ignored_when_no_game(commands, update, context):
    update.message.text = "Alfa0"
    await commands.handle_top_countries_answer(update, context)
    context.bot.edit_message_text.assert_not_called()


@pytest.mark.asyncio
async def test_answer_ignored_in_other_thread(commands, update, context):
    cached = make_cached(thread_id=99)
    commands.bot_state.top_countries_quiz_cache[111] = cached
    update.message.text = "Alfa0"

    await commands.handle_top_countries_answer(update, context)

    context.bot.edit_message_text.assert_not_called()
    assert cached["game"].correct_count == 0


@pytest.mark.asyncio
async def test_correct_answer_edits_single_message_and_resets_timeout(commands, update, context):
    cached = make_cached()
    old_job = cached["job"]
    commands.bot_state.top_countries_quiz_cache[111] = cached
    update.message.text = "Echo4"

    await commands.handle_top_countries_answer(update, context)

    assert "Echo4" in last_edit(context)["text"]
    assert last_edit(context)["message_id"] == 555
    assert last_edit(context)["parse_mode"] == "MarkdownV2"
    context.bot.send_message.assert_not_called()
    old_job.schedule_removal.assert_called_once()
    context.job_queue.run_once.assert_called_once()
    assert cached["job"] is context.job_queue.run_once.return_value
    assert cached["game"].guessed == {4}


@pytest.mark.asyncio
async def test_wrong_answer_adds_strike(commands, update, context):
    cached = make_cached()
    commands.bot_state.top_countries_quiz_cache[111] = cached
    update.message.text = "Kilo10"

    await commands.handle_top_countries_answer(update, context)

    assert cached["game"].strikes == 1
    assert "❌✖️✖️" in last_edit(context)["text"]


@pytest.mark.asyncio
async def test_duplicate_answer_is_ignored(commands, update, context):
    game = make_game()
    game.submit_guess("Echo4")
    commands.bot_state.top_countries_quiz_cache[111] = make_cached(game)
    update.message.text = "Echo4"

    await commands.handle_top_countries_answer(update, context)

    context.bot.edit_message_text.assert_not_called()
    assert game.strikes == 0


@pytest.mark.asyncio
async def test_third_strike_ends_game_without_reward(commands, update, context):
    game = make_game()
    game.strikes = 2
    cached = make_cached(game)
    commands.bot_state.top_countries_quiz_cache[111] = cached
    update.message.text = "Kilo10"

    await commands.handle_top_countries_answer(update, context)

    assert 111 not in commands.bot_state.top_countries_quiz_cache
    commands.credits.update_credits.assert_not_called()
    cached["job"].schedule_removal.assert_called_once()
    kwargs = last_edit(context)
    assert "3 strikes" in kwargs["text"]
    assert "No reward" in kwargs["text"]
    assert kwargs["reply_markup"].inline_keyboard[0][0].callback_data == "quizreplay:topcountries:111"


@pytest.mark.asyncio
async def test_completing_the_list_pays_full_reward_and_offers_replay(commands, update, context):
    game = make_game()
    for entry in game.entries[:9]:
        game.submit_guess(entry.name)
    commands.bot_state.top_countries_quiz_cache[111] = make_cached(game)
    update.message.text = game.entries[9].name

    await commands.handle_top_countries_answer(update, context)

    commands.credits.update_credits.assert_called_once_with(user_id=111, credit_change=75000, action_type=CreditActionType.QUIZ)
    kwargs = last_edit(context)
    assert "Perfect" in kwargs["text"]
    assert "4000" in kwargs["text"]
    assert kwargs["reply_markup"].inline_keyboard[0][0].callback_data == "quizreplay:topcountries:111"
    assert 111 not in commands.bot_state.top_countries_quiz_cache


@pytest.mark.asyncio
async def test_reply_answer_is_deleted(commands, update, context):
    commands.bot_state.top_countries_quiz_cache[111] = make_cached()
    update.message.text = "Echo4"
    update.message.reply_to_message = MagicMock(message_id=555)

    await commands.handle_top_countries_answer(update, context)

    update.message.delete.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("reply_id", [None, 777])
async def test_non_reply_answer_is_not_deleted(commands, update, context, reply_id):
    commands.bot_state.top_countries_quiz_cache[111] = make_cached()
    update.message.text = "Echo4"
    if reply_id is not None:
        update.message.reply_to_message = MagicMock(message_id=reply_id)

    await commands.handle_top_countries_answer(update, context)

    update.message.delete.assert_not_called()
    assert last_edit(context)["message_id"] == 555


@pytest.mark.asyncio
async def test_delete_failure_is_swallowed(commands, update, context):
    cached = make_cached()
    commands.bot_state.top_countries_quiz_cache[111] = cached
    update.message.text = "Echo4"
    update.message.reply_to_message = MagicMock(message_id=555)
    update.message.delete.side_effect = telegram.error.BadRequest("can't delete")

    await commands.handle_top_countries_answer(update, context)

    assert cached["game"].guessed == {4}
    context.bot.edit_message_text.assert_called_once()


@pytest.mark.asyncio
async def test_typed_tip_reveals_letter_in_place(commands, update, context):
    cached = make_cached()
    commands.bot_state.top_countries_quiz_cache[111] = cached
    update.message.text = "!tip"

    await commands.handle_top_countries_answer(update, context)

    assert cached["game"].tips_given == 1
    assert "Tips\\: 1/5" in last_edit(context)["text"]
    assert cached["game"].strikes == 0


@pytest.mark.asyncio
async def test_tip_button_reveals_letter_and_answers_callback(commands, context):
    cached = make_cached()
    commands.bot_state.top_countries_quiz_cache[111] = cached
    tip_update = make_tip_update()

    await commands.btn_top_countries_tip_callback(tip_update, context)

    tip_update.callback_query.answer.assert_called_once_with()
    assert cached["game"].tips_given == 1
    assert "Tip 2/5" in last_edit(context)["reply_markup"].inline_keyboard[0][0].text


@pytest.mark.asyncio
async def test_tip_button_removed_after_last_tip(commands, context):
    cached = make_cached()
    commands.bot_state.top_countries_quiz_cache[111] = cached

    for _ in range(5):
        await commands.btn_top_countries_tip_callback(make_tip_update(), context)

    assert cached["game"].tips_given == 5
    assert last_edit(context)["reply_markup"].inline_keyboard == ()


@pytest.mark.asyncio
async def test_tip_button_no_more_tips(commands, context):
    game = make_game()
    for _ in range(5):
        game.reveal_tip()
    commands.bot_state.top_countries_quiz_cache[111] = make_cached(game)
    tip_update = make_tip_update()

    await commands.btn_top_countries_tip_callback(tip_update, context)

    tip_update.callback_query.answer.assert_called_once_with("No more tips available!")
    context.bot.edit_message_text.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "caller_id, message_id, data, has_game, expected_answer",
    [
        (222, 555, "toptip:111", True, "This isn't your quiz!"),
        (111, 555, "garbage", True, "This isn't your quiz!"),
        (111, 555, "toptip:111", False, "This quiz is already over."),
        (111, 999, "toptip:111", True, "This quiz is already over."),
    ],
)
async def test_tip_button_rejected(commands, context, caller_id, message_id, data, has_game, expected_answer):
    cached = make_cached()
    if has_game:
        commands.bot_state.top_countries_quiz_cache[111] = cached
    tip_update = make_tip_update(user_id=caller_id, message_id=message_id, data=data)

    await commands.btn_top_countries_tip_callback(tip_update, context)

    tip_update.callback_query.answer.assert_called_once_with(expected_answer)
    context.bot.edit_message_text.assert_not_called()
    assert cached["game"].tips_given == 0


@pytest.mark.asyncio
async def test_timeout_pays_partial_reward_and_reveals_list(commands, context):
    game = make_game()
    for entry in game.entries[:6]:
        game.submit_guess(entry.name)
    commands.bot_state.top_countries_quiz_cache[111] = make_cached(game)
    context.job.data = 111

    await commands.top_countries_timeout(context)

    commands.credits.update_credits.assert_called_once_with(
        user_id=111, credit_change=tcq.calculate_reward(10, 6, 0, 0), action_type=CreditActionType.QUIZ
    )
    kwargs = last_edit(context)
    assert "Time's up" in kwargs["text"]
    assert "❌" in kwargs["text"]
    assert kwargs["reply_markup"].inline_keyboard[0][0].callback_data == "quizreplay:topcountries:111"
    assert 111 not in commands.bot_state.top_countries_quiz_cache


@pytest.mark.asyncio
async def test_timeout_below_minimum_pays_nothing(commands, context):
    commands.bot_state.top_countries_quiz_cache[111] = make_cached()
    context.job.data = 111

    await commands.top_countries_timeout(context)

    commands.credits.update_credits.assert_not_called()
    assert "No reward" in last_edit(context)["text"]


@pytest.mark.asyncio
async def test_timeout_when_job_lookup_error_raised(commands, context):
    cached = make_cached()
    cached["job"].schedule_removal.side_effect = Exception("No job found")
    commands.bot_state.top_countries_quiz_cache[111] = cached
    context.job.data = 111

    await commands.top_countries_timeout(context)

    assert 111 not in commands.bot_state.top_countries_quiz_cache
    kwargs = last_edit(context)
    assert "Time's up" in kwargs["text"]
    assert "❌" in kwargs["text"]


@pytest.mark.asyncio
async def test_timeout_after_game_already_finished_is_noop(commands, context):
    context.job.data = 111

    await commands.top_countries_timeout(context)

    context.bot.edit_message_text.assert_not_called()
    commands.credits.update_credits.assert_not_called()


@pytest.mark.asyncio
async def test_replay_button_starts_new_game(mocker, commands, context):
    mock_cmd = mocker.patch.object(commands, "cmd_guess_top_countries", new_callable=AsyncMock)
    commands.bot_state.quiz_replay_cache[111]["topcountries"] = ["--by", "area"]
    replay_update = MagicMock()
    replay_update.callback_query.data = "quizreplay:topcountries:111"
    replay_update.callback_query.from_user.id = 111
    replay_update.callback_query.answer = AsyncMock()
    replay_update.callback_query.edit_message_reply_markup = AsyncMock()

    await commands.btn_quiz_replay_callback(replay_update, context)

    mock_cmd.assert_called_once()
    assert context.args == ["--by", "area"]


@pytest.mark.asyncio
async def test_reply_answer_accepted_even_if_thread_id_differs(commands, update, context):
    cached = make_cached(thread_id=99)
    commands.bot_state.top_countries_quiz_cache[111] = cached
    update.message.text = "Echo4"
    update.message.message_thread_id = 42
    update.message.reply_to_message = MagicMock(message_id=555)

    await commands.handle_top_countries_answer(update, context)

    context.bot.edit_message_text.assert_called_once()
    assert cached["game"].guessed == {4}


@pytest.mark.asyncio
@pytest.mark.parametrize("cached_thread,msg_thread", [(None, 42), (42, None), (None, None)])
async def test_answer_accepted_when_thread_id_is_none(commands, update, context, cached_thread, msg_thread):
    cached = make_cached(thread_id=cached_thread)
    commands.bot_state.top_countries_quiz_cache[111] = cached
    update.message.text = "Echo4"
    update.message.message_thread_id = msg_thread
    update.message.reply_to_message = None

    await commands.handle_top_countries_answer(update, context)

    context.bot.edit_message_text.assert_called_once()
    assert cached["game"].guessed == {4}


@pytest.mark.asyncio
async def test_answer_ignored_in_different_chat(commands, update, context):
    cached = make_cached(chat_id=999)
    commands.bot_state.top_countries_quiz_cache[111] = cached
    update.effective_chat.id = 888
    update.message.text = "Echo4"

    await commands.handle_top_countries_answer(update, context)

    context.bot.edit_message_text.assert_not_called()
    assert cached["game"].correct_count == 0


@pytest.mark.asyncio
async def test_update_quiz_message_retries_without_markdown_on_bad_request(context):
    from src.commands.helpers.quiz_helpers import update_quiz_message

    cached_quiz = {"chat_id": 999, "message_id": 555, "has_photo": False}
    context.bot.edit_message_text.side_effect = [telegram.error.BadRequest("Can't parse entities"), None]

    await update_quiz_message(context, cached_quiz, "*test text*", None)

    assert context.bot.edit_message_text.call_count == 2
    # Second call should have parse_mode = None
    assert context.bot.edit_message_text.call_args_list[1].kwargs["parse_mode"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "lang_args, expected_lang",
    [
        (["--language", "pl"], tcq.Language.PL),
        (["--language", "polish"], tcq.Language.PL),
        (["--language", "en"], tcq.Language.EN),
        (["--lang", "english"], tcq.Language.EN),
    ],
)
async def test_cmd_starts_game_with_language(commands, update, context, lang_args, expected_lang):
    context.args = lang_args

    await commands.cmd_guess_top_countries(update, context)

    game = commands.bot_state.top_countries_quiz_cache[111]["game"]
    assert game.language == expected_lang


@pytest.mark.asyncio
async def test_cmd_defaults_to_english_language(commands, update, context):
    context.args = []

    await commands.cmd_guess_top_countries(update, context)

    game = commands.bot_state.top_countries_quiz_cache[111]["game"]
    assert game.language == tcq.Language.EN


@pytest.mark.asyncio
async def test_cmd_invalid_language_returns_error(commands, update, context):
    context.args = ["--language", "german"]

    await commands.cmd_guess_top_countries(update, context)

    context.bot.send_message.assert_called_once()
    assert "Invalid --language value" in context.bot.send_message.call_args.kwargs["text"]
    assert 111 not in commands.bot_state.top_countries_quiz_cache
