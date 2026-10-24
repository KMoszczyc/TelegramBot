from collections import defaultdict
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
    bs.quiz_cache = {}
    bs.map_quiz_cache = {}
    bs.flag_quiz_cache = {}
    bs.capital_quiz_cache = {}
    bs.quiz_replay_cache = defaultdict(dict)
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
    u.message.reply_photo = AsyncMock()
    u.message.reply_text = AsyncMock()
    return u


@pytest.fixture()
def context():
    ctx = MagicMock()
    ctx.args = []
    ctx.bot = MagicMock()
    ctx.bot.send_message = AsyncMock()
    ctx.bot.edit_message_text = AsyncMock()
    ctx.bot.edit_message_caption = AsyncMock()
    ctx.bot.edit_message_reply_markup = AsyncMock()
    ctx.job_queue = MagicMock()
    job_mock = MagicMock()
    ctx.job_queue.run_once.return_value = job_mock
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
async def test_cmd_guess_person_on_a_map_valid(mocker, commands, update, context):
    person = {
        "name_pl": "Maria Skłodowska",
        "category": "nauka",
        "description": "Maria Skłodowska - fizyk. Noblista.",
        "latitude": 52.0,
        "longitude": 21.0,
    }
    mocker.patch("src.models.quizzes.map_quiz.MapQuiz.guess_random_person_on_map", return_value=("fake_map.jpg", person))
    update.message.reply_photo.return_value.message_id = 777

    await commands.cmd_guess_person_on_a_map(update, context)

    update.message.reply_photo.assert_called_once()
    kwargs = update.message.reply_photo.call_args.kwargs
    assert kwargs["photo"] == "fake_map.jpg"
    assert kwargs["parse_mode"] == "MarkdownV2"
    button = kwargs["reply_markup"].inline_keyboard[0][0]
    assert button.callback_data == "quiztip:111"
    assert "Tip 1/" in button.text

    assert 111 in commands.bot_state.map_quiz_cache
    cached = commands.bot_state.map_quiz_cache[111]
    assert cached["message_id"] == 777
    assert cached["has_photo"] is True
    assert cached["person"] == person
    assert cached["tips_given"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("cache_name", ["map_quiz_cache", "flag_quiz_cache", "capital_quiz_cache"])
async def test_cmd_guess_person_blocked_by_active_quizzes(mocker, commands, update, context, cache_name):
    getattr(commands.bot_state, cache_name)[111] = {"thread_id": 42}
    mock_send = mocker.patch("src.commands.credit_commands.core_utils.send_message", new_callable=AsyncMock)

    await commands.cmd_guess_person_on_a_map(update, context)

    mock_send.assert_called_once()
    assert "already have an active quiz" in mock_send.call_args[0][3]


@pytest.mark.asyncio
async def test_map_quiz_tip_button_edits_caption_in_place(commands, context):
    person = {"name_pl": "Jan Kowalski", "description": "Jan Kowalski - fizyk. Noblista z Polski."}
    commands.bot_state.map_quiz_cache[111] = {
        "chat_id": 999,
        "thread_id": 42,
        "person": person,
        "difficulty": "easy",
        "category_specified": False,
        "continent_specified": False,
        "tips_given": 0,
        "tip_lines": [],
        "message_id": 777,
        "has_photo": True,
        "base_text": "Difficulty: Easy",
        "job": MagicMock(),
    }
    tip_update = MagicMock()
    tip_update.callback_query.data = "quiztip:111"
    tip_update.callback_query.from_user.id = 111
    tip_update.callback_query.message.message_id = 777
    tip_update.callback_query.answer = AsyncMock()

    await commands.btn_quiz_tip_callback(tip_update, context)

    tip_update.callback_query.answer.assert_called_once_with()
    context.bot.edit_message_caption.assert_called_once()
    caption = context.bot.edit_message_caption.call_args.kwargs["caption"]
    assert "Tip 1/" in caption
    assert commands.bot_state.map_quiz_cache[111]["tips_given"] == 1


@pytest.mark.asyncio
async def test_map_quiz_tip_button_falls_back_to_reply_when_caption_too_long(commands, context):
    person = {"name_pl": "Jan Kowalski", "description": "Jan Kowalski - fizyk. Noblista z Polski."}
    commands.bot_state.map_quiz_cache[111] = {
        "chat_id": 999,
        "thread_id": 42,
        "person": person,
        "difficulty": "easy",
        "category_specified": False,
        "continent_specified": False,
        "tips_given": 0,
        "tip_lines": [],
        "message_id": 777,
        "has_photo": True,
        "base_text": "x" * 1020,
        "job": MagicMock(),
    }
    tip_update = MagicMock()
    tip_update.callback_query.data = "quiztip:111"
    tip_update.callback_query.from_user.id = 111
    tip_update.callback_query.message.message_id = 777
    tip_update.callback_query.answer = AsyncMock()

    await commands.btn_quiz_tip_callback(tip_update, context)

    context.bot.edit_message_caption.assert_not_called()
    context.bot.send_message.assert_called_once()
    assert context.bot.send_message.call_args.kwargs["reply_to_message_id"] == 777


@pytest.mark.asyncio
async def test_handle_map_quiz_answer_removes_tip_button(commands, update, context):
    person = {"name_pl": "Jan Kowalski", "description": "Jan Kowalski - fizyk."}
    commands.bot_state.map_quiz_cache[111] = {
        "chat_id": 999,
        "thread_id": 42,
        "person": person,
        "message_id": 777,
        "job": MagicMock(),
    }
    update.message.text = "jan kowalski"

    await commands.handle_map_quiz_answer(update, context)

    context.bot.edit_message_reply_markup.assert_called_once_with(chat_id=999, message_id=777, reply_markup=None)


@pytest.mark.asyncio
async def test_map_quiz_timeout_removes_tip_button(commands, context):
    person = {"name_pl": "Jan Kowalski", "description": "Jan Kowalski - fizyk."}
    commands.bot_state.map_quiz_cache[111] = {
        "chat_id": 999,
        "thread_id": 42,
        "person": person,
        "message_id": 777,
        "job": MagicMock(),
    }
    context.job.data = 111

    await commands.map_quiz_timeout(context)

    context.bot.edit_message_reply_markup.assert_called_once_with(chat_id=999, message_id=777, reply_markup=None)


@pytest.mark.asyncio
async def test_handle_map_quiz_answer_typed_tip(commands, update, context):
    person = {"name_pl": "Jan Kowalski", "description": "Jan Kowalski - fizyk. Noblista."}
    commands.bot_state.map_quiz_cache[111] = {
        "chat_id": 999,
        "thread_id": 42,
        "person": person,
        "difficulty": "easy",
        "category_specified": False,
        "continent_specified": False,
        "tips_given": 0,
        "tip_lines": [],
        "message_id": 777,
        "has_photo": True,
        "base_text": "Difficulty: Easy",
        "job": MagicMock(),
    }
    update.message.text = "!tip"

    await commands.handle_map_quiz_answer(update, context)

    context.bot.edit_message_caption.assert_called_once()
    assert commands.bot_state.map_quiz_cache[111]["tips_given"] == 1


@pytest.mark.asyncio
async def test_handle_map_quiz_answer_typed_tip_no_more_tips(commands, update, context):
    person = {"name_pl": "Jan Kowalski", "description": "Jan Kowalski - fizyk."}
    commands.bot_state.map_quiz_cache[111] = {
        "chat_id": 999,
        "thread_id": 42,
        "person": person,
        "difficulty": "easy",
        "category_specified": False,
        "continent_specified": False,
        "tips_given": 10,
        "tip_lines": [],
        "message_id": 777,
        "has_photo": True,
        "base_text": "Difficulty: Easy",
        "job": MagicMock(),
    }
    update.message.text = "!tip"

    await commands.handle_map_quiz_answer(update, context)

    context.bot.send_message.assert_called_once()
    assert "No more tips available for this person" in context.bot.send_message.call_args.kwargs["text"]


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


@pytest.mark.asyncio
async def test_handle_tournament_message_start_command(mocker, commands, update, context):
    context.args = ["roulette", "100"]
    commands.bot_state.is_tournament_banned.return_value = False
    commands.credits.credits = {111: 500, 222: 500}
    mocker.patch("src.commands.credit_commands.core_utils.send_message", new_callable=AsyncMock)

    await commands.cmd_tournament(update, context)
    t = commands.active_tournaments[999]

    # Non-host tries to start
    update.effective_user.id = 222
    update.message.text = "start"
    await commands.handle_tournament_message(update, context)
    assert t.state.value == "joining"

    # Host tries to start when < 2 players
    update.effective_user.id = 111
    update.message.text = "start"
    await commands.handle_tournament_message(update, context)
    assert t.state.value == "joining"

    # Player 2 joins
    update.effective_user.id = 222
    update.message.text = "join"
    await commands.handle_tournament_message(update, context)
    assert len(t.players) == 2

    # Host starts early
    update.effective_user.id = 111
    update.message.text = "start"
    await commands.handle_tournament_message(update, context)
    assert t.state.value == "starting"

    # Countdown finished
    job_mock = MagicMock()
    job_mock.data = {"chat_id": 999, "thread_id": 42}
    context.job = job_mock
    await commands._on_start_countdown_finished(context)
    assert t.state.value == "betting"


@pytest.mark.asyncio
async def test_cmd_tournament_wait_arg(mocker, commands, update, context):
    context.args = ["roulette", "100", "--wait", "120"]
    commands.bot_state.is_tournament_banned.return_value = False
    commands.credits.credits = {111: 500}
    mocker.patch("src.commands.credit_commands.core_utils.send_message", new_callable=AsyncMock)

    await commands.cmd_tournament(update, context)

    context.job_queue.run_once.assert_called_once()
    timeout_arg = context.job_queue.run_once.call_args[0][1]
    assert timeout_arg == 120


@pytest.mark.asyncio
async def test_btn_quiz_replay_callback_unauthorized_user(commands, context):
    update = MagicMock()
    update.callback_query = MagicMock()
    update.callback_query.data = "quizreplay:quiz:111"
    update.callback_query.from_user.id = 222
    update.callback_query.answer = AsyncMock()
    commands.cmd_quiz = AsyncMock()

    await commands.btn_quiz_replay_callback(update, context)

    update.callback_query.answer.assert_awaited_once_with("Start your own quiz with /quiz!")
    commands.cmd_quiz.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "quiz_type, method_name, cached_args",
    [
        ("quiz", "cmd_quiz", ["--difficulty", "hard"]),
        ("map", "cmd_guess_person_on_a_map", ["--category", "Science"]),
        ("flag", "cmd_guess_flag", ["--continent", "Europe"]),
        ("capital", "cmd_guess_capital", ["--difficulty", "easy"]),
    ],
)
async def test_btn_quiz_replay_callback_dispatches_with_cached_args(commands, context, quiz_type, method_name, cached_args):
    update = MagicMock()
    query = MagicMock()
    query.data = f"quizreplay:{quiz_type}:111"
    query.from_user.id = 111
    query.answer = AsyncMock()
    query.edit_message_reply_markup = AsyncMock()
    query.message = MagicMock()
    update.callback_query = query

    commands.bot_state.quiz_replay_cache[111][quiz_type] = cached_args
    mock_method = AsyncMock()
    setattr(commands, method_name, mock_method)

    await commands.btn_quiz_replay_callback(update, context)

    query.answer.assert_awaited_once()
    query.edit_message_reply_markup.assert_awaited_once_with(reply_markup=None)
    assert context.args == cached_args
    mock_method.assert_awaited_once()
    called_update, called_context = mock_method.call_args[0]
    assert called_update.message is query.message
    assert called_update.effective_user.id == 111
    assert called_context is context


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["timeout", "wrong", "correct"])
async def test_btn_quiz_callback_attaches_replay_button(mocker, commands, context, outcome):
    from datetime import datetime, timedelta

    from src.models.quizzes.quiz_model import QuizModel

    update = MagicMock()
    query = MagicMock()
    query.message.message_id = 123
    query.from_user.id = 111
    query.edit_message_reply_markup = AsyncMock()

    now = datetime(2026, 1, 1, 12, 0, 0)
    mocker.patch("src.commands.credit_commands.core_utils.get_dt_now", return_value=now)
    mocker.patch("src.commands.credit_commands.core_utils.roll", return_value=False)
    mock_send = mocker.patch("src.commands.credit_commands.core_utils.send_message", new_callable=AsyncMock)

    if outcome == "timeout":
        start_dt = now - timedelta(seconds=50)
        seconds_to_answer = 10
        query.data = "4"
    elif outcome == "wrong":
        start_dt = now - timedelta(seconds=2)
        seconds_to_answer = 10
        query.data = "3"
    else:
        start_dt = now - timedelta(seconds=2)
        seconds_to_answer = 10
        query.data = "4"

    quiz = QuizModel(
        quiz_id=1,
        user_id=111,
        question="What is 2+2?",
        difficulty="easy",
        type="multiple",
        correct_answer="4",
        display_answer="4",
        start_dt=start_dt,
        seconds_to_answer=seconds_to_answer,
    )
    commands.bot_state.quiz_cache[123] = quiz
    update.callback_query = query

    await commands.btn_quiz_callback(update, context)

    mock_send.assert_awaited_once()
    reply_markup = mock_send.call_args.kwargs.get("reply_markup")
    assert reply_markup is not None
    assert reply_markup.inline_keyboard[0][0].callback_data == "quizreplay:quiz:111"


@pytest.mark.asyncio
@pytest.mark.parametrize("is_correct, user_answer", [(True, "jan kowalski"), (False, "adam mickiewicz")])
async def test_handle_map_quiz_answer_attaches_replay_button(commands, update, context, is_correct, user_answer):
    job_mock = MagicMock()
    person = {"name_pl": "Jan Kowalski", "category": "Science", "description": "some desc"}
    commands.bot_state.map_quiz_cache[111] = {
        "chat_id": 999,
        "thread_id": 42,
        "person": person,
        "difficulty": "easy",
        "category_specified": False,
        "tips_given": 0,
        "job": job_mock,
    }
    update.message.text = user_answer

    await commands.handle_map_quiz_answer(update, context)

    job_mock.schedule_removal.assert_called_once()
    assert 111 not in commands.bot_state.map_quiz_cache
    context.bot.send_message.assert_called_once()
    reply_markup = context.bot.send_message.call_args.kwargs.get("reply_markup")
    assert reply_markup is not None
    assert reply_markup.inline_keyboard[0][0].callback_data == "quizreplay:map:111"


@pytest.mark.asyncio
async def test_map_quiz_timeout_attaches_replay_button(commands, context):
    person = {"name_pl": "Jan Kowalski", "category": "Science", "description": "some desc"}
    commands.bot_state.map_quiz_cache[111] = {
        "chat_id": 999,
        "thread_id": 42,
        "person": person,
        "difficulty": "easy",
        "category_specified": False,
        "tips_given": 0,
        "job": MagicMock(),
    }
    context.job = MagicMock()
    context.job.data = 111

    await commands.map_quiz_timeout(context)

    assert 111 not in commands.bot_state.map_quiz_cache
    context.bot.send_message.assert_called_once()
    reply_markup = context.bot.send_message.call_args.kwargs.get("reply_markup")
    assert reply_markup is not None
    assert reply_markup.inline_keyboard[0][0].callback_data == "quizreplay:map:111"
