from collections import defaultdict
from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest

from src.commands.credit_commands import CreditCommands
from src.config.enums import CreditActionType
from src.models.quizzes.capital_quiz import CapitalQuiz


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
    c.update_credits.return_value = (4000, True)
    return c


@pytest.fixture()
def bot_state():
    bs = MagicMock()
    bs.map_quiz_cache = {}
    bs.flag_quiz_cache = {}
    bs.capital_quiz_cache = {}
    bs.quiz_replay_cache = defaultdict(dict)
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

    def mock_pop_random_country(filtered_df=None):
        df = filtered_df if filtered_df is not None else countries_df
        return df.sample(n=1).iloc[0].to_dict()

    a.countries.get_countries.side_effect = mock_get_countries
    a.countries.pop_random_country.side_effect = mock_pop_random_country
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
async def test_cmd_guess_capital_already_active(mocker, commands, update, context):
    commands.bot_state.capital_quiz_cache[111] = {"thread_id": 42}
    mock_send = mocker.patch("src.commands.credit_commands.core_utils.send_message", new_callable=AsyncMock)

    await commands.cmd_guess_capital(update, context)

    mock_send.assert_called_once()
    assert "already have an active quiz" in mock_send.call_args[0][3]


@pytest.mark.asyncio
async def test_cmd_guess_capital_blocked_by_flag_quiz(mocker, commands, update, context):
    commands.bot_state.flag_quiz_cache[111] = {"thread_id": 42}
    mock_send = mocker.patch("src.commands.credit_commands.core_utils.send_message", new_callable=AsyncMock)

    await commands.cmd_guess_capital(update, context)

    mock_send.assert_called_once()
    assert "already have an active quiz" in mock_send.call_args[0][3]


@pytest.mark.asyncio
async def test_cmd_guess_capital_invalid_difficulty(mocker, commands, update, context):
    context.args = ["--difficulty", "supercrazy"]
    mock_send = mocker.patch("src.commands.credit_commands.core_utils.send_message", new_callable=AsyncMock)

    await commands.cmd_guess_capital(update, context)

    mock_send.assert_called_once()
    assert "Invalid difficulty" in mock_send.call_args[0][3]


@pytest.mark.asyncio
async def test_cmd_guess_capital_valid(mocker, commands, update, context):
    context.args = ["--difficulty", "easy"]
    await commands.cmd_guess_capital(update, context)

    update.message.reply_photo.assert_not_called()
    update.message.reply_text.assert_called_once()
    sent_text = update.message.reply_text.call_args.kwargs["text"]
    assert "Country\\: *Polska*" in sent_text
    assert "Guess the Capital" in sent_text
    assert 111 in commands.bot_state.capital_quiz_cache
    cached = commands.bot_state.capital_quiz_cache[111]
    assert cached["difficulty"] == "easy"
    assert cached["country"]["country_code"] == "PL"
    assert not cached["continent_specified"]
    context.job_queue.run_once.assert_called_once()


@pytest.mark.asyncio
async def test_cmd_guess_capital_with_continent(mocker, commands, update, context):
    context.args = ["--continent", "Europa", "--difficulty", "medium"]
    await commands.cmd_guess_capital(update, context)

    update.message.reply_photo.assert_not_called()
    update.message.reply_text.assert_called_once()
    assert "Country\\: *Niemcy*" in update.message.reply_text.call_args.kwargs["text"]
    assert 111 in commands.bot_state.capital_quiz_cache
    cached = commands.bot_state.capital_quiz_cache[111]
    assert cached["continent_specified"] is True
    assert cached["country"]["country_code"] == "DE"


@pytest.mark.asyncio
async def test_cmd_guess_capital_no_args(mocker, commands, update, context):
    context.args = []
    await commands.cmd_guess_capital(update, context)

    update.message.reply_photo.assert_not_called()
    update.message.reply_text.assert_called_once()
    assert 111 in commands.bot_state.capital_quiz_cache
    cached = commands.bot_state.capital_quiz_cache[111]
    assert cached["difficulty"] in ["easy", "medium", "hard"]
    assert not cached["continent_specified"]


@pytest.mark.asyncio
async def test_cmd_guess_capital_continent_only(mocker, commands, update, context):
    context.args = ["--continent", "Europa"]
    await commands.cmd_guess_capital(update, context)

    update.message.reply_photo.assert_not_called()
    update.message.reply_text.assert_called_once()
    assert 111 in commands.bot_state.capital_quiz_cache
    cached = commands.bot_state.capital_quiz_cache[111]
    assert cached["difficulty"] in ["easy", "medium", "hard"]
    assert cached["continent_specified"] is True


@pytest.mark.asyncio
async def test_handle_capital_quiz_answer_ignores_not_in_cache(commands, update, context):
    update.message.text = "Warszawa"
    await commands.handle_capital_quiz_answer(update, context)
    context.bot.send_message.assert_not_called()
    commands.credits.update_credits.assert_not_called()


@pytest.mark.asyncio
async def test_handle_capital_quiz_answer_wrong_thread(commands, update, context):
    commands.bot_state.capital_quiz_cache[111] = {"thread_id": 99, "country": {}, "job": MagicMock()}
    update.message.text = "Warszawa"
    await commands.handle_capital_quiz_answer(update, context)
    context.bot.send_message.assert_not_called()
    commands.credits.update_credits.assert_not_called()


@pytest.mark.asyncio
async def test_handle_capital_quiz_answer_tip_without_continent(commands, update, context):
    country = {"country_name": "Polska", "country_code": "PL", "continent": "Europa", "population": 38000000, "capital": "Warszawa"}
    commands.bot_state.capital_quiz_cache[111] = {
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
    await commands.handle_capital_quiz_answer(update, context)
    assert commands.bot_state.capital_quiz_cache[111]["tips_given"] == 1
    text1 = context.bot.send_message.call_args[1]["text"]
    assert "Tip 1/4" in text1
    assert "Continent\\: Europa" in text1

    # Tip 2 (First letter of capital)
    await commands.handle_capital_quiz_answer(update, context)
    assert commands.bot_state.capital_quiz_cache[111]["tips_given"] == 2
    text2 = context.bot.send_message.call_args[1]["text"]
    assert "Tip 2/4" in text2
    assert "First letter\\: W" in text2

    # Tip 3 (Second letter of capital)
    await commands.handle_capital_quiz_answer(update, context)
    assert commands.bot_state.capital_quiz_cache[111]["tips_given"] == 3
    text3 = context.bot.send_message.call_args[1]["text"]
    assert "Tip 3/4" in text3
    assert "Second letter\\: a" in text3

    # Tip 4 (Next two letters of capital)
    await commands.handle_capital_quiz_answer(update, context)
    assert commands.bot_state.capital_quiz_cache[111]["tips_given"] == 4
    text4 = context.bot.send_message.call_args[1]["text"]
    assert "Tip 4/4" in text4
    assert "Next two letters\\: rs" in text4


@pytest.mark.asyncio
async def test_handle_capital_quiz_answer_tip_with_continent(commands, update, context):
    country = {"country_name": "Polska", "country_code": "PL", "continent": "Europa", "population": 38000000, "capital": "Warszawa"}
    commands.bot_state.capital_quiz_cache[111] = {
        "chat_id": 999,
        "thread_id": 42,
        "country": country,
        "difficulty": "easy",
        "continent_specified": True,
        "tips_given": 0,
        "job": MagicMock(),
    }
    update.message.text = "!tip"

    # Tip 1 (First letter — continent tip skipped, already shown in the caption)
    await commands.handle_capital_quiz_answer(update, context)
    assert commands.bot_state.capital_quiz_cache[111]["tips_given"] == 1
    text1 = context.bot.send_message.call_args[1]["text"]
    assert "Tip 1/3" in text1
    assert "First letter\\: W" in text1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "user_answer, is_correct",
    [
        ("warszawa", True),
        ("Warszawa", True),
        ("warszaw", True),  # Fuzzy 1
        ("berlin", False),
    ],
)
async def test_handle_capital_quiz_answer_logic(commands, update, context, user_answer, is_correct):
    job_mock = MagicMock()
    country = {"country_name": "Polska", "country_code": "PL", "continent": "Europa", "population": 38000000, "capital": "Warszawa"}
    commands.bot_state.capital_quiz_cache[111] = {
        "chat_id": 999,
        "thread_id": 42,
        "country": country,
        "difficulty": "easy",
        "continent_specified": False,
        "tips_given": 0,
        "job": job_mock,
    }
    update.message.text = user_answer

    await commands.handle_capital_quiz_answer(update, context)

    job_mock.schedule_removal.assert_called_once()
    assert 111 not in commands.bot_state.capital_quiz_cache

    if is_correct:
        expected_reward = CapitalQuiz.REWARD_LEVELS["easy"]
        commands.credits.update_credits.assert_called_once_with(
            user_id=111, credit_change=expected_reward, action_type=CreditActionType.QUIZ
        )
        assert "Correct\\! The capital of *Polska* is *Warszawa*" in context.bot.send_message.call_args[1]["text"]
    else:
        commands.credits.update_credits.assert_not_called()
        assert "Wrong\\! The capital of *Polska* was *Warszawa*" in context.bot.send_message.call_args[1]["text"]


@pytest.mark.asyncio
async def test_capital_quiz_timeout(commands, context):
    country = {"country_name": "Polska", "country_code": "PL", "continent": "Europa", "population": 38000000, "capital": "Warszawa"}
    commands.bot_state.capital_quiz_cache[111] = {
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

    await commands.capital_quiz_timeout(context)

    assert 111 not in commands.bot_state.capital_quiz_cache
    context.bot.send_message.assert_called_once()
    assert "Time's up\\! The capital of *Polska* was *Warszawa*" in context.bot.send_message.call_args[1]["text"]


POLAND = {"country_name": "Polska", "country_code": "PL", "continent": "Europa", "population": 38000000, "capital": "Warszawa"}
BASE_TEXT = "Difficulty: Easy\n\nCountry: *Polska*"


def make_cached_quiz(**overrides):
    cached = {
        "chat_id": 999,
        "thread_id": 42,
        "country": POLAND,
        "difficulty": "easy",
        "continent_specified": False,
        "tips_given": 0,
        "tip_lines": [],
        "message_id": 555,
        "has_photo": False,
        "base_text": BASE_TEXT,
        "job": MagicMock(),
    }
    cached.update(overrides)
    return cached


def make_tip_update(user_id=111, message_id=555, data="quiztip:111"):
    u = MagicMock()
    u.callback_query.data = data
    u.callback_query.from_user.id = user_id
    u.callback_query.message.message_id = message_id
    u.callback_query.answer = AsyncMock()
    return u


@pytest.mark.asyncio
async def test_cmd_guess_capital_attaches_tip_button(commands, update, context):
    context.args = ["--difficulty", "easy"]
    update.message.reply_text.return_value.message_id = 555

    await commands.cmd_guess_capital(update, context)

    kwargs = update.message.reply_text.call_args.kwargs
    assert kwargs["parse_mode"] == "MarkdownV2"
    button = kwargs["reply_markup"].inline_keyboard[0][0]
    assert button.callback_data == "quiztip:111"
    assert "Tip 1/4" in button.text
    cached = commands.bot_state.capital_quiz_cache[111]
    assert cached["message_id"] == 555
    assert cached["has_photo"] is False
    assert "Country: *Polska*" in cached["base_text"]
    assert cached["tip_lines"] == []


@pytest.mark.asyncio
async def test_cmd_guess_capital_continent_tip_button_counts_three_tips(commands, update, context):
    context.args = ["--continent", "Europa", "--difficulty", "easy"]

    await commands.cmd_guess_capital(update, context)

    button = update.message.reply_text.call_args.kwargs["reply_markup"].inline_keyboard[0][0]
    assert "Tip 1/3" in button.text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "has_photo, edit_method, other_method",
    [(False, "edit_message_text", "edit_message_caption"), (True, "edit_message_caption", "edit_message_text")],
)
async def test_tip_button_edits_quiz_message_in_place(commands, context, has_photo, edit_method, other_method):
    cached = make_cached_quiz(has_photo=has_photo)
    commands.bot_state.capital_quiz_cache[111] = cached
    tip_update = make_tip_update()

    await commands.btn_quiz_tip_callback(tip_update, context)

    tip_update.callback_query.answer.assert_called_once_with()
    getattr(context.bot, other_method).assert_not_called()
    context.bot.send_message.assert_not_called()
    edit_kwargs = getattr(context.bot, edit_method).call_args.kwargs
    text = edit_kwargs["caption" if has_photo else "text"]
    assert "Country\\: *Polska*" in text
    assert "Tip 1/4" in text
    assert "Continent\\: Europa" in text
    assert edit_kwargs["chat_id"] == 999
    assert edit_kwargs["message_id"] == 555
    assert edit_kwargs["parse_mode"] == "MarkdownV2"
    assert "Tip 2/4" in edit_kwargs["reply_markup"].inline_keyboard[0][0].text
    assert cached["tips_given"] == 1


@pytest.mark.asyncio
async def test_tip_button_accumulates_tips_and_removes_button_after_last(commands, context):
    commands.bot_state.capital_quiz_cache[111] = make_cached_quiz()

    for _ in range(4):
        await commands.btn_quiz_tip_callback(make_tip_update(), context)

    edit_kwargs = context.bot.edit_message_text.call_args.kwargs
    for number in range(1, 5):
        assert f"Tip {number}/4" in edit_kwargs["text"]
    assert "Next two letters\\: rs" in edit_kwargs["text"]
    assert edit_kwargs["reply_markup"].inline_keyboard == ()


@pytest.mark.asyncio
async def test_tip_button_no_more_tips(commands, context):
    commands.bot_state.capital_quiz_cache[111] = make_cached_quiz(tips_given=4)
    tip_update = make_tip_update()

    await commands.btn_quiz_tip_callback(tip_update, context)

    tip_update.callback_query.answer.assert_called_once_with("No more tips available!")
    context.bot.edit_message_text.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "caller_id, message_id, data, cache_quiz, expected_answer",
    [
        (222, 555, "quiztip:111", True, "This isn't your quiz!"),
        (111, 555, "garbage", True, "This isn't your quiz!"),
        (111, 555, "quiztip:111", False, "This quiz is already over."),
        (111, 999, "quiztip:111", True, "This quiz is already over."),
    ],
)
async def test_tip_button_rejected(commands, context, caller_id, message_id, data, cache_quiz, expected_answer):
    if cache_quiz:
        commands.bot_state.capital_quiz_cache[111] = make_cached_quiz()
    tip_update = make_tip_update(user_id=caller_id, message_id=message_id, data=data)

    await commands.btn_quiz_tip_callback(tip_update, context)

    tip_update.callback_query.answer.assert_called_once_with(expected_answer)
    context.bot.edit_message_text.assert_not_called()
    context.bot.send_message.assert_not_called()
    if cache_quiz:
        assert commands.bot_state.capital_quiz_cache[111]["tips_given"] == 0


@pytest.mark.asyncio
async def test_tip_button_flag_quiz(commands, context):
    commands.bot_state.flag_quiz_cache[111] = make_cached_quiz(has_photo=True, base_text="Difficulty: Easy")

    await commands.btn_quiz_tip_callback(make_tip_update(), context)

    caption = context.bot.edit_message_caption.call_args.kwargs["caption"]
    assert "Tip 1/3" in caption
    assert "Continent\\: Europa" in caption


@pytest.mark.asyncio
async def test_tip_button_falls_back_to_reply_when_caption_too_long(commands, context):
    commands.bot_state.capital_quiz_cache[111] = make_cached_quiz(has_photo=True, base_text="x" * 1000)

    await commands.btn_quiz_tip_callback(make_tip_update(), context)

    context.bot.edit_message_caption.assert_not_called()
    send_kwargs = context.bot.send_message.call_args.kwargs
    assert send_kwargs["reply_to_message_id"] == 555
    assert "Tip 1/4" in send_kwargs["text"]
    reply_markup_kwargs = context.bot.edit_message_reply_markup.call_args.kwargs
    assert "Tip 2/4" in reply_markup_kwargs["reply_markup"].inline_keyboard[0][0].text


@pytest.mark.asyncio
async def test_tip_button_edit_failure_is_swallowed(commands, context):
    import telegram

    commands.bot_state.capital_quiz_cache[111] = make_cached_quiz()
    context.bot.edit_message_text.side_effect = telegram.error.BadRequest("Message is not modified")

    await commands.btn_quiz_tip_callback(make_tip_update(), context)

    assert commands.bot_state.capital_quiz_cache[111]["tips_given"] == 1


@pytest.mark.asyncio
async def test_typed_tip_edits_quiz_message_in_place(commands, update, context):
    commands.bot_state.capital_quiz_cache[111] = make_cached_quiz()
    update.message.text = "!tip"

    await commands.handle_capital_quiz_answer(update, context)

    context.bot.send_message.assert_not_called()
    assert "Tip 1/4" in context.bot.edit_message_text.call_args.kwargs["text"]


@pytest.mark.asyncio
async def test_typed_tip_no_more_tips(commands, update, context):
    commands.bot_state.capital_quiz_cache[111] = make_cached_quiz(tips_given=4)
    update.message.text = "!tip"

    await commands.handle_capital_quiz_answer(update, context)

    assert "No more tips available for this quiz" in context.bot.send_message.call_args.kwargs["text"]


@pytest.mark.asyncio
async def test_answering_removes_tip_button(commands, update, context):
    commands.bot_state.capital_quiz_cache[111] = make_cached_quiz()
    update.message.text = "warszawa"

    await commands.handle_capital_quiz_answer(update, context)

    context.bot.edit_message_reply_markup.assert_called_once_with(chat_id=999, message_id=555, reply_markup=None)
    reply_markup = context.bot.send_message.call_args.kwargs.get("reply_markup")
    assert reply_markup is not None
    assert reply_markup.inline_keyboard[0][0].callback_data == "quizreplay:capital:111"


@pytest.mark.asyncio
async def test_timeout_removes_tip_button(commands, context):
    commands.bot_state.capital_quiz_cache[111] = make_cached_quiz()
    context.job.data = 111

    await commands.capital_quiz_timeout(context)

    context.bot.edit_message_reply_markup.assert_called_once_with(chat_id=999, message_id=555, reply_markup=None)
    reply_markup = context.bot.send_message.call_args.kwargs.get("reply_markup")
    assert reply_markup is not None
    assert reply_markup.inline_keyboard[0][0].callback_data == "quizreplay:capital:111"


@pytest.mark.asyncio
async def test_cmd_guess_capital_caches_replay_args(commands, update, context):
    context.args = ["--continent", "Europe", "--difficulty", "easy"]
    await commands.cmd_guess_capital(update, context)
    assert commands.bot_state.quiz_replay_cache[111]["capital"] == ["--continent", "Europe", "--difficulty", "easy"]
