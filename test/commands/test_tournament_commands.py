from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest

from src.commands.tournament_commands import TournamentCommands
from src.config.enums import TournamentState, TournamentType
from src.models.tournaments.poker_tournament import PokerTournament


@pytest.fixture
def tournament_commands():
    db = MagicMock()
    db.load_table.return_value = pd.DataFrame(
        {
            "user_id": [111, 222, 333],
            "first_name": ["Alice", "Bob", "Charlie"],
            "last_name": ["", "", ""],
            "username": ["a", "b", "c"],
            "final_username": ["Alice", "Bob", "Charlie"],
            "nicknames": [[], [], []],
        }
    )
    credits = MagicMock()
    credits.credits = {111: 1000, 222: 1000, 333: 1000}
    return TournamentCommands(
        command_logger=MagicMock(),
        job_persistance=MagicMock(),
        bot_state=MagicMock(),
        credits=credits,
        db=db,
        assets=MagicMock(),
    )


@pytest.fixture
def update():
    u = MagicMock()
    u.effective_chat.id = 999
    u.effective_user.id = 111
    u.effective_user.first_name = "Alice"
    u.effective_user.last_name = ""
    u.message.message_thread_id = 42
    u.callback_query = AsyncMock()
    return u


@pytest.fixture
def context():
    c = MagicMock()
    c.bot.send_message = AsyncMock()
    sent_photo = MagicMock()
    sent_photo.message_id = 555
    c.bot.send_photo = AsyncMock(return_value=sent_photo)
    c.bot.edit_message_caption = AsyncMock()
    c.bot.edit_message_media = AsyncMock()
    c.bot.delete_message = AsyncMock()
    c.job_queue.run_once = MagicMock()
    c.job_queue.get_jobs_by_name = MagicMock(return_value=[])
    return c


@pytest.mark.asyncio
async def test_cmd_poker_creates_poker_tournament_with_lobby_buttons(mocker, tournament_commands, update, context):
    context.args = ["100", "--rounds", "3"]
    send_mock = mocker.patch("src.commands.tournament_commands.core_utils.send_message", new_callable=AsyncMock)

    await tournament_commands.cmd_poker(update, context)

    send_mock.assert_called_once()
    assert 999 in tournament_commands.active_tournaments
    t = tournament_commands.active_tournaments[999]
    assert isinstance(t, PokerTournament)
    assert t.tournament_type == TournamentType.POKER
    assert t.max_rounds == 3
    assert send_mock.call_args.kwargs.get("reply_markup") is not None


@pytest.mark.asyncio
async def test_lobby_callback_join_and_start(mocker, tournament_commands, update, context):
    context.args = ["100"]
    mocker.patch("src.commands.tournament_commands.core_utils.send_message", new_callable=AsyncMock)
    await tournament_commands.cmd_poker(update, context)

    update.effective_user.id = 222
    update.effective_user.first_name = "Bob"
    update.callback_query.data = "tourlobby:join"
    await tournament_commands.btn_tournament_lobby_callback(update, context)

    t = tournament_commands.active_tournaments[999]
    assert len(t.players) == 2

    update.effective_user.id = 111
    update.callback_query.data = "tourlobby:start"
    await tournament_commands.btn_tournament_lobby_callback(update, context)

    assert t.state == TournamentState.BETTING
    context.bot.send_photo.assert_called_once()
    assert t.table_message_id == 555


@pytest.mark.asyncio
async def test_poker_callbacks_and_smart_edit_vs_repost(mocker, tournament_commands, update, context):
    context.args = ["100"]
    mocker.patch("src.commands.tournament_commands.core_utils.send_message", new_callable=AsyncMock)
    sleep_mock = mocker.patch("src.commands.tournament_commands.asyncio.sleep", new_callable=AsyncMock)
    await tournament_commands.cmd_poker(update, context)
    t = tournament_commands.active_tournaments[999]
    t.add_player(222, "Bob")
    await tournament_commands._start_betting_round(context, 999, 42)
    context.bot.send_photo.reset_mock()

    update.effective_user.id = 111
    update.callback_query.data = "poker:cards"
    await tournament_commands.btn_poker_callback(update, context)
    update.callback_query.answer.assert_called_with(mocker.ANY, show_alert=True)

    turn_uid = t.current_turn_user_id
    update.effective_user.id = turn_uid
    update.callback_query.data = "poker:call"
    await tournament_commands.btn_poker_callback(update, context)

    context.bot.edit_message_media.assert_called_once()
    context.bot.send_photo.assert_not_called()

    update.message.text = "hello chat"
    await tournament_commands.handle_tournament_message(update, context)
    assert t.chat_messages_since_table == 1

    next_uid = t.current_turn_user_id
    update.effective_user.id = next_uid
    update.callback_query.data = "poker:check"
    await tournament_commands.btn_poker_callback(update, context)

    sleep_mock.assert_called()
    context.bot.send_photo.assert_called_once()
    context.bot.delete_message.assert_called_once_with(chat_id=999, message_id=555)
    assert t.chat_messages_since_table == 0


@pytest.mark.asyncio
async def test_poker_fold_flex_and_next_hand_callbacks(mocker, tournament_commands, update, context):
    context.args = ["100"]
    mocker.patch("src.commands.tournament_commands.core_utils.send_message", new_callable=AsyncMock)
    await tournament_commands.cmd_poker(update, context)
    t = tournament_commands.active_tournaments[999]
    t.add_player(222, "Bob")
    await tournament_commands._start_betting_round(context, 999, 42)

    turn_uid = t.current_turn_user_id
    other_uid = 222 if turn_uid == 111 else 111
    update.effective_user.id = turn_uid
    update.callback_query.data = "poker:fold"
    await tournament_commands.btn_poker_callback(update, context)

    assert t.state == TournamentState.ROUND_RESULT
    assert t.won_by_fold_user_id == other_uid

    update.effective_user.id = other_uid
    update.callback_query.data = "poker:flex"
    await tournament_commands.btn_poker_callback(update, context)
    assert t.players[other_uid].revealed_cards is True
    assert context.bot.edit_message_media.call_count == 2

    update.callback_query.data = "poker:next_hand"
    await tournament_commands.btn_poker_callback(update, context)
    assert t.round_number == 2
    assert t.state == TournamentState.BETTING


@pytest.mark.asyncio
async def test_cmd_poker_no_args_returns_missing_argument_error(mocker, tournament_commands, update, context):
    context.args = []
    send_mock = mocker.patch("src.commands.tournament_commands.core_utils.send_message", new_callable=AsyncMock)

    await tournament_commands.cmd_poker(update, context)

    send_mock.assert_called_once()
    assert "Missing required argument" in send_mock.call_args.args[3]
    assert 999 not in tournament_commands.active_tournaments


@pytest.mark.asyncio
async def test_solo_poker_start_auto_adds_bot_and_add_bot_button(mocker, tournament_commands, update, context):
    context.args = ["100"]
    mocker.patch("src.commands.tournament_commands.core_utils.send_message", new_callable=AsyncMock)
    await tournament_commands.cmd_poker(update, context)

    t = tournament_commands.active_tournaments[999]
    assert len(t.players) == 1

    update.callback_query.data = "tourlobby:add_bot"
    await tournament_commands.btn_tournament_lobby_callback(update, context)
    assert len(t.players) == 2
    assert -1 in t.players

    update.callback_query.data = "tourlobby:start"
    await tournament_commands.btn_tournament_lobby_callback(update, context)
    assert t.state in (TournamentState.BETTING, TournamentState.ROUND_RESULT)


@pytest.mark.asyncio
async def test_out_of_turn_premove_callback_and_auto_execution(mocker, tournament_commands, update, context):
    context.args = ["100"]
    mocker.patch("src.commands.tournament_commands.core_utils.send_message", new_callable=AsyncMock)
    mocker.patch("src.commands.tournament_commands.asyncio.sleep", new_callable=AsyncMock)
    await tournament_commands.cmd_poker(update, context)
    t = tournament_commands.active_tournaments[999]
    t.add_player(222, "Bob")
    await tournament_commands._start_betting_round(context, 999, 42)

    turn_uid = t.current_turn_user_id
    other_uid = 222 if turn_uid == 111 else 111

    # Out-of-turn player clicks check -> sets premove
    update.effective_user.id = other_uid
    update.callback_query.data = "poker:check"
    await tournament_commands.btn_poker_callback(update, context)
    assert t.players[other_uid].premove is not None
    update.callback_query.answer.assert_called_with(mocker.ANY, show_alert=True)

    # Current turn player calls -> other_uid's premove check auto-executes and advances street to Flop!
    update.effective_user.id = turn_uid
    update.callback_query.data = "poker:call"
    await tournament_commands.btn_poker_callback(update, context)
    assert t.street.value == "Flop"


@pytest.mark.asyncio
async def test_cmd_poker_cancel_refunds_and_cleans_up(mocker, tournament_commands, update, context):
    context.args = ["100"]
    send_mock = mocker.patch("src.commands.tournament_commands.core_utils.send_message", new_callable=AsyncMock)
    await tournament_commands.cmd_poker(update, context)
    assert 999 in tournament_commands.active_tournaments

    # Cancel command
    context.args = ["cancel"]
    await tournament_commands.cmd_poker(update, context)
    assert 999 not in tournament_commands.active_tournaments
    send_mock.assert_called_with(update, context, mocker.ANY, mocker.ANY)
    assert "cancelled" in send_mock.call_args[0][3].lower()


@pytest.mark.asyncio
async def test_stale_tournament_auto_cleaned_on_new_poker_game(mocker, tournament_commands, update, context):
    context.args = ["100"]
    mocker.patch("src.commands.tournament_commands.core_utils.send_message", new_callable=AsyncMock)
    await tournament_commands.cmd_poker(update, context)
    t = tournament_commands.active_tournaments[999]

    # Artificially make tournament stale (> 180s)
    t.last_activity_time -= 200
    assert t.is_stale() is True

    # Starting a new game cleans up the stale one
    context.args = ["200"]
    await tournament_commands.cmd_poker(update, context)
    assert 999 in tournament_commands.active_tournaments
    assert tournament_commands.active_tournaments[999].buy_in == 200
