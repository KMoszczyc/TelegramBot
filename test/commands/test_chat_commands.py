from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from src.commands.chat_commands import ChatCommands
from src.config.enums import EmojiType, ErrorMessage, MessageType, Table
from src.models.bot_state import BotState

# ---------------------------------------------------------------------------
# Fixture data
# ---------------------------------------------------------------------------

TIMEZONE = "Europe/Warsaw"

# fmt: off
USERS_DATA = [
    ("Alice", None, "alice", "user_a", ["nick_a"]),
    ("Bob", None, "bob", "user_b", ["nick_b1", "nick_b2"]),
    ("Charlie", None, "charlie", "user_c", []),
]
USERS_COLS = ["first_name", "last_name", "username", "final_username", "nicknames"]
USER_IDS = [111, 222, 333]

CHAT_DATA = [
    (1, pd.Timestamp("2025-01-10 10:00", tz=TIMEZONE), 111, "user_a", "hello world",   None, ["👍", "😂"],  [222, 333], "text"),
    (2, pd.Timestamp("2025-01-10 11:00", tz=TIMEZONE), 222, "user_b", "foo bar baz",   None, ["👍"],        [111],      "text"),
    (3, pd.Timestamp("2025-01-10 12:00", tz=TIMEZONE), 333, "user_c", "test message",  None, [],            [],         "text"),
    (4, pd.Timestamp("2025-01-10 13:00", tz=TIMEZONE), 111, "user_a", "",              None, [],            [],         "image"),
    (5, pd.Timestamp("2025-01-10 14:00", tz=TIMEZONE), 222, "user_b", "another text",  None, ["❤️"],       [111],      "text"),
    (6, pd.Timestamp("2025-01-11 09:00", tz=TIMEZONE), 111, "user_a", "morning msg",   None, ["👎"],        [222],      "text"),
    (7, pd.Timestamp("2025-01-11 10:00", tz=TIMEZONE), 333, "user_c", "video caption", None, ["👍", "👍"], [111, 222], "video"),
    (8, pd.Timestamp("2025-01-11 11:00", tz=TIMEZONE), 222, "user_b", "last one",      None, [],            [],         "video_note"),
]
CHAT_COLS = ["message_id", "timestamp", "user_id", "final_username", "text", "image_text", "reaction_emojis", "reaction_user_ids", "message_type"]

REACTIONS_DATA = [
    (1, pd.Timestamp("2025-01-10 10:05", tz=TIMEZONE), "user_a", "user_b", "hello world", "👍"),
    (1, pd.Timestamp("2025-01-10 10:06", tz=TIMEZONE), "user_a", "user_c", "hello world", "😂"),
    (2, pd.Timestamp("2025-01-10 11:05", tz=TIMEZONE), "user_b", "user_a", "foo bar baz", "👍"),
    (5, pd.Timestamp("2025-01-10 14:05", tz=TIMEZONE), "user_b", "user_a", "another text", "❤️"),
    (6, pd.Timestamp("2025-01-11 09:05", tz=TIMEZONE), "user_a", "user_b", "morning msg", "👎"),
    (7, pd.Timestamp("2025-01-11 10:05", tz=TIMEZONE), "user_c", "user_a", "video caption", "👍"),
    (7, pd.Timestamp("2025-01-11 10:06", tz=TIMEZONE), "user_c", "user_b", "video caption", "👍"),
]
REACTIONS_COLS = ["message_id", "timestamp", "reacted_to_username", "reacting_username", "text", "emoji"]

CWEL_DATA = [
    (pd.Timestamp("2025-01-10 10:00", tz=TIMEZONE), "user_a", "user_b", 1, 1),
    (pd.Timestamp("2025-01-10 11:00", tz=TIMEZONE), "user_a", "user_c", 2, 1),
    (pd.Timestamp("2025-01-11 09:00", tz=TIMEZONE), "user_b", "user_a", 3, 1),
]
CWEL_COLS = ["timestamp", "receiver_username", "giver_username", "reply_message_id", "value"]
# fmt: on


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def users_df():
    return pd.DataFrame(USERS_DATA, columns=USERS_COLS, index=USER_IDS)


@pytest.fixture()
def chat_df():
    return pd.DataFrame(CHAT_DATA, columns=CHAT_COLS)


@pytest.fixture()
def reactions_df():
    return pd.DataFrame(REACTIONS_DATA, columns=REACTIONS_COLS)


@pytest.fixture()
def cwel_df():
    return pd.DataFrame(CWEL_DATA, columns=CWEL_COLS)


@pytest.fixture()
def db(users_df, chat_df, reactions_df, cwel_df):
    class FakeDB:
        def __init__(self):
            self._tables = {
                Table.USERS: users_df,
                Table.CLEANED_CHAT_HISTORY: chat_df,
                Table.REACTIONS: reactions_df,
                Table.CWEL: cwel_df,
            }
            self._pending_ids: list[int] = []

        def load_table(self, table):
            return self._tables[table]

        def pop_updated_message_ids(self) -> list[int]:
            ids, self._pending_ids = self._pending_ids, []
            return ids

        def record_updated_message_ids(self, message_ids) -> None:
            self._pending_ids.extend(int(mid) for mid in message_ids)

        def load_rows_by_message_ids(self, table, message_ids):
            df = self._tables[table]
            return df[df["message_id"].isin(message_ids)].copy()

    return FakeDB()


@pytest.fixture()
def command_logger():
    cl = MagicMock()
    cl.get_commands.return_value = ["summary", "fun"]
    usage_df = pd.DataFrame(
        [
            (pd.Timestamp("2025-01-10 10:00", tz=TIMEZONE), 111, "summary", "user_a"),
            (pd.Timestamp("2025-01-10 11:00", tz=TIMEZONE), 222, "fun", "user_b"),
            (pd.Timestamp("2025-01-10 12:00", tz=TIMEZONE), 111, "summary", "user_a"),
        ],
        columns=["timestamp", "user_id", "command_name", "username"],
    )
    cl.preprocess_data.return_value = usage_df
    return cl


@pytest.fixture()
def job_persistance():
    return MagicMock()


@pytest.fixture()
def bot_state():
    bs = MagicMock(spec=BotState)
    return bs


@pytest.fixture()
def assets():
    a = MagicMock()
    return a


@pytest.fixture()
def chat_commands(command_logger, job_persistance, bot_state, db, assets):
    with patch("src.commands.chat_commands.WordStats"):
        cmds = ChatCommands(command_logger, job_persistance, bot_state, db, assets)
    cmds.ytdl = MagicMock()
    return cmds


@pytest.fixture()
def update():
    u = MagicMock()
    u.effective_chat.id = 999
    u.effective_user.id = 111
    u.message.message_thread_id = None
    u.message.message_id = 123
    u.message.reply_to_message = None
    u.message.from_user.id = 111
    return u


@pytest.fixture()
def context():
    ctx = MagicMock()
    ctx.args = []
    ctx.bot = MagicMock()
    ctx.bot.send_message = AsyncMock()
    ctx.job_queue = MagicMock()
    return ctx


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def sent_text(context) -> str:
    return context.bot.send_message.await_args.kwargs["text"]


def sent_kwargs(context) -> dict:
    return context.bot.send_message.await_args.kwargs


# ---------------------------------------------------------------------------
# Error path: parse error short-circuits
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "method_name, extra_kwargs",
    [
        pytest.param("cmd_messages_by_reactions", {}, id="messages_by_reactions"),
        pytest.param("cmd_last_messages", {}, id="last_messages"),
        pytest.param("cmd_fun", {}, id="fun"),
        pytest.param("cmd_wholesome", {}, id="wholesome"),
        pytest.param("cmd_funchart", {}, id="funchart"),
        pytest.param("cmd_spamchart", {}, id="spamchart"),
        pytest.param("cmd_monologuechart", {}, id="monologuechart"),
        pytest.param("cmd_likechart", {}, id="likechart"),
        pytest.param("cmd_relationship_graph", {}, id="relationship_graph"),
        pytest.param("cmd_topcwel", {}, id="topcwel"),
    ],
)
async def test_preprocess_error_short_circuits(mocker, chat_commands, update, context, method_name, extra_kwargs):
    ca = MagicMock(error="err")
    mocker.patch("src.commands.chat_commands.core_utils.parse_args", return_value=ca)

    await getattr(chat_commands, method_name)(update, context, **extra_kwargs)

    assert sent_text(context) == "err"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "method_name",
    [
        pytest.param("cmd_add_nickname", id="add_nickname"),
        pytest.param("cmd_set_username", id="set_username"),
        pytest.param("cmd_command_usage", id="command_usage"),
        pytest.param("cmd_cwel", id="cwel"),
        pytest.param("cmd_remind", id="remind"),
        pytest.param("cmd_play", id="play"),
        pytest.param("cmd_wordstats", id="wordstats"),
    ],
)
async def test_parse_only_error_short_circuits(mocker, chat_commands, update, context, method_name):
    ca = MagicMock(error="parse_err")
    mocker.patch("src.commands.chat_commands.core_utils.parse_args", return_value=ca)

    await getattr(chat_commands, method_name)(update, context)

    assert sent_text(context) == "parse_err"


# ---------------------------------------------------------------------------
# cmd_display_users
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cmd_display_users(mocker, chat_commands, update, context):
    mocker.patch("src.commands.chat_commands.stats_utils.escape_special_characters", side_effect=lambda s: s)

    await chat_commands.cmd_display_users(update, context)

    text = sent_text(context)
    assert "user_a" in text
    assert "user_b" in text
    assert "user_c" in text
    assert "nick_a" in text


# ---------------------------------------------------------------------------
# cmd_add_nickname
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cmd_add_nickname_happy_path(mocker, chat_commands, update, context):
    ca = MagicMock(error="", string="new_nick")
    mocker.patch("src.commands.chat_commands.core_utils.parse_args", return_value=ca)
    mocker.patch("src.commands.chat_commands.stats_utils.escape_special_characters", side_effect=lambda s: s)
    mocker.patch("src.commands.chat_commands.core_utils.save_df")

    await chat_commands.cmd_add_nickname(update, context)

    text = sent_text(context)
    assert "new_nick" in text
    assert "added" in text


@pytest.mark.asyncio
async def test_cmd_add_nickname_max_reached(mocker, chat_commands, update, context):
    ca = MagicMock(error="", string="new_nick")
    mocker.patch("src.commands.chat_commands.core_utils.parse_args", return_value=ca)
    mocker.patch("src.commands.chat_commands.stats_utils.escape_special_characters", side_effect=lambda s: s)
    mocker.patch("src.commands.chat_commands.MAX_NICKNAMES_NUM", 1)

    await chat_commands.cmd_add_nickname(update, context)

    text = sent_text(context)
    assert "limit" in text.lower() or "not added" in text.lower()


# ---------------------------------------------------------------------------
# cmd_set_username — disabled message
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cmd_set_username_disabled(mocker, chat_commands, update, context):
    context.args = ["newname"]
    mocker.patch("src.commands.chat_commands.stats_utils.escape_special_characters", side_effect=lambda s: s)

    await chat_commands.cmd_set_username(update, context)

    text = sent_text(context)
    assert "disabled" in text.lower()


# ---------------------------------------------------------------------------
# cmd_messages_by_reactions
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cmd_messages_by_reactions_happy_path(mocker, chat_commands, update, context):
    mocker.patch("src.commands.chat_commands.stats_utils.dt_to_str", return_value="10.01.2025")

    await chat_commands.cmd_messages_by_reactions(update, context, EmojiType.ALL)

    text = sent_text(context)
    assert "hello world" in text or "Cinco" in text


@pytest.mark.asyncio
async def test_cmd_messages_by_reactions_text_filter(mocker, chat_commands, update, context):
    context.args = ["--text", "hello"]
    mocker.patch("src.commands.chat_commands.stats_utils.dt_to_str", return_value="10.01.2025")

    await chat_commands.cmd_messages_by_reactions(update, context, EmojiType.ALL)

    text = sent_text(context)
    assert "foo bar" not in text


# ---------------------------------------------------------------------------
# cmd_last_messages
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cmd_last_messages_happy_path(mocker, chat_commands, update, context, chat_df):
    ca = MagicMock(error="", number=3, user=None)
    mocker.patch.object(chat_commands, "preprocess_input", return_value=(chat_df.copy(), pd.DataFrame(), ca))
    mocker.patch("src.commands.chat_commands.stats_utils.dt_to_str", return_value="10.01.2025")

    await chat_commands.cmd_last_messages(update, context)

    text = sent_text(context)
    assert "Last 3 messages" in text


@pytest.mark.asyncio
async def test_cmd_last_messages_too_long(mocker, chat_commands, update, context, chat_df):
    ca = MagicMock(error="", number=100, user=None)
    mocker.patch.object(chat_commands, "preprocess_input", return_value=(chat_df.copy(), pd.DataFrame(), ca))
    mocker.patch(
        "src.commands.chat_commands.stats_utils.dt_to_str",
        return_value="X" * 600,
    )

    await chat_commands.cmd_last_messages(update, context)

    text = sent_text(context)
    assert text == ErrorMessage.TOO_MUCH_TEXT


# ---------------------------------------------------------------------------
# cmd_summary — image-based output
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cmd_summary_sends_image(mocker, chat_commands, update, context):
    mocker.patch("src.commands.chat_commands.stats_utils.dt_to_str", return_value="10.01.2025")
    mocker.patch("src.commands.chat_commands.stats_utils.text_to_word_length_sum", return_value=5)
    mocker.patch("src.commands.chat_commands.stats_utils.filter_by_shifted_time_df", return_value=pd.DataFrame())
    mocker.patch("src.commands.chat_commands.stats_utils.filter_emoji_by_emoji_type", return_value=pd.DataFrame(columns=REACTIONS_COLS))
    mocker.patch("src.commands.chat_commands.charts.create_table_plotly", return_value="/fake/path.png")
    mock_send = mocker.patch("src.commands.chat_commands.core_utils.send_message", new_callable=AsyncMock)
    mocker.patch("src.commands.chat_commands.core_utils.generate_period_headline", return_value="Total")

    await chat_commands.cmd_summary(update, context)

    mock_send.assert_awaited_once()
    call_args = mock_send.await_args
    assert call_args.args[2] == MessageType.IMAGE


# ---------------------------------------------------------------------------
# cmd_fun / cmd_wholesome
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cmd_fun_lists_users(mocker, chat_commands, update, context):
    mocker.patch("src.commands.chat_commands.stats_utils.escape_special_characters", side_effect=lambda s: s)

    await chat_commands.cmd_fun(update, context)

    text = sent_text(context)
    assert "user_a" in text or "user_b" in text


@pytest.mark.asyncio
async def test_cmd_wholesome_lists_users(mocker, chat_commands, update, context):
    mocker.patch("src.commands.chat_commands.stats_utils.escape_special_characters", side_effect=lambda s: s)

    await chat_commands.cmd_wholesome(update, context)

    text = sent_text(context)
    assert "user_a" in text or "user_b" in text


# ---------------------------------------------------------------------------
# Chart commands — verify image sent
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "method_name",
    [
        pytest.param("cmd_funchart", id="funchart"),
        pytest.param("cmd_spamchart", id="spamchart"),
        pytest.param("cmd_likechart", id="likechart"),
    ],
)
async def test_chart_commands_send_image(mocker, chat_commands, update, context, method_name):
    mocker.patch("src.commands.chat_commands.charts.generate_plot", return_value="/fake/chart.png")
    mock_send = mocker.patch("src.commands.chat_commands.core_utils.send_message", new_callable=AsyncMock)

    await getattr(chat_commands, method_name)(update, context)

    mock_send.assert_awaited_once()
    assert mock_send.await_args.args[2] == MessageType.IMAGE


@pytest.mark.asyncio
async def test_cmd_monologuechart_sends_image(mocker, chat_commands, update, context):
    mocker.patch("src.commands.chat_commands.charts.generate_plot", return_value="/fake/chart.png")
    mock_send = mocker.patch("src.commands.chat_commands.core_utils.send_message", new_callable=AsyncMock)

    await chat_commands.cmd_monologuechart(update, context)

    mock_send.assert_awaited_once()
    assert mock_send.await_args.args[2] == MessageType.IMAGE


@pytest.mark.asyncio
async def test_cmd_relationship_graph_sends_image(mocker, chat_commands, update, context):
    mocker.patch("src.commands.chat_commands.charts.create_bidirectional_relationship_graph", return_value="/fake/graph.png")
    mock_send = mocker.patch("src.commands.chat_commands.core_utils.send_message", new_callable=AsyncMock)

    await chat_commands.cmd_relationship_graph(update, context)

    mock_send.assert_awaited_once()
    assert mock_send.await_args.args[2] == MessageType.IMAGE


@pytest.mark.asyncio
async def test_cmd_relationship_graph_empty_reactions(mocker, chat_commands, update, context, chat_df):
    empty_reactions = pd.DataFrame(columns=REACTIONS_COLS)
    ca = MagicMock(error="")
    mocker.patch.object(chat_commands, "preprocess_input", return_value=(chat_df.copy(), empty_reactions, ca))

    await chat_commands.cmd_relationship_graph(update, context)

    assert sent_text(context) == ErrorMessage.NO_DATA_FOR_PERIOD


# ---------------------------------------------------------------------------
# cmd_command_usage
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cmd_command_usage_lists_counts(mocker, chat_commands, update, context):
    mocker.patch("src.commands.chat_commands.stats_utils.escape_special_characters", side_effect=lambda s: s)

    await chat_commands.cmd_command_usage(update, context)

    text = sent_text(context)
    assert "summary" in text
    assert "fun" in text


@pytest.mark.asyncio
async def test_cmd_command_usage_chart_sends_image(mocker, chat_commands, update, context):
    mocker.patch("src.commands.chat_commands.charts.generate_plot", return_value="/fake/chart.png")
    mock_send = mocker.patch("src.commands.chat_commands.core_utils.send_message", new_callable=AsyncMock)

    await chat_commands.cmd_command_usage_chart(update, context)

    mock_send.assert_awaited_once()


@pytest.mark.asyncio
async def test_cmd_command_usage_chart_nonexistent_command(mocker, chat_commands, update, context):
    context.args = ["--command", "fake_cmd"]

    await chat_commands.cmd_command_usage_chart(update, context)

    text = sent_text(context)
    assert "does not exist" in text


# ---------------------------------------------------------------------------
# cmd_cwel
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cmd_cwel_no_reply(mocker, chat_commands, update, context):
    mocker.patch("src.commands.chat_commands.core_utils.parse_args", return_value=MagicMock(error=""))
    update.message.reply_to_message = None

    await chat_commands.cmd_cwel(update, context)

    assert sent_text(context) == ErrorMessage.CWEL_NO_REPLY


@pytest.mark.asyncio
async def test_cmd_cwel_bot_reply(mocker, chat_commands, update, context):
    mocker.patch("src.commands.chat_commands.core_utils.parse_args", return_value=MagicMock(error=""))
    reply = MagicMock()
    reply.from_user.id = 0  # will be patched
    update.message.reply_to_message = reply

    with patch("src.commands.chat_commands.BOT_ID", reply.from_user.id):
        await chat_commands.cmd_cwel(update, context)

    assert sent_text(context) == ErrorMessage.CWEL_BOT


@pytest.mark.asyncio
async def test_cmd_cwel_self_cwel(mocker, chat_commands, update, context):
    mocker.patch("src.commands.chat_commands.core_utils.parse_args", return_value=MagicMock(error=""))
    reply = MagicMock()
    reply.from_user.id = 111
    reply.message_id = 1
    update.message.reply_to_message = reply
    update.message.from_user.id = 111

    await chat_commands.cmd_cwel(update, context)

    assert sent_text(context) == ErrorMessage.CWEL_SELF


@pytest.mark.asyncio
async def test_cmd_cwel_happy_path(mocker, chat_commands, update, context):
    mocker.patch("src.commands.chat_commands.core_utils.parse_args", return_value=MagicMock(error=""))
    mocker.patch("src.commands.chat_commands.stats_utils.update_cwel_stats", return_value=chat_commands.cwel_stats_df)
    mocker.patch("src.commands.chat_commands.stats_utils.escape_special_characters", side_effect=lambda s: s)
    reply = MagicMock()
    reply.from_user.id = 222
    reply.message_id = 5
    reply.date = pd.Timestamp("2025-01-12 10:00", tz=TIMEZONE)
    update.message.reply_to_message = reply
    update.message.from_user.id = 111

    await chat_commands.cmd_cwel(update, context)

    text = sent_text(context)
    assert "cwel" in text.lower()
    assert "user_a" in text
    assert "user_b" in text


# ---------------------------------------------------------------------------
# cmd_topcwel
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cmd_topcwel_lists_rankings(mocker, chat_commands, update, context):
    mocker.patch("src.commands.chat_commands.stats_utils.escape_special_characters", side_effect=lambda s: s)

    await chat_commands.cmd_topcwel(update, context)

    text = sent_text(context)
    assert "user_a" in text


# ---------------------------------------------------------------------------
# cmd_remind
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cmd_remind_dt_error(mocker, chat_commands, update, context):
    context.args = ["1h", "user_a", "hello"]
    ca = MagicMock(error="", user_id=111)
    mocker.patch("src.commands.chat_commands.core_utils.parse_args", return_value=ca)
    mocker.patch("src.commands.chat_commands.core_utils.period_offset_to_dt", return_value=(None, "bad_dt"))
    mocker.patch("src.commands.chat_commands.stats_utils.get_last_message_id_of_a_user", return_value=(1, ""))

    await chat_commands.cmd_remind(update, context)

    assert "bad_dt" in sent_text(context)


@pytest.mark.asyncio
async def test_cmd_remind_happy_path(mocker, chat_commands, update, context):
    context.args = ["1h", "user_a", "hello"]
    ca = MagicMock(error="", user="user_a", user_id=111, string="hello")
    mocker.patch("src.commands.chat_commands.core_utils.parse_args", return_value=ca)
    mocker.patch("src.commands.chat_commands.core_utils.period_offset_to_dt", return_value=("DT", ""))
    mocker.patch("src.commands.chat_commands.stats_utils.get_last_message_id_of_a_user", return_value=(1, ""))
    mocker.patch("src.commands.chat_commands.core_utils.dt_to_pretty_str", return_value="pretty_dt")

    await chat_commands.cmd_remind(update, context)

    chat_commands.job_persistance.save_job.assert_called_once()
    assert "pretty_dt" in sent_text(context)


# ---------------------------------------------------------------------------
# cmd_wordstats
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cmd_wordstats_dispatches_to_handler(mocker, chat_commands, update, context):
    ca = MagicMock(error="", named_args={})
    mocker.patch("src.commands.chat_commands.core_utils.parse_args", return_value=ca)
    chat_commands.word_stats.filter_ngrams.return_value = {}
    chat_commands.word_stats.wordstats_cmd_handler.return_value = "result_text"

    await chat_commands.cmd_wordstats(update, context)

    chat_commands.word_stats.wordstats_cmd_handler.assert_called_once()
    assert sent_text(context) == "result_text"


# ---------------------------------------------------------------------------
# cmd_play
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cmd_play_download_error(mocker, chat_commands, update, context):
    ca = MagicMock(error="", string="some_url")
    mocker.patch("src.commands.chat_commands.core_utils.parse_args", return_value=ca)
    chat_commands.ytdl.download.return_value = (None, "download_failed")

    await chat_commands.cmd_play(update, context)

    assert sent_text(context) == "download_failed"


@pytest.mark.asyncio
async def test_cmd_play_full_flag_sends_voice(mocker, chat_commands, update, context):
    ca = MagicMock(error="", string="url", named_args={"full": None})
    mocker.patch("src.commands.chat_commands.core_utils.parse_args", return_value=ca)
    chat_commands.ytdl.download.return_value = ("/audio.mp3", "")
    mock_send = mocker.patch("src.commands.chat_commands.core_utils.send_message", new_callable=AsyncMock)

    await chat_commands.cmd_play(update, context)

    mock_send.assert_awaited_once()
    assert mock_send.await_args.args[2] == MessageType.VOICE


@pytest.mark.asyncio
async def test_cmd_play_default_video_note(mocker, chat_commands, update, context):
    ca = MagicMock(error="", string="url", named_args={})
    mocker.patch("src.commands.chat_commands.core_utils.parse_args", return_value=ca)
    chat_commands.ytdl.download.return_value = ("/audio.mp3", "")
    chat_commands.ytdl.swap_video_audio.return_value = "/output.mp4"
    mocker.patch("src.commands.chat_commands.stats_utils.get_random_media_path", return_value="/random.mp4")
    mock_send = mocker.patch("src.commands.chat_commands.core_utils.send_message", new_callable=AsyncMock)
    update.message.reply_to_message = None

    await chat_commands.cmd_play(update, context)

    mock_send.assert_awaited_once()
    assert mock_send.await_args.args[2] == MessageType.VIDEO_NOTE


# ---------------------------------------------------------------------------
# cmd_media_by_reactions
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cmd_media_by_reactions_sends_media(mocker, chat_commands, update, context):
    mocker.patch("src.commands.chat_commands.stats_utils.dt_to_str", return_value="10.01.2025")
    mocker.patch("src.commands.chat_commands.core_utils.message_id_to_path", return_value="/path/to/media")
    mocker.patch("src.commands.chat_commands.core_utils.send_message", new_callable=AsyncMock)

    await chat_commands.cmd_media_by_reactions(update, context, MessageType.VIDEO, EmojiType.ALL)

    assert context.bot.send_message.await_count >= 1


# ---------------------------------------------------------------------------
# Pure methods
# ---------------------------------------------------------------------------


def test_calculate_fun_metric(chat_commands, chat_df, reactions_df):
    result = chat_commands.calculate_fun_metric(chat_df, reactions_df)

    assert "final_username" in result.columns
    assert "ratio" in result.columns
    assert len(result) > 0
    assert result.iloc[0]["ratio"] > 0


def test_calculate_wholesome_metric(chat_commands, reactions_df):
    result = chat_commands.calculate_wholesome_metric(reactions_df)

    assert "reacting_username" in result.columns
    assert "ratio" in result.columns
    assert len(result) > 0


def test_get_reply_message_type_found(chat_commands):
    result = chat_commands.get_reply_message_type(7)
    assert result == MessageType.VIDEO


def test_get_reply_message_type_not_found(chat_commands):
    result = chat_commands.get_reply_message_type(9999)
    assert result is None


# ---------------------------------------------------------------------------
# update() — incremental merge
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_update_noop_when_no_ids(chat_commands):
    """update() returns immediately without mutating DataFrames when nothing changed."""
    original_chat_len = len(chat_commands.chat_df)
    original_reactions_len = len(chat_commands.reactions_df)

    await chat_commands.update()

    assert len(chat_commands.chat_df) == original_chat_len
    assert len(chat_commands.reactions_df) == original_reactions_len


@pytest.mark.asyncio
async def test_update_merges_new_rows(chat_commands, chat_df, reactions_df):
    """update() replaces rows for changed message IDs and appends genuinely new ones."""
    new_row_id = 99
    new_chat_row = pd.DataFrame(
        [(new_row_id, pd.Timestamp("2025-01-12 10:00", tz=TIMEZONE), 111, "user_a", "brand new", None, [], [], "text")],
        columns=CHAT_COLS,
    )
    new_reaction_row = pd.DataFrame(
        [(new_row_id, pd.Timestamp("2025-01-12 10:01", tz=TIMEZONE), "user_a", "user_b", "brand new", "👍")],
        columns=REACTIONS_COLS,
    )

    # Inject synthetic data into FakeDB so load_rows_by_message_ids returns it
    chat_commands.db._tables[Table.CLEANED_CHAT_HISTORY] = pd.concat([chat_df, new_chat_row], ignore_index=True)
    chat_commands.db._tables[Table.REACTIONS] = pd.concat([reactions_df, new_reaction_row], ignore_index=True)
    chat_commands.db._pending_ids = [new_row_id]

    await chat_commands.update()

    assert new_row_id in chat_commands.chat_df["message_id"].values
    assert new_row_id in chat_commands.reactions_df["message_id"].values
    # Existing rows should still be present
    assert 1 in chat_commands.chat_df["message_id"].values
