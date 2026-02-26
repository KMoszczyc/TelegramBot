from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest

from src.commands.misc_commands import Commands
from src.config.enums import ErrorMessage, HolyTextType, Table
from src.models.bot_state import BotState


@pytest.fixture()
def users_df():
    data = [("A", None, "a", "user_a"), ("B", None, "b", "user_b")]
    return pd.DataFrame(data, columns=["first_name", "last_name", "username", "final_username"], index=[111, 222])


# fmt: off
@pytest.fixture()
def bible_df():
    data = [
        ("Rdz", "Księga Rodzaju", 1, "1", "Na początku Bóg stworzył niebo i ziemię."),
        ("Rdz", "Księga Rodzaju", 1, "2", "Ziemia zaś była bezładem i pustkowiem: ciemność była nad powierzchnią bezmiaru wód, a Duch Boży unosił się nad wodami."),
        ("Rdz", "Księga Rodzaju", 1, "3", "Wtedy Bóg rzekł: Niechaj się stanie światłość! I stała się światłość."),
        ("Pwt", "Księga Powtórzonego Prawa", 27, "7", "Jemu złożycie też ofiary biesiadne, spożyjecie je na miejscu i będziecie się cieszyć wobec Pana, Boga swego."),
        ("Ps", "Księga Psalmów", 27, "6", "Już teraz głowa moja się podnosi nad nieprzyjaciół, co wokół mnie stoją. Złożę w Jego przybytku ofiary radości, zaśpiewam i zagram Panu."),
        ("2 Krl", "2 Księga Królewska", 21, "7",
         "Posąg Aszery, który sporządził, postawił w świątyni, o której Pan powiedział do Dawida i do syna jego, Salomona: W świątyni tej i w Jeruzalem, które wybrałem ze wszystkich pokoleń Izraela, kładę moje Imię na wieki."),
        ("Mt", "Ewangelia Mateusza", 11, "11", "Zaprawdę, powiadam wam: Między narodzonymi z niewiast nie powstał większy od Jana Chrzciciela. Lecz najmniejszy w królestwie niebieskim większy jest niż on.")
    ]
    return pd.DataFrame(data, columns=["abbreviation", "book", "chapter", "verse", "text"])


@pytest.fixture()
def quran_df():
    data = [
        (24, "Światło", "10", "I gdyby nie łaska i miłosierdzie Boga nad wami, i gdyby Bóg nie był Przebaczający, Mądry!"),
        (34, "Saba", "9",
         "Czy oni nie widzą tego, co jest przed nimi i za nimi, z nieba i ziemi? Jeśli zechcemy, to sprawimy, że pochłonie ich ziemia albo spadnie na nich kawałek nieba. Zaprawdę, w tym jest znak dla każdego sługi okazującego skruchę."),
        (34, "Saba", "45", "I ci, którzy byli przed nimi, odrzucili prawdę. A ci nie otrzymali nawet dziesiątej części tego, cośmy tamtym dali, i uznali za kłamców Moich posłańców. Jakże było Moje oburzenie!"),
        (114, "Ludzie", "1", "Powiedz: „Szukam schronienia u Pana ludzi,"),
        (114, "Ludzie", "2", "Władcy ludzi,"),
        (114, "Ludzie", "3-6", "Boga ludzi, przed złem szeptów upadłego, który szepcze w piersi ludzi, spośród dżinnów i ludzi!”")
    ]
    return pd.DataFrame(data, columns=["chapter_nr", "chapter_name", "verse", "text"])


# fmt: on


@pytest.fixture()
def db(users_df):
    class FakeDB:
        def __init__(self, users_df):
            self._tables = {Table.USERS: users_df}

        def load_table(self, table):
            return self._tables[table]

    return FakeDB(users_df)


@pytest.fixture()
def command_logger():
    return MagicMock()


@pytest.fixture()
def job_persistance():
    return MagicMock()


@pytest.fixture()
def bot_state():
    bs = MagicMock(spec=BotState)
    bs.last_bible_verse_id = -1
    bs.last_quran_verse_id = -1
    bs.set_holy_text_last_verse_id = MagicMock()
    return bs


@pytest.fixture()
def assets(bible_df, quran_df):
    a = MagicMock()
    a.bible_df = bible_df
    a.quran_df = quran_df
    a.ozjasz_phrases = ["Fajansen, moansen", "Guten tag", "Szanuje"]
    a.boczek_phrases = ["curse_one", "curse_two"]
    a.europejskafirma_phrases = ["firma phrase"]
    a.bartosiak_phrases = ["bartosiak quote"]
    a.shopping_sundays = ["01-01-2024"]
    a.kiepscy_df = pd.DataFrame([{"nr": "1", "title": "t", "url": "u", "description": "d"}])
    a.tvp_headlines = ["Tusk powraca", "Kaczynski na wakacjach"]
    a.tvp_latest_headlines = ["Nowy podatek"]
    a.walesa_phrases = ["Ja to zrobilem"]
    a.commands = ["/help", "/ozjasz"]
    a.arguments_help = ["--all"]
    return a


@pytest.fixture()
def commands(command_logger, job_persistance, bot_state, db, assets):
    return Commands(command_logger, job_persistance, bot_state, db, assets)


@pytest.fixture()
def update():
    update = MagicMock()
    update.effective_chat.id = 999
    update.effective_user.id = 111
    update.message.message_thread_id = None
    update.message.message_id = 123
    return update


@pytest.fixture()
def context():
    ctx = MagicMock()
    ctx.args = []
    ctx.bot = MagicMock()
    ctx.bot.send_message = AsyncMock()
    ctx.job_queue = MagicMock()
    return ctx


@pytest.mark.asyncio
async def test_cmd_all_sends_markdown_mentions(commands, update, context):
    await commands.cmd_all(update, context)

    assert context.bot.send_message.await_count == 1
    kwargs = context.bot.send_message.await_args.kwargs
    assert kwargs["chat_id"] == update.effective_chat.id
    assert kwargs["parse_mode"] == "Markdown"
    # Both users should be mentioned
    assert "tg://user?id=111" in kwargs["text"]
    assert "tg://user?id=222" in kwargs["text"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "args, expected_text",
    [
        pytest.param([], "Fajansen, moansen", id="no_args_returns_random"),
        pytest.param(["guten"], "Guten tag", id="filter_matches"),
        pytest.param(["nonexistent_xyz"], ErrorMessage.NO_SUCH_PHRASE.value, id="no_matching_phrase"),
    ],
)
async def test_cmd_ozjasz_edge_cases(mocker, commands, update, context, args, expected_text):
    context.args = args
    mocker.patch("src.commands.misc_commands.core_utils.random.choice", side_effect=lambda seq: seq[0])

    await commands.cmd_ozjasz(update, context)

    kwargs = context.bot.send_message.await_args.kwargs
    assert kwargs["text"] == expected_text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "args, expected_prefix",
    [
        pytest.param([], "", id="no_text_arg"),
        pytest.param(["test"], "test to ", id="with_text"),
        pytest.param(['"two', 'words"'], "two words to ", id="quoted_multiword"),
    ],
)
async def test_cmd_boczek_uses_real_boczek_phrases_and_arg_parsing(mocker, commands, update, context, args, expected_prefix):
    context.args = args
    commands.assets.boczek_phrases = ["curse_one", "curse_two"]

    await commands.cmd_boczek(update, context)
    sent = context.bot.send_message.await_args.kwargs["text"]
    curse_part = sent[len(expected_prefix) :] if expected_prefix else sent

    assert sent.startswith(expected_prefix)
    assert curse_part in {"curse_one", "curse_two"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "method_name",
    [
        pytest.param("cmd_europejskafirma", id="europejskafirma"),
        pytest.param("cmd_bartosiak", id="bartosiak"),
        pytest.param("cmd_tvp", id="tvp"),
        pytest.param("cmd_tvp_latest", id="tvp_latest"),
        pytest.param("cmd_tusk", id="tusk"),
        pytest.param("cmd_walesa", id="walesa"),
    ],
)
async def test_phrase_commands_preprocess_error_short_circuits(mocker, commands, update, context, method_name):
    ca = MagicMock(error="err")
    mocker.patch("src.commands.misc_commands.core_utils.preprocess_input", return_value=([], ca))

    await getattr(commands, method_name)(update, context)

    kwargs = context.bot.send_message.await_args.kwargs
    assert kwargs["text"] == "err"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "method_name, error_message, expected_text, replace_newlines",
    [
        pytest.param("cmd_europejskafirma", ErrorMessage.NO_SUCH_PHRASE, "hello", False, id="europejskafirma"),
        pytest.param("cmd_bartosiak", ErrorMessage.NO_SUCH_PHRASE, "hello", False, id="bartosiak"),
        pytest.param("cmd_tvp", ErrorMessage.NO_SUCH_HEADLINE, "hello", False, id="tvp"),
        pytest.param("cmd_tvp_latest", ErrorMessage.NO_SUCH_HEADLINE, "hello", False, id="tvp_latest"),
        pytest.param("cmd_tusk", ErrorMessage.NO_SUCH_HEADLINE, "hello", False, id="tusk"),
        pytest.param("cmd_walesa", ErrorMessage.NO_SUCH_ITEM, "hello\nworld", True, id="walesa"),
    ],
)
async def test_phrase_commands_success(mocker, commands, update, context, method_name, error_message, expected_text, replace_newlines):
    ca = MagicMock(error="")
    expected = expected_text.replace("\n", r"\n") if replace_newlines else expected_text
    mocker.patch("src.commands.misc_commands.core_utils.preprocess_input", return_value=([expected], ca))
    mocker.patch("src.commands.misc_commands.core_utils.random.choice", side_effect=lambda seq: seq[0])

    await getattr(commands, method_name)(update, context)

    kwargs = context.bot.send_message.await_args.kwargs
    assert kwargs["text"] == expected_text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "method_name, error_message, expected_text, replace_newlines",
    [
        pytest.param("cmd_europejskafirma", ErrorMessage.NO_SUCH_PHRASE, "hello", False, id="europejskafirma"),
        pytest.param("cmd_bartosiak", ErrorMessage.NO_SUCH_PHRASE, "hello", False, id="bartosiak"),
        pytest.param("cmd_tvp", ErrorMessage.NO_SUCH_HEADLINE, "hello", False, id="tvp"),
        pytest.param("cmd_tvp_latest", ErrorMessage.NO_SUCH_HEADLINE, "hello", False, id="tvp_latest"),
        pytest.param("cmd_tusk", ErrorMessage.NO_SUCH_HEADLINE, "hello", False, id="tusk"),
        pytest.param("cmd_walesa", ErrorMessage.NO_SUCH_ITEM, "hello\nworld", True, id="walesa"),
    ],
)
async def test_phrase_commands_fallback(mocker, commands, update, context, method_name, error_message, expected_text, replace_newlines):
    ca = MagicMock(error="")
    mocker.patch("src.commands.misc_commands.core_utils.preprocess_input", return_value=([], ca))

    await getattr(commands, method_name)(update, context)

    kwargs = context.bot.send_message.await_args.kwargs
    assert kwargs["text"] == error_message.value


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "args, expected_args_provided",
    [
        pytest.param([], False, id="no_args"),
        pytest.param(["abc"], True, id="with_args"),
        pytest.param(["  "], True, id="spaces"),
    ],
)
async def test_cmd_are_you_lucky_today_args_provided(mocker, commands, update, context, args, expected_args_provided):
    context.args = args
    lucky = mocker.patch("src.commands.misc_commands.core_utils.are_you_lucky", return_value=(False, "resp"))

    await commands.cmd_are_you_lucky_today(update, context)

    kwargs = context.bot.send_message.await_args.kwargs
    assert lucky.call_args.args[1] == expected_args_provided
    assert kwargs["text"] == "resp"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "joined_args, filtered_commands, expected_in_text",
    [
        pytest.param("", [], "Arguments", id="no_args_shows_all"),
        pytest.param("help", ["help"], "/help", id="filter_some"),
        pytest.param("x", [], "/", id="filter_none_falls_back"),
    ],
)
async def test_cmd_help_variants(mocker, commands, update, context, joined_args, filtered_commands, expected_in_text):
    ca = MagicMock(error="", joined_args=joined_args, joined_args_lower=joined_args.lower())
    mocker.patch("src.commands.misc_commands.core_utils.preprocess_input", return_value=(filtered_commands, ca))
    # avoid markdown escaping complexity
    mocker.patch("src.commands.misc_commands.stats_utils.escape_special_characters", side_effect=lambda s: s)

    await commands.cmd_help(update, context)

    sent_texts = [c.kwargs["text"] for c in context.bot.send_message.await_args_list]
    assert any(expected_in_text in t for t in sent_texts)


@pytest.mark.asyncio
async def test_cmd_bible_parse_error_sends_error(mocker, commands, update, context, bot_state):
    mocker.patch("src.commands.misc_commands.core_utils.parse_args", return_value=MagicMock(error="bad"))

    await commands.cmd_bible(update, context, bot_state)

    kwargs = context.bot.send_message.await_args.kwargs
    assert kwargs["text"] == "bad"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "args, expected_substring",
    [
        pytest.param(["Bóg"], "[Rdz", id="default_random_match"),
        pytest.param(["Bóg", "--num", "1"], 'bible verses with "bóg"', id="num"),
        pytest.param(["Bóg", "--all"], 'bible verses with "bóg"', id="all"),
        pytest.param(["Bóg", "--count"], 'bible verses with "bóg"', id="count"),
        pytest.param(["Bóg", "--prev", "1"], "[Rdz", id="prev_without_last_falls_back_to_random"),
        pytest.param(["Bóg", "--next", "1"], "[Rdz", id="next_without_last_falls_back_to_random"),
        pytest.param(["Bóg", "-n", "1"], "after", id="next_alias_with_last"),
        pytest.param(["Bóg", "-p", "1"], "before", id="prev_alias_with_last"),
    ],
)
async def test_cmd_bible_named_args_variants(mocker, commands, update, context, bot_state, bible_df, args, expected_substring):
    context.args = args
    mocker.patch("src.core.utils.random.choice", side_effect=lambda seq: seq[0])
    mocker.patch("src.commands.misc_commands.core_utils.random.choice", side_effect=lambda seq: seq[0])
    if "-n" in args:
        bot_state.last_bible_verse_id = 1
    if "-p" in args:
        bot_state.last_bible_verse_id = 1

    await commands.cmd_bible(update, context, bot_state)

    sent = context.bot.send_message.await_args.kwargs["text"]
    assert expected_substring in sent


@pytest.mark.asyncio
async def test_cmd_quran_parse_error_sends_error(mocker, commands, update, context, bot_state):
    mocker.patch("src.commands.misc_commands.core_utils.parse_args", return_value=MagicMock(error="bad"))

    await commands.cmd_quran(update, context, bot_state)

    kwargs = context.bot.send_message.await_args.kwargs
    assert kwargs["text"] == "bad"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "args, expected_substring",
    [
        pytest.param(["Bóg"], "[", id="default_random_match"),
        pytest.param(["Bóg", "--num", "1"], 'quran verses with "bóg"', id="num"),
        pytest.param(["Bóg", "--all"], 'quran verses with "bóg"', id="all"),
        pytest.param(["Bóg", "--count"], 'quran verses with "bóg"', id="count"),
        pytest.param(["--ch", "__no_such_chapter__"], "Nie ma takiego wersetu", id="chapter_name_prefix_when_no_match"),
        pytest.param(["--verse", "34:9"], "[34:9]", id="verse_parse_by_chapter_nr"),
        pytest.param(["--verse", "saba:9"], "[34:9]", id="verse_parse_by_chapter_name"),
        pytest.param(["--verse", "x"], "Failed to parse", id="verse_parse_invalid"),
    ],
)
async def test_cmd_quran_named_args_variants(mocker, commands, update, context, bot_state, quran_df, args, expected_substring):
    context.args = args
    mocker.patch("src.core.utils.random.choice", side_effect=lambda seq: seq[0])
    mocker.patch("src.commands.misc_commands.core_utils.random.choice", side_effect=lambda seq: seq[0])

    await commands.cmd_quran(update, context, bot_state)

    sent = context.bot.send_message.await_args.kwargs["text"]
    assert expected_substring in sent


@pytest.mark.parametrize(
    "named_args, last_bible_verse_id, last_quran_verse_id, holy_text_type, expected_substring",
    [
        pytest.param({}, -1, -1, "bible", "[", id="bible_random"),
        pytest.param({"all": None}, -1, -1, "bible", 'bible verses with "x"', id="bible_all"),
        pytest.param({"num": 1}, -1, -1, "bible", 'bible verses with "x"', id="bible_num"),
        pytest.param({"count": None}, -1, -1, "bible", 'bible verses with "x"', id="bible_count"),
        pytest.param({"prev": 1}, 2, -1, "bible", "before", id="bible_prev"),
        pytest.param({"next": 1}, 2, -1, "bible", "after", id="bible_next"),
        pytest.param({"verse": "34:9"}, -1, -1, "quran", "[sig]", id="quran_verse"),
        pytest.param({"verse": "x"}, -1, -1, "quran", "Failed to parse", id="quran_verse_invalid"),
        pytest.param({}, -1, -1, "quran", "[", id="quran_random"),
        pytest.param({"all": None}, -1, -1, "quran", 'quran verses with "x"', id="quran_all"),
        pytest.param({"num": 1}, -1, -1, "quran", 'quran verses with "x"', id="quran_num"),
        pytest.param({"count": None}, -1, -1, "quran", 'quran verses with "x"', id="quran_count"),
        pytest.param({}, -1, -1, "bible_empty_filtered", "Nie ma takiego wersetu", id="empty_filtered"),
    ],
)
def test_handle_holy_text_named_params_branches(
    mocker,
    commands,
    bot_state,
    bible_df,
    quran_df,
    named_args,
    last_bible_verse_id,
    last_quran_verse_id,
    holy_text_type,
    expected_substring,
):
    bot_state.last_bible_verse_id = last_bible_verse_id
    bot_state.last_quran_verse_id = last_quran_verse_id
    ca = MagicMock(named_args=named_args)
    mocker.patch("src.commands.misc_commands.core_utils.get_siglum", return_value="sig")

    if holy_text_type.startswith("bible"):
        raw_df = bible_df.copy().reset_index(drop=True)
        filtered_df = raw_df.copy() if holy_text_type != "bible_empty_filtered" else raw_df.iloc[0:0]
        ht = HolyTextType.BIBLE
    else:
        raw_df = quran_df.copy().reset_index(drop=True)
        filtered_df = raw_df.copy()
        ht = HolyTextType.QURAN

    resp, err = commands.handle_holy_text_named_params(ca, filtered_df, raw_df, bot_state, "x", ht)
    assert err == "" or "Failed" in err or "doesn't exist" in err
    assert expected_substring in resp or expected_substring in err


@pytest.mark.parametrize(
    "args",
    [
        pytest.param(["--num"], id="num_missing_value"),
        pytest.param(["--prev"], id="prev_missing_value"),
        pytest.param(["--next"], id="next_missing_value"),
        pytest.param(["--chapter"], id="chapter_missing_value"),
        pytest.param(["--book"], id="book_missing_value"),
        pytest.param(["--num", "-1"], id="num_invalid_negative"),
        pytest.param(["--num", "x"], id="num_invalid_non_numeric"),
    ],
)
@pytest.mark.asyncio
async def test_cmd_bible_named_arg_parse_errors_are_reported(mocker, commands, update, context, bot_state, bible_df, args):
    context.args = args

    await commands.cmd_bible(update, context, bot_state)

    sent = context.bot.send_message.await_args.kwargs["text"]
    assert len(sent) > 5, f"Expected a meaningful error message, got: {sent!r}"


@pytest.mark.parametrize(
    "args",
    [
        pytest.param(["--num"], id="num_missing_value"),
        pytest.param(["--prev"], id="prev_missing_value"),
        pytest.param(["--next"], id="next_missing_value"),
        pytest.param(["--chapter"], id="chapter_missing_value"),
        pytest.param(["--verse"], id="verse_missing_value"),
        pytest.param(["--num", "-1"], id="num_invalid_negative"),
        pytest.param(["--num", "x"], id="num_invalid_non_numeric"),
    ],
)
@pytest.mark.asyncio
async def test_cmd_quran_named_arg_parse_errors_are_reported(mocker, commands, update, context, bot_state, quran_df, args):
    context.args = args

    await commands.cmd_quran(update, context, bot_state)

    sent = context.bot.send_message.await_args.kwargs["text"]
    assert len(sent) > 5, f"Expected a meaningful error message, got: {sent!r}"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "named_args, dt_now, shopping_sundays, expected",
    [
        pytest.param({}, "01-01-2024", ["01-01-2024"], "Dziś niedziela handlowa", id="today_is_sunday"),
        pytest.param({}, "01-01-2024", ["08-01-2024"], "Kolejna handlowa niedziela jest", id="next_sunday"),
        pytest.param({}, "31-12-2024", ["01-01-2024"], "Nie ma już handlowych", id="none_left"),
        pytest.param({"all": None}, "01-01-2024", ["01-01-2024", "08-01-2024"], "Wszystkie handlowe niedziele", id="all"),
    ],
)
async def test_cmd_show_shopping_sundays_variants(mocker, commands, update, context, named_args, dt_now, shopping_sundays, expected):
    from datetime import datetime

    ca = MagicMock(error="", named_args=named_args)
    mocker.patch("src.commands.misc_commands.core_utils.parse_args", return_value=ca)
    commands.assets.shopping_sundays = shopping_sundays
    mocker.patch("src.commands.misc_commands.datetime", wraps=datetime)
    mocker.patch("src.commands.misc_commands.datetime.now", return_value=datetime.strptime(dt_now, "%d-%m-%Y"))
    mocker.patch("src.commands.misc_commands.core_utils.display_shopping_sunday", return_value="X")

    await commands.cmd_show_shopping_sundays(update, context)

    assert expected in context.bot.send_message.await_args.kwargs["text"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "parse_error, period_error",
    [
        pytest.param("bad", "", id="parse_error"),
        pytest.param("", "period_bad", id="period_error"),
    ],
)
async def test_cmd_remind_me_errors(mocker, commands, update, context, parse_error, period_error):
    ca = MagicMock(error=parse_error, string="msg")
    mocker.patch("src.commands.misc_commands.core_utils.parse_args", return_value=ca)
    mocker.patch("src.commands.misc_commands.core_utils.period_offset_to_dt", return_value=(None, period_error))

    await commands.cmd_remind_me(update, context)

    assert context.bot.send_message.await_args.kwargs["text"] in {"bad", "period_bad"}


@pytest.mark.asyncio
async def test_cmd_remind_me_success_saves_job(mocker, commands, update, context):
    ca = MagicMock(error="", string="msg")
    mocker.patch("src.commands.misc_commands.core_utils.parse_args", return_value=ca)
    mocker.patch("src.commands.misc_commands.core_utils.period_offset_to_dt", return_value=("DT", ""))
    mocker.patch("src.commands.misc_commands.core_utils.dt_to_pretty_str", return_value="pretty")

    await commands.cmd_remind_me(update, context)

    commands.job_persistance.save_job.assert_called_once()
    assert "pretty" in context.bot.send_message.await_args.kwargs["text"]


@pytest.mark.asyncio
async def test_cmd_kiepscyurl_no_episode_sends_error(mocker, commands, update, context):
    ca = MagicMock(error="", number=999)
    mocker.patch("src.commands.misc_commands.core_utils.parse_args", return_value=ca)
    df = pd.DataFrame([{"nr": "1", "title": "t", "url": "u", "description": "d"}])
    commands.assets.kiepscy_df = df

    await commands.cmd_kiepscyurl(update, context)

    assert "Nie ma takiego epizodu" in context.bot.send_message.await_args.kwargs["text"]


@pytest.mark.asyncio
async def test_cmd_kiepscyurl_happy_path(mocker, commands, update, context):
    ca = MagicMock(error="", number=1)
    mocker.patch("src.commands.misc_commands.core_utils.parse_args", return_value=ca)
    mocker.patch("src.commands.misc_commands.stats_utils.escape_special_characters", side_effect=lambda s: s)

    await commands.cmd_kiepscyurl(update, context)

    sent = context.bot.send_message.await_args.kwargs["text"]
    assert "t" in sent
    assert "u" in sent


@pytest.mark.asyncio
async def test_cmd_bible_stats(mocker, commands, update, context):
    mocker.patch("src.commands.misc_commands.stats_utils.escape_special_characters", side_effect=lambda s: s)

    await commands.cmd_bible_stats(update, context)

    kwargs = context.bot.send_message.await_args.kwargs
    assert "Bible stats" in kwargs["text"]
    assert "Rdz" in kwargs["text"]
    assert "Ps" in kwargs["text"]


@pytest.mark.asyncio
async def test_cmd_kiepscy_search(mocker, commands, update, context):
    context.args = ["t"]
    mocker.patch("src.commands.misc_commands.stats_utils.escape_special_characters", side_effect=lambda s: s)

    await commands.cmd_kiepscy(update, context)

    sent = context.bot.send_message.await_args.kwargs["text"]
    assert "t" in sent
