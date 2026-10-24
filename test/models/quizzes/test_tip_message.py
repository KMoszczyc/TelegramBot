import pytest
from telegram import InlineKeyboardMarkup

from src.models.quizzes import tip_message
from src.models.quizzes.capital_quiz import CapitalQuiz
from src.models.quizzes.flag_quiz import FlagQuiz
from src.models.quizzes.map_quiz import MapQuiz


@pytest.mark.parametrize(
    "user_id, expected",
    [(111, "quiztip:111"), (0, "quiztip:0"), (987654321, "quiztip:987654321")],
)
def test_build_tip_callback_data(user_id, expected):
    assert tip_message.build_tip_callback_data(user_id) == expected


@pytest.mark.parametrize(
    "data, expected",
    [
        ("quiztip:111", 111),
        ("quiztip:0", 0),
        ("quiztip:", None),
        ("quiztip:abc", None),
        ("other:111", None),
        ("", None),
        (None, None),
    ],
)
def test_parse_tip_callback_data(data, expected):
    assert tip_message.parse_tip_callback_data(data) == expected


def test_format_tip_line():
    assert tip_message.format_tip_line(2, 4, 1250, 50, "First letter: W") == "💡 Tip 2/4 - [1250 credits, -50%]: First letter: W"


@pytest.mark.parametrize(
    "quiz_class, difficulty, continent_specified, tips_given, total_tips, expected",
    [
        (CapitalQuiz, "easy", False, 0, 4, "💡 Tip 1/4 (reward → 1250)"),
        (CapitalQuiz, "easy", False, 1, 4, "💡 Tip 2/4 (reward → 937)"),
        (CapitalQuiz, "easy", True, 0, 3, "💡 Tip 1/3 (reward → 625)"),
        (FlagQuiz, "medium", False, 0, 3, "💡 Tip 1/3 (reward → 1500)"),
        (MapQuiz, "easy", False, 0, 3, "💡 Tip 1/3 (reward → 500)"),
        (MapQuiz, "easy", True, 0, 3, "💡 Tip 1/3 (reward → 250)"),
        (CapitalQuiz, "easy", False, 4, 4, None),
        (CapitalQuiz, "easy", False, 5, 4, None),
    ],
)
def test_build_tip_label(quiz_class, difficulty, continent_specified, tips_given, total_tips, expected):
    assert tip_message.build_tip_label(quiz_class, difficulty, continent_specified, tips_given, total_tips) == expected


@pytest.mark.parametrize("label, expected_rows", [("💡 Tip 1/3", 1), (None, 0)])
def test_build_tip_keyboard(label, expected_rows):
    keyboard = tip_message.build_tip_keyboard(111, label)
    assert isinstance(keyboard, InlineKeyboardMarkup)
    assert len(keyboard.inline_keyboard) == expected_rows
    if label:
        button = keyboard.inline_keyboard[0][0]
        assert button.text == label
        assert button.callback_data == "quiztip:111"


@pytest.mark.parametrize(
    "base_text, tip_lines, limit, expected",
    [
        ("Country: *Polska*", [], 100, "Country\\: *Polska*"),
        ("Title!", [], 100, "Title\\!"),
        ("Country: *Polska*", ["Tip 1/3: a"], 100, "Country\\: *Polska*\n\nTip 1/3\\: a"),
        ("Q", ["T1", "T2"], 100, "Q\n\nT1\nT2"),
        ("Q", ["a.b"], 100, "Q\n\na\\.b"),
        ("Q", ["x" * 50], 10, None),
        ("Q", ["x" * 5], 8, "Q\n\nxxxxx"),
        ("Q", ["x" * 5], 7, None),
        ("a.b", [], 3, None),
    ],
)
def test_compose_quiz_text(base_text, tip_lines, limit, expected):
    assert tip_message.compose_quiz_text(base_text, tip_lines, limit) == expected


@pytest.fixture
def country():
    return {"country_name": "Polska", "country_code": "PL", "continent": "Europa", "capital": "Warszawa"}


def test_reveal_next_tip_walks_through_all_tips(country):
    cached = {"difficulty": "easy", "continent_specified": False, "tips_given": 0}
    quiz = CapitalQuiz()

    lines = [tip_message.reveal_next_tip(cached, quiz, CapitalQuiz, country) for _ in range(5)]

    assert lines[0] == "💡 Tip 1/4 - [1250 credits, -50%]: Continent: Europa"
    assert lines[1] == "💡 Tip 2/4 - [937 credits, -63%]: First letter: W"
    assert lines[2].endswith("Second letter: a")
    assert lines[3].endswith("Next two letters: rs")
    assert lines[4] is None
    assert cached["tips_given"] == 4
    assert cached["tip_lines"] == lines[:4]


@pytest.mark.parametrize(
    "continent_specified, expected_total",
    [(False, 4), (True, 3)],
)
def test_reveal_next_tip_respects_continent_specified(country, continent_specified, expected_total):
    cached = {"difficulty": "easy", "continent_specified": continent_specified}
    quiz = CapitalQuiz()
    revealed = []
    while (line := tip_message.reveal_next_tip(cached, quiz, CapitalQuiz, country)) is not None:
        revealed.append(line)
    assert len(revealed) == expected_total
    assert all(f"/{expected_total}" in line for line in revealed)


def test_reveal_next_tip_map_quiz():
    person = {"name_pl": "Maria Skłodowska", "description": "Maria Skłodowska - wybitna polska fizyk. Zdobyła Nagrodę Nobla."}
    cached = {"difficulty": "easy", "category_specified": True, "tips_given": 0}
    quiz = MapQuiz()

    line = tip_message.reveal_next_tip(cached, quiz, MapQuiz, person)
    assert line is not None
    assert "Tip 1/" in line
    assert cached["tips_given"] == 1
    assert cached["tip_lines"] == [line]


def test_build_initial_tip_keyboard():
    keyboard = tip_message.build_initial_tip_keyboard(CapitalQuiz, user_id=123, difficulty="easy", filter_specified=False, total_tips=4)
    assert len(keyboard.inline_keyboard) == 1
    assert "Tip 1/4" in keyboard.inline_keyboard[0][0].text
    assert keyboard.inline_keyboard[0][0].callback_data == "quiztip:123"


@pytest.mark.parametrize(
    "has_photo, base_text_len, expected_can_edit",
    [
        (True, 10, True),
        (True, 1500, False),  # exceeds caption limit 1024
        (False, 2000, True),  # fits message limit 4096
        (False, 5000, False),  # exceeds message limit 4096
    ],
)
def test_plan_tip_display_edit_vs_overflow(has_photo, base_text_len, expected_can_edit):
    country = {"country_name": "Polska", "country_code": "PL", "continent": "Europa", "capital": "Warszawa"}
    cached = {
        "chat_id": 999,
        "thread_id": 42,
        "message_id": 777,
        "has_photo": has_photo,
        "base_text": "x" * base_text_len,
        "tips_given": 1,
        "tip_lines": ["💡 Tip 1/4: Europa"],
        "country": country,
        "difficulty": "easy",
        "continent_specified": False,
    }
    quiz = CapitalQuiz()
    tip_line = "💡 Tip 2/4: W"
    plan = tip_message.plan_tip_display(cached, quiz, CapitalQuiz, user_id=123, tip_line=tip_line)

    assert plan.can_edit == expected_can_edit
    assert plan.chat_id == 999
    assert plan.thread_id == 42
    assert plan.message_id == 777
    assert plan.has_photo == has_photo
    if expected_can_edit:
        assert plan.edit_text is not None
    else:
        assert plan.edit_text is None
    assert "Tip 2/4" in plan.overflow_text
