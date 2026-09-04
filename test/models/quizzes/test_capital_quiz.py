import pytest

from src.models.quizzes.capital_quiz import CapitalQuiz


@pytest.fixture
def mock_country_dict():
    return {
        "country_name": "Polska Rzeczpospolita",
        "country_code": "PL",
        "continent": "Europa",
        "population": 38000000,
        "capital": "Warszawa",
    }


@pytest.mark.parametrize(
    "difficulty, continent_specified, tips_given",
    [
        ("easy", False, 0),
        ("medium", False, 0),
        ("hard", False, 0),
        ("crazy", False, 0),
        ("crazy", True, 0),
        ("easy", True, 0),
        ("crazy", False, 1),
        ("crazy", False, 2),
        ("crazy", False, 3),
        ("crazy", True, 1),
    ],
)
def test_get_reward(difficulty, continent_specified, tips_given):
    base_reward = CapitalQuiz.REWARD_LEVELS.get(difficulty, CapitalQuiz.REWARD_LEVELS.get("crazy", 0))
    if continent_specified:
        base_reward = base_reward // 2

    if tips_given == 0:
        expected_reward = base_reward
        expected_decrease_pct = 0
    else:
        expected_reward = int(base_reward * 0.5 * (0.75 ** (tips_given - 1)))
        expected_decrease_pct = int(round((base_reward - expected_reward) / base_reward * 100)) if base_reward > 0 else 0

    reward, decrease = CapitalQuiz.get_reward(difficulty, continent_specified, tips_given)
    assert reward == expected_reward
    assert decrease == expected_decrease_pct


def test_get_capital_display_name(mock_country_dict):
    assert CapitalQuiz().get_display_name(mock_country_dict) == "Warszawa"


def test_get_country_description(mock_country_dict):
    desc = CapitalQuiz().get_description(mock_country_dict)
    assert "Capital: Warszawa" in desc
    assert "Continent: Europa" in desc
    assert "Population: 38,000,000" in desc


@pytest.mark.parametrize(
    "continent_specified, expected_tips",
    [
        (False, ["Country: Polska Rzeczpospolita", "First letter: W", "Second letter: a"]),
        (True, ["Country: Polska Rzeczpospolita", "First letter: W", "Second letter: a"]),
    ],
)
def test_get_tips(mock_country_dict, continent_specified, expected_tips):
    assert CapitalQuiz().get_tips(mock_country_dict, continent_specified) == expected_tips


def test_get_valid_answers(mock_country_dict):
    expected_answers = {"warszawa"}
    assert CapitalQuiz().get_valid_answers(mock_country_dict) == expected_answers


def test_get_valid_answers_multi_word():
    country = {"capital": "Washington D.C."}
    expected_answers = {"washington d.c.", "washington", "d.c."}
    assert CapitalQuiz().get_valid_answers(country) == expected_answers


@pytest.mark.parametrize(
    "user_answer, is_correct",
    [
        ("warszawa", True),
        ("Warszawa", True),
        ("warszaw", True),  # Fuzzy 1
        ("berlin", False),
    ],
)
def test_is_answer_correct(user_answer, is_correct):
    valid_answers = {"warszawa"}
    assert CapitalQuiz.is_answer_correct(user_answer, valid_answers) == is_correct
