import pandas as pd
import pytest

from src.models.quizzes.capital_quiz import CapitalQuiz
from src.models.quizzes.flag_quiz import FlagQuiz


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
        (False, ["Continent: Europa", "First letter: W", "Second letter: a", "Next two letters: rs"]),
        (True, ["First letter: W", "Second letter: a", "Next two letters: rs"]),
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


@pytest.mark.parametrize(
    "country, expected_prompt",
    [
        ({"country_name": "Polska", "capital": "Warszawa"}, "Country: *Polska*"),
        ({"country_name": "Stany Zjednoczone", "capital": "Waszyngton"}, "Country: *Stany Zjednoczone*"),
        (pd.Series({"country_name": "Niemcy", "capital": "Berlin"}), "Country: *Niemcy*"),
    ],
)
def test_get_prompt(country, expected_prompt):
    assert CapitalQuiz().get_prompt(country) == expected_prompt


@pytest.mark.parametrize(
    "country",
    [
        {"country_name": "Polska", "country_code": "PL", "capital": "Warszawa"},
        pd.Series({"country_name": "Niemcy", "country_code": "DE", "capital": "Berlin"}),
    ],
)
def test_get_image_path_is_none(country):
    assert CapitalQuiz().get_image_path(country) is None


def test_flag_quiz_still_has_image(mock_country_dict):
    assert FlagQuiz().get_image_path(mock_country_dict).endswith("PL.jpg")
    assert FlagQuiz().get_prompt(mock_country_dict) == ""


@pytest.mark.parametrize(
    "capital, expected_last_tip",
    [
        ("Berlin", "Next two letters: rl"),
        ("Rome", "Next two letters: me"),
        ("Oslo", "Next two letters: lo"),
        ("Lima", "Next two letters: ma"),
        ("Bern", "Next two letters: rn"),
        ("Abu", "Next two letters: u"),
        ("Ab", "Next two letters: (none)"),
        ("A", "Next two letters: (none)"),
        ("", "Next two letters: (none)"),
    ],
)
def test_get_tips_next_two_letters(capital, expected_last_tip):
    country = {"continent": "Europa", "capital": capital}
    assert CapitalQuiz().get_tips(country)[-1] == expected_last_tip


def test_capital_quiz_prompt_and_outcome_formatting(mock_country_dict):
    quiz = CapitalQuiz()
    prompt = quiz.build_quiz_prompt_text(mock_country_dict, "easy", 60, filter_specified=True)
    assert "Difficulty: Easy" in prompt
    assert "Time to answer: 60s" in prompt
    assert "Reward: 1250 credits" in prompt
    assert "Continent: Europa" in prompt
    assert "Country: *Polska Rzeczpospolita*" in prompt

    timeout_msg = quiz.format_timeout_message(mock_country_dict)
    assert (
        timeout_msg
        == "Time's up! The capital of *Polska Rzeczpospolita* was *Warszawa*.\n\nCapital: Warszawa | Continent: Europa | Population: 38,000,000"
    )

    correct_msg = quiz.format_correct_message(mock_country_dict, reward=1250, total_credits=3000)
    assert (
        correct_msg
        == "Correct! The capital of *Polska Rzeczpospolita* is *Warszawa*.\nYou receive *1250* credits! [*3000* in total]\n\nCapital: Warszawa | Continent: Europa | Population: 38,000,000"
    )

    wrong_msg = quiz.format_wrong_message(mock_country_dict)
    assert (
        wrong_msg
        == "Wrong! The capital of *Polska Rzeczpospolita* was *Warszawa*.\n\nCapital: Warszawa | Continent: Europa | Population: 38,000,000"
    )
