import pandas as pd
import pytest

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
    base_reward = FlagQuiz.REWARD_LEVELS.get(difficulty, FlagQuiz.REWARD_LEVELS.get("crazy", 0))
    if continent_specified:
        base_reward = base_reward // 2

    if tips_given == 0:
        expected_reward = base_reward
        expected_decrease_pct = 0
    else:
        expected_reward = int(base_reward * 0.5 * (0.75 ** (tips_given - 1)))
        expected_decrease_pct = int(round((base_reward - expected_reward) / base_reward * 100)) if base_reward > 0 else 0

    reward, decrease = FlagQuiz.get_reward(difficulty, continent_specified, tips_given)
    assert reward == expected_reward
    assert decrease == expected_decrease_pct


def test_get_country_display_name(mock_country_dict):
    assert FlagQuiz().get_display_name(mock_country_dict) == "Polska Rzeczpospolita"


def test_get_country_description(mock_country_dict):
    desc = FlagQuiz().get_description(mock_country_dict)
    assert "Capital: Warszawa" in desc
    assert "Continent: Europa" in desc
    assert "Population: 38,000,000" in desc


@pytest.mark.parametrize(
    "continent_specified, expected_tips",
    [
        (False, ["Continent: Europa", "First letter: P", "Second letter: o"]),
        (True, ["First letter: P", "Second letter: o"]),
    ],
)
def test_get_tips(mock_country_dict, continent_specified, expected_tips):
    assert FlagQuiz().get_tips(mock_country_dict, continent_specified) == expected_tips


def test_get_valid_answers(mock_country_dict):
    valid = FlagQuiz().get_valid_answers(mock_country_dict)
    assert "polska rzeczpospolita" in valid
    assert "polska" in valid
    assert "rzeczpospolita" in valid
    assert "pl" in valid


@pytest.mark.parametrize(
    "user_answer, is_correct",
    [
        ("polska rzeczpospolita", True),
        ("Polska", True),
        ("pl", True),
        ("polsk", True),  # Fuzzy 1
        ("niemcy", False),
    ],
)
def test_is_answer_correct(user_answer, is_correct):
    valid_answers = {"polska rzeczpospolita", "polska", "pl"}
    assert FlagQuiz.is_answer_correct(user_answer, valid_answers) == is_correct


def test_guess_random_flag():
    df = pd.DataFrame(
        [{"country_name": "Polska", "country_code": "PL", "continent": "Europa", "population": 38000000, "capital": "Warszawa"}]
    )
    quiz = FlagQuiz()
    image_path, country = quiz.guess_random_flag(df)
    assert country["country_code"] == "PL"
    assert image_path.endswith("PL.jpg")


def test_flag_quiz_prompt_and_outcome_formatting(mock_country_dict):
    quiz = FlagQuiz()
    prompt = quiz.build_quiz_prompt_text(mock_country_dict, "medium", 60, filter_specified=True, title="Which country's flag is this?")
    assert "Which country's flag is this?" in prompt
    assert "Difficulty: Medium" in prompt
    assert "Time to answer: 60s" in prompt
    assert "Reward: 1500 credits" in prompt
    assert "Continent: Europa" in prompt

    timeout_msg = quiz.format_timeout_message(mock_country_dict)
    assert (
        timeout_msg
        == "Time's up! The country was *Polska Rzeczpospolita*.\n\nCapital: Warszawa | Continent: Europa | Population: 38,000,000"
    )

    correct_msg = quiz.format_correct_message(mock_country_dict, reward=1500, total_credits=5000)
    assert (
        correct_msg
        == "Correct! The country is *Polska Rzeczpospolita*.\nYou receive *1500* credits! [*5000* in total]\n\nCapital: Warszawa | Continent: Europa | Population: 38,000,000"
    )

    wrong_msg = quiz.format_wrong_message(mock_country_dict)
    assert (
        wrong_msg
        == "Wrong! The correct answer was *Polska Rzeczpospolita*.\n\nCapital: Warszawa | Continent: Europa | Population: 38,000,000"
    )
