import pandas as pd
import pytest

from src.models.flag_quiz import FlagQuiz


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
    "difficulty, continent_specified, tips_given, expected_reward, expected_decrease_pct",
    [
        ("easy", False, 0, 1000, 0),
        ("medium", False, 0, 3000, 0),
        ("hard", False, 0, 10000, 0),
        ("crazy", False, 0, 25000, 0),
        ("crazy", True, 0, 12500, 0),  # Halved when continent specified
        ("easy", True, 0, 500, 0),  # Halved when continent specified
        ("crazy", False, 1, 12500, 50),
        ("crazy", False, 2, 9375, 62),
        ("crazy", False, 3, 7031, 72),
        ("crazy", True, 1, 6250, 50),  # Halved base (12500) * 0.5
    ],
)
def test_get_reward(difficulty, continent_specified, tips_given, expected_reward, expected_decrease_pct):
    reward, decrease = FlagQuiz.get_reward(difficulty, continent_specified, tips_given)
    assert reward == expected_reward
    assert decrease == expected_decrease_pct


def test_get_country_display_name(mock_country_dict):
    assert FlagQuiz.get_country_display_name(mock_country_dict) == "Polska Rzeczpospolita"
    series = pd.Series(mock_country_dict)
    assert FlagQuiz.get_country_display_name(series) == "Polska Rzeczpospolita"


def test_get_country_description(mock_country_dict):
    desc = FlagQuiz.get_country_description(mock_country_dict)
    assert "Capital: Warszawa" in desc
    assert "Continent: Europa" in desc
    assert "Population: 38,000,000" in desc


@pytest.mark.parametrize(
    "continent_specified, expected_tips",
    [
        (
            False,
            [
                "Continent: Europa",
                "First letter: P",
                "Second letter: o",
            ],
        ),
        (
            True,
            [
                "First letter: P",
                "Second letter: o",
            ],
        ),
    ],
)
def test_get_tips(mock_country_dict, continent_specified, expected_tips):
    tips = FlagQuiz.get_tips(mock_country_dict, continent_specified=continent_specified)
    assert tips == expected_tips


def test_get_valid_answers(mock_country_dict):
    valid = FlagQuiz.get_valid_answers(mock_country_dict)
    assert "polska rzeczpospolita" in valid
    assert "polska" in valid
    assert "rzeczpospolita" in valid
    assert "pl" in valid


@pytest.mark.parametrize(
    "user_answer, expected_correct",
    [
        ("polska rzeczpospolita", True),
        ("Polska", True),
        ("pl", True),
        ("polsk", True),  # Fuzzy distance 1
        ("niemcy", False),
    ],
)
def test_is_answer_correct(mock_country_dict, user_answer, expected_correct):
    valid = FlagQuiz.get_valid_answers(mock_country_dict)
    assert FlagQuiz.is_answer_correct(user_answer, valid) is expected_correct


def test_guess_random_flag():
    df = pd.DataFrame(
        [{"country_name": "Polska", "country_code": "PL", "continent": "Europa", "population": 38000000, "capital": "Warszawa"}]
    )
    quiz = FlagQuiz()
    image_path, country = quiz.guess_random_flag(df)
    assert country["country_code"] == "PL"
    assert image_path.endswith("PL.jpg")
