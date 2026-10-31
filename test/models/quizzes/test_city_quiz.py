import random

import numpy as np
import pandas as pd
import pytest

from src.models.data.cities import Cities
from src.models.quizzes.city_quiz import (
    CityQuiz,
    _angle_diff,
    _angle_diff_np,
    _bearing_deg,
    _bearing_np,
    _haversine,
    _haversine_np,
    _prominence,
    generate_diagram,
    select_hint_cities,
)


@pytest.fixture
def mock_city_dict():
    return {
        "city": "Kraków",
        "city_ascii": "Krakow",
        "lat": 50.0647,
        "lng": 19.9450,
        "country": "Poland",
        "iso2": "PL",
        "iso3": "POL",
        "admin_name": "Małopolskie",
        "capital": "admin",
        "population": 800000,
        "id": 1616000001,
        "continent": "Europa",
        "difficulty": "medium",
    }


@pytest.fixture
def mock_cities_df():
    return pd.DataFrame(
        [
            {
                "city": "Warszawa",
                "city_ascii": "Warsaw",
                "lat": 52.2297,
                "lng": 21.0122,
                "country": "Poland",
                "iso2": "PL",
                "iso3": "POL",
                "admin_name": "Mazowieckie",
                "capital": "primary",
                "population": 1800000,
                "id": 1616000002,
                "continent": "Europa",
                "difficulty": "easy",
            },
            {
                "city": "Gdańsk",
                "city_ascii": "Gdansk",
                "lat": 54.3520,
                "lng": 18.6466,
                "country": "Poland",
                "iso2": "PL",
                "iso3": "POL",
                "admin_name": "Pomorskie",
                "capital": "admin",
                "population": 470000,
                "id": 1616000003,
                "continent": "Europa",
                "difficulty": "medium",
            },
            {
                "city": "Wrocław",
                "city_ascii": "Wroclaw",
                "lat": 51.1079,
                "lng": 17.0385,
                "country": "Poland",
                "iso2": "PL",
                "iso3": "POL",
                "admin_name": "Dolnośląskie",
                "capital": "admin",
                "population": 670000,
                "id": 1616000004,
                "continent": "Europa",
                "difficulty": "medium",
            },
            {
                "city": "Rzeszów",
                "city_ascii": "Rzeszow",
                "lat": 50.0412,
                "lng": 21.9991,
                "country": "Poland",
                "iso2": "PL",
                "iso3": "POL",
                "admin_name": "Podkarpackie",
                "capital": "admin",
                "population": 190000,
                "id": 1616000005,
                "continent": "Europa",
                "difficulty": "hard",
            },
            {
                "city": "Zakopane",
                "city_ascii": "Zakopane",
                "lat": 49.2992,
                "lng": 19.9496,
                "country": "Poland",
                "iso2": "PL",
                "iso3": "POL",
                "admin_name": "Małopolskie",
                "capital": "minor",
                "population": 35000,
                "id": 1616000006,
                "continent": "Europa",
                "difficulty": "very_hard",
            },
            {
                "city": "Mała Wieś",
                "city_ascii": "Mala Wies",
                "lat": 52.4000,
                "lng": 20.0000,
                "country": "Poland",
                "iso2": "PL",
                "iso3": "POL",
                "admin_name": "Mazowieckie",
                "capital": "minor",
                "population": 5000,
                "id": 1616000007,
                "continent": "Europa",
                "difficulty": "crazy",
            },
        ]
    )


@pytest.mark.parametrize(
    ("difficulty", "filter_specified", "tips_given", "expected_penalty_pct"),
    [
        ("easy", False, 0, 0),
        ("medium", False, 1, 10),
        ("hard", False, 2, 25),
        ("very_hard", False, 3, 45),
        ("crazy", False, 4, 70),
        ("easy", True, 0, 0),
        ("medium", True, 1, 10),
        ("hard", True, 2, 25),
        ("very_hard", True, 3, 45),
        ("crazy", True, 4, 70),
    ],
)
def test_city_quiz_rewards(difficulty, filter_specified, tips_given, expected_penalty_pct):
    base_reward = CityQuiz.REWARD_LEVELS[difficulty]
    if filter_specified:
        base_reward = base_reward // 2

    expected_reward = int(round(base_reward * (1.0 - expected_penalty_pct / 100.0)))
    reward, decrease = CityQuiz.get_reward(difficulty, filter_specified, tips_given)
    assert reward == expected_reward
    assert decrease == expected_penalty_pct


def test_city_quiz_display_and_description(mock_city_dict):
    quiz = CityQuiz()
    assert quiz.get_display_name(mock_city_dict) == "Kraków"
    desc = quiz.get_description(mock_city_dict)
    assert "Poland" in desc
    assert "Europa" in desc
    assert "800,000" in desc


def test_city_quiz_valid_answers(mock_city_dict):
    quiz = CityQuiz()
    answers = quiz.get_valid_answers(mock_city_dict)
    assert "kraków" in answers
    assert "krakow" in answers


def test_city_quiz_tips(mock_city_dict):
    quiz = CityQuiz()
    tips = quiz.get_tips(mock_city_dict)
    assert len(tips) == 4
    assert "Distances" in tips[0]
    assert tips[1] == "👥 Population: *800,000*"
    assert "*K*" in tips[2]
    assert "Multiple-choice" in tips[3]


@pytest.mark.parametrize(
    ("pop_value", "expected_pop_str"),
    [
        (1_234_567, "1,234,567"),
        (500, "500"),
        (0, "unknown"),
        (None, "unknown"),
    ],
)
def test_city_quiz_population_tip(pop_value, expected_pop_str):
    quiz = CityQuiz()
    item = {"city": "TestCity", "population": pop_value}
    tips = quiz.get_tips(item)
    assert tips[1] == f"👥 Population: *{expected_pop_str}*"


def test_generate_mc_options(mock_city_dict, mock_cities_df):
    options = CityQuiz.generate_mc_options(mock_city_dict, mock_cities_df, n=4)
    assert len(options) == 4
    assert mock_city_dict["city"] in options
    # Options should be unique
    assert len(set(options)) == 4


def test_bot_state_find_active_city_quiz():
    from unittest.mock import MagicMock

    from src.models.core.bot_state import BotState

    state = BotState(job_queue=MagicMock(), assets=MagicMock())
    state.city_quiz_cache[999] = {"city": "Warszawa"}
    result = state.find_active_guess_quiz(999)
    assert result is not None
    cache, cls = result
    assert cls is CityQuiz
    assert cache[999]["city"] == "Warszawa"


def test_city_quiz_format_messages(mock_city_dict):
    quiz = CityQuiz()
    timeout_msg = quiz.format_timeout_message(mock_city_dict)
    assert "Time's up!" in timeout_msg
    assert "Kraków" in timeout_msg

    correct_msg = quiz.format_correct_message(mock_city_dict, 1000, 5000)
    assert "Correct!" in correct_msg
    assert "1000" in correct_msg
    assert "5000" in correct_msg

    wrong_msg = quiz.format_wrong_message(mock_city_dict)
    assert "Wrong!" in wrong_msg
    assert "Kraków" in wrong_msg


@pytest.mark.parametrize(
    "lon1, lat1, lon2, lat2, expected_dist_approx",
    [
        (0.0, 0.0, 0.0, 0.0, 0),
        (21.0122, 52.2297, 19.9450, 50.0647, 252),  # Warsaw to Krakow ~ 252km
    ],
)
def test_haversine(lon1, lat1, lon2, lat2, expected_dist_approx):
    dist = _haversine(lon1, lat1, lon2, lat2)
    assert abs(dist - expected_dist_approx) < 5


@pytest.mark.parametrize(
    "a, b, expected_diff",
    [
        (0, 0, 0),
        (10, 350, 20),
        (90, 270, 180),
        (355, 5, 10),
    ],
)
def test_angle_diff(a, b, expected_diff):
    assert _angle_diff(a, b) == expected_diff


def test_select_hint_cities(mock_city_dict, mock_cities_df):
    hints = select_hint_cities(mock_city_dict, mock_cities_df, n=4, min_gap_deg=15.0)
    assert len(hints) > 0
    # Must not include mystery city itself
    for h in hints:
        assert h["id"] != mock_city_dict["id"]


def test_generate_diagram(mock_city_dict, mock_cities_df):
    hints = select_hint_cities(mock_city_dict, mock_cities_df, n=3, min_gap_deg=10.0)
    buf = generate_diagram(mock_city_dict["lng"], mock_city_dict["lat"], hints, show_distances=False)
    assert buf is not None
    assert buf.getbuffer().nbytes > 1000

    buf_dist = generate_diagram(mock_city_dict["lng"], mock_city_dict["lat"], hints, show_distances=True)
    assert buf_dist is not None
    assert buf_dist.getbuffer().nbytes > 1000


@pytest.mark.parametrize("show_distances", [False, True])
def test_generate_diagram_without_windows_fonts(mock_city_dict, mock_cities_df, show_distances):
    import os
    from unittest.mock import patch

    from src.core.font_utils import get_font

    hints = select_hint_cities(mock_city_dict, mock_cities_df, n=3, min_gap_deg=10.0)
    # Simulate an environment where C:/Windows/Fonts does not exist
    orig_exists = os.path.exists

    def fake_exists(path):
        if str(path).startswith("C:/Windows") or str(path).startswith("C:\\Windows"):
            return False
        return orig_exists(path)

    get_font.cache_clear()
    with patch("os.path.exists", side_effect=fake_exists):
        buf = generate_diagram(mock_city_dict["lng"], mock_city_dict["lat"], hints, show_distances=show_distances)
        assert buf is not None
        assert buf.getbuffer().nbytes > 1000
    get_font.cache_clear()


def test_cities_model(tmp_path, mock_cities_df):
    parquet_path = tmp_path / "test_cities.parquet"
    mock_cities_df.to_parquet(parquet_path, index=False)

    # By default, min_population=30_000 filters out Mała Wieś (5,000)
    cities = Cities(parquet_path=str(parquet_path))
    assert len(cities.df) == 5
    assert "Mała Wieś" not in cities.df["city"].values

    # Without filter, all 6 cities are loaded
    unfiltered = Cities(parquet_path=str(parquet_path), min_population=0)
    assert len(unfiltered.df) == 6
    assert "Mała Wieś" in unfiltered.df["city"].values

    easy_cities = cities.get_cities(difficulty="easy")
    assert len(easy_cities) == 1
    assert easy_cities.iloc[0]["city"] == "Warszawa"

    poland_cities = cities.get_cities(country_iso2="PL")
    assert len(poland_cities) == 5

    diffs = cities.get_available_difficulties()
    assert "easy" in diffs
    assert "medium" in diffs

    sample = cities.sample_city(easy_cities)
    assert sample["city"] == "Warszawa"


@pytest.mark.parametrize("district_input", ["Dolnośląskie", "dolnoslaskie", "DOLNOSLASKIE"])
def test_cities_district_filtering(tmp_path, mock_cities_df, district_input):
    parquet_path = tmp_path / "test_cities.parquet"
    mock_cities_df.to_parquet(parquet_path, index=False)

    cities = Cities(parquet_path=str(parquet_path))
    result = cities.get_cities(district=district_input)
    assert len(result) == 1
    assert result.iloc[0]["city"] == "Wrocław"

    country = cities.find_country_for_district(district_input)
    assert country == "PL"

    districts = cities.get_districts_for_country("PL")
    assert "Dolnośląskie" in districts
    assert "Mazowieckie" in districts


def test_cities_hint_pool_district(tmp_path, mock_cities_df):
    parquet_path = tmp_path / "test_cities.parquet"
    mock_cities_df.to_parquet(parquet_path, index=False)

    cities = Cities(parquet_path=str(parquet_path))
    # District with only 1 city (< 4) should fall back to country pool (5 cities)
    hint_pool = cities.get_hint_pool(mystery_iso2="PL", country_iso2="PL", district="Dolnośląskie")
    assert len(hint_pool) == 5


def test_arg_parser_optional_string():
    import pandas as pd

    from src.config.enums import ArgType
    from src.core.arg_parser import ArgParser
    from src.models.data.command_args import CommandArgs

    users_df = pd.DataFrame()
    available_named_args = {
        "country": ArgType.STRING,
        "district": ArgType.OPTIONAL_STRING,
    }
    # 1. Bare --district without value
    args1 = CommandArgs(
        args=["--country", "PL", "--district"],
        available_named_args=available_named_args,
    )
    parsed1 = ArgParser.parse_named_args(users_df, args1)
    assert parsed1.named_args["country"] == "PL"
    assert parsed1.named_args["district"] == ""

    # 2. --district with value
    args2 = CommandArgs(
        args=["--district", "Dolnośląskie"],
        available_named_args=available_named_args,
    )
    parsed2 = ArgParser.parse_named_args(users_df, args2)
    assert parsed2.named_args["district"] == "Dolnośląskie"

    # 3. Multiple optional string flags
    available_named_args_multi = {
        "country": ArgType.STRING,
        "district": ArgType.OPTIONAL_STRING,
        "region": ArgType.OPTIONAL_STRING,
    }
    args3 = CommandArgs(
        args=["--region", "Pomorskie"],
        available_named_args=available_named_args_multi,
    )
    parsed3 = ArgParser.parse_named_args(users_df, args3)
    assert parsed3.named_args["region"] == "Pomorskie"


@pytest.mark.parametrize(
    "lon1, lat1, lon2, lat2",
    [(21.0, 52.2, 19.9, 50.1), (139.7, 35.7, 141.3, 43.1), (-79.4, 43.7, -75.7, 45.4), (0.0, 0.0, 0.0, 0.0)],
)
def test_vectorised_geometry_matches_scalar(lon1, lat1, lon2, lat2):
    lons, lats = np.array([lon2]), np.array([lat2])
    assert _haversine_np(lon1, lat1, lons, lats)[0] == pytest.approx(_haversine(lon1, lat1, lon2, lat2))
    if (lon1, lat1) != (lon2, lat2):
        assert _bearing_np(lon1, lat1, lons, lats)[0] == pytest.approx(_bearing_deg(lon1, lat1, lon2, lat2))


@pytest.mark.parametrize("a, b, expected", [(0, 0, 0), (10, 350, 20), (90, 270, 180), (355, 5, 10)])
def test_angle_diff_np(a, b, expected):
    assert _angle_diff_np(np.array([a]), b)[0] == expected


def test_prominence_boosts_capitals():
    prom = _prominence(pd.Series([100_000, 100_000, 100_000, 10]), pd.Series(["primary", "admin", "minor", "minor"]))
    assert prom.tolist() == [300_000, 140_000, 100_000, 1_000]


@pytest.fixture
def ring_cities():
    """A mystery city at the origin surrounded by famous cities in all directions plus tiny suburbs."""
    rows = [{"id": 0, "city": "Centre", "lat": 50.0, "lng": 10.0, "population": 500_000, "capital": "minor"}]
    for i, bearing in enumerate(range(0, 360, 30), start=1):
        lat = 50.0 + 3.0 * np.cos(np.radians(bearing))
        lng = 10.0 + 3.0 * np.sin(np.radians(bearing)) / np.cos(np.radians(50.0))
        rows.append({"id": i, "city": f"Big{bearing}", "lat": lat, "lng": lng, "population": 900_000, "capital": "minor"})
        lat_mid = 50.0 + 1.0 * np.cos(np.radians(bearing))
        lng_mid = 10.0 + 1.0 * np.sin(np.radians(bearing)) / np.cos(np.radians(50.0))
        rows.append({"id": 200 + i, "city": f"Mid{bearing}", "lat": lat_mid, "lng": lng_mid, "population": 800_000, "capital": "minor"})
        rows.append({"id": 100 + i, "city": f"Tiny{bearing}", "lat": lat + 0.02, "lng": lng, "population": 3_000, "capital": "minor"})
    rows.append({"id": 999, "city": "Suburb", "lat": 50.05, "lng": 10.0, "population": 2_000_000, "capital": "primary"})
    return pd.DataFrame(rows)


@pytest.mark.parametrize("seed", range(8))
def test_select_hint_cities_prefers_famous_distant_cities(ring_cities, seed):
    random.seed(seed)
    mystery = ring_cities.iloc[0].to_dict()
    hints = select_hint_cities(mystery, ring_cities, n=5)
    assert len(hints) == 5
    names = {h["city"] for h in hints}
    assert not any(n.startswith("Tiny") for n in names)
    assert "Suburb" not in names
    assert all(h["_dist_km"] >= 50 for h in hints)
    bearings = [h["_bearing"] for h in hints]
    assert all(_angle_diff(a, b) >= 28.0 for i, a in enumerate(bearings) for b in bearings[i + 1 :])


def test_select_hint_cities_varies_distance_between_games(ring_cities):
    mystery = ring_cities.iloc[0].to_dict()
    scales = set()
    for seed in range(12):
        random.seed(seed)
        hints = select_hint_cities(mystery, ring_cities, n=5)
        scales.add(round(sum(h["_dist_km"] for h in hints) / len(hints), -1))
    assert len(scales) > 1


def test_select_hint_cities_empty_pool(mock_city_dict):
    assert select_hint_cities(mock_city_dict, pd.DataFrame(columns=["id", "lat", "lng", "population", "capital"])) == []


def test_select_hint_cities_scales_with_difficulty(ring_cities):
    mystery = ring_cities.iloc[0].to_dict()
    hints_easy = select_hint_cities(mystery, ring_cities, n=5, difficulty="easy")
    hints_crazy = select_hint_cities(mystery, ring_cities, n=5, difficulty="crazy")
    assert len(hints_easy) == 5
    assert len(hints_crazy) == 5
    avg_easy = sum(h["_dist_km"] for h in hints_easy) / len(hints_easy)
    avg_crazy = sum(h["_dist_km"] for h in hints_crazy) / len(hints_crazy)
    # Hints for crazy tier should be closer to mystery city than easy tier
    assert avg_crazy <= avg_easy
