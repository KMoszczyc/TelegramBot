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
        lat_close = 50.0 + 0.5 * np.cos(np.radians(bearing))
        lng_close = 10.0 + 0.5 * np.sin(np.radians(bearing)) / np.cos(np.radians(50.0))
        rows.append(
            {"id": 300 + i, "city": f"Close{bearing}", "lat": lat_close, "lng": lng_close, "population": 700_000, "capital": "minor"}
        )
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


@pytest.mark.parametrize(
    "admin_name,country,expected_part",
    [
        ("Małopolskie", "Poland", "Poland, Małopolskie (Europa)"),
        (None, "Poland", "Poland (Europa)"),
        ("", "Poland", "Poland (Europa)"),
        ("Monaco", "Monaco", "Monaco (Europa)"),
        ("Singapore", "Singapore", "Singapore (Europa)"),
    ],
)
def test_city_quiz_description_district_scenarios(mock_city_dict, admin_name, country, expected_part):
    quiz = CityQuiz()
    city = dict(mock_city_dict)
    city["country"] = country
    city["admin_name"] = admin_name
    desc = quiz.get_description(city)
    assert expected_part in desc
    assert "population: 800,000" in desc


def test_city_quiz_messages_include_district(mock_city_dict):
    quiz = CityQuiz()
    correct = quiz.format_correct_message(mock_city_dict, reward=100)
    assert "Poland, Małopolskie (Europa)" in correct

    wrong = quiz.format_wrong_message(mock_city_dict)
    assert "Poland, Małopolskie (Europa)" in wrong

    timeout = quiz.format_timeout_message(mock_city_dict)
    assert "Poland, Małopolskie (Europa)" in timeout


@pytest.mark.parametrize(
    "filter_kwargs,expected_diff",
    [
        ({"difficulty": "easy", "country_iso2": "PL"}, "easy"),
        ({"difficulty": "hard", "country_iso2": "PL"}, "hard"),
        ({"country_iso2": "PL"}, None),
    ],
)
def test_cities_sample_mystery_city_with_filters(tmp_path, mock_cities_df, filter_kwargs, expected_diff):
    parquet_path = tmp_path / "test_cities.parquet"
    mock_cities_df["general_eligible"] = True
    mock_cities_df.to_parquet(parquet_path, index=False)
    cities = Cities(parquet_path=str(parquet_path), min_population=0)

    mystery, diff = cities.sample_mystery_city(**filter_kwargs)
    assert isinstance(mystery, dict)
    assert mystery["iso2"] == "PL"
    if expected_diff:
        assert diff == expected_diff


def test_cities_sample_mystery_city_general_mode_respects_regional_eligibility():
    cities = Cities(min_population=0)
    for _ in range(30):
        mystery, diff = cities.sample_mystery_city()
        assert diff in ["easy", "medium", "hard", "very_hard", "crazy"]
        continent = str(mystery.get("continent", "")).lower()
        if continent not in ("europa", "europe") and mystery.get("iso2") != "PL":
            assert mystery.get("general_eligible") is True
            assert diff in ("easy", "medium")


@pytest.mark.parametrize(
    "exclude_count",
    [1, 5, 8],
)
def test_cities_sample_mystery_city_respects_exclude_ids(exclude_count):
    cities = Cities(min_population=0)
    pl_cities = cities.get_cities(country_iso2="PL", difficulty="easy")
    all_ids = set(pl_cities["id"].tolist()[:exclude_count])

    for _ in range(15):
        mystery, _ = cities.sample_mystery_city(country_iso2="PL", difficulty="easy", exclude_ids=all_ids)
        assert mystery["id"] not in all_ids


def test_cities_sample_mystery_city_exclude_ids_fallback_when_all_excluded():
    cities = Cities(min_population=0)
    pl_cities = cities.get_cities(country_iso2="PL", difficulty="easy")
    all_ids = set(pl_cities["id"].tolist())

    mystery, _ = cities.sample_mystery_city(country_iso2="PL", difficulty="easy", exclude_ids=all_ids)
    assert isinstance(mystery, dict)
    assert mystery["iso2"] == "PL"


def test_bot_state_recent_mystery_city_ids_maxlen():
    from collections import deque
    from unittest.mock import MagicMock

    from src.models.core.bot_state import BotState

    bs = BotState(MagicMock(), MagicMock())
    assert isinstance(bs.recent_mystery_city_ids, deque)
    assert bs.recent_mystery_city_ids.maxlen == 25

    for i in range(35):
        bs.recent_mystery_city_ids.append(i)

    assert len(bs.recent_mystery_city_ids) == 25
    assert list(bs.recent_mystery_city_ids) == list(range(10, 35))


def test_real_cities_parquet_poland_difficulty_calibration():
    cities = Cities(min_population=0)
    pl_cities = cities.get_cities(country_iso2="PL")
    easy_pl = pl_cities[pl_cities["difficulty"] == "easy"]
    assert len(easy_pl) >= 10
    easy_names = set(easy_pl["city"].tolist())
    for expected in [
        "Warsaw",
        "Kraków",
        "Wrocław",
        "Łódź",
        "Poznań",
        "Gdańsk",
        "Szczecin",
        "Bydgoszcz",
        "Lublin",
        "Katowice",
    ]:
        assert expected in easy_names

    medium_pl = pl_cities[pl_cities["difficulty"] == "medium"]
    assert len(medium_pl) >= 20


@pytest.mark.parametrize("city_name", ["Keratsíni", "Karaman"])
def test_obscure_european_cities_are_very_hard(city_name):
    cities = Cities(min_population=0)
    match = cities.df[cities.df["city"] == city_name]
    assert not match.empty
    assert match.iloc[0]["difficulty"] == "very_hard"


def test_select_hint_cities_katowice_local_proximity():
    cities = Cities(min_population=0)
    katowice = cities.df[(cities.df["city"] == "Katowice") & (cities.df["country"] == "Poland")].iloc[0].to_dict()
    hints = select_hint_cities(katowice, cities.df, difficulty="easy")
    assert len(hints) == 5
    for h in hints:
        assert h["_dist_km"] <= 280.0
    hint_cities = {h["city"] for h in hints}
    for distant in ["Berlin", "Lviv", "Budapest", "Vienna"]:
        assert distant not in hint_cities


def test_select_hint_cities_bratislava_includes_vienna():
    cities = Cities(min_population=0)
    bratislava = cities.df[cities.df["city"] == "Bratislava"].iloc[0].to_dict()
    hints = select_hint_cities(bratislava, cities.df, difficulty="medium")
    assert len(hints) == 5
    hint_cities = {h["city"] for h in hints}
    assert "Vienna" in hint_cities


def test_select_hint_cities_bytom_includes_nearby_anchor_and_avoids_distant_outliers():
    cities = Cities(min_population=0)
    bytom = cities.df[cities.df["city"].str.lower() == "bytom"].iloc[0].to_dict()
    hints = select_hint_cities(bytom, cities.df, difficulty="medium")
    assert len(hints) == 5
    hint_names = {h["city"] for h in hints}
    # One close anchor (e.g. Katowice, Zabrze, Gliwice, etc.) within 30 km
    close_hints = [h for h in hints if h["_dist_km"] <= 30.0]
    assert len(close_hints) == 1
    # No distant outliers > 150 km (like Wałbrzych, Kalisz, Rzeszów, Łódź)
    assert all(h["_dist_km"] <= 150.0 for h in hints)
    assert "Wałbrzych" not in hint_names
    assert "Rzeszów" not in hint_names


def test_select_hint_cities_rome_uses_famous_cities():
    cities = Cities(min_population=0)
    rome = cities.df[(cities.df["city"] == "Rome") & (cities.df["country"] == "Italy")].iloc[0].to_dict()
    hints = select_hint_cities(rome, cities.df, difficulty="easy")
    assert len(hints) == 5
    hint_cities = {h["city"] for h in hints}
    for obscure in ["Latina", "Terni", "Civitavecchia", "Aprilia", "Ladispoli"]:
        assert obscure not in hint_cities
    assert any(
        c in hint_cities
        for c in [
            "Naples",
            "Florence",
            "Milan",
            "Palermo",
            "Bari",
            "Genoa",
            "Venice",
            "Bologna",
            "Nice",
            "Zagreb",
            "Ljubljana",
            "Sarajevo",
            "Cagliari",
        ]
    )


def test_select_hint_cities_portland_includes_seattle_and_vancouver():
    cities = Cities(min_population=0)
    portland_row = cities.df[cities.df["id"] == 1840019941]
    assert not portland_row.empty
    portland = portland_row.iloc[0].to_dict()
    pool = cities.get_hint_pool(portland["iso2"])
    hints = select_hint_cities(portland, pool)
    assert len(hints) == 5
    hint_names = {h["city"] for h in hints}
    assert "Seattle" in hint_names
    assert "Vancouver" in hint_names


def test_generate_diagram_corridor_labels_no_collision():
    import math

    from PIL import Image, ImageDraw

    from src.models.quizzes.city_quiz import (
        _ARROW_LEN,
        _CIRCLE_R,
        _FONT_BOLD,
        _FONT_DIST,
        _LABEL_PAD,
        _SCALE,
        _bearing_deg,
        _load_flag,
    )

    mystery_lon, mystery_lat = -122.65, 45.54
    hints = [
        {"city": "Seattle", "iso2": "US", "lng": -122.33, "lat": 47.61, "_dist_km": 233.1},
        {"city": "Vancouver", "iso2": "CA", "lng": -123.12, "lat": 49.28, "_dist_km": 414.2},
        {"city": "Spokane", "iso2": "US", "lng": -117.43, "lat": 47.66, "_dist_km": 463.5},
        {"city": "Eugene", "iso2": "US", "lng": -123.09, "lat": 44.05, "_dist_km": 168.7},
        {"city": "Kennewick", "iso2": "US", "lng": -119.14, "lat": 46.21, "_dist_km": 279.0},
    ]

    # Verify diagram generates successfully
    buf = generate_diagram(mystery_lon, mystery_lat, hints, show_distances=True)
    assert buf is not None
    assert buf.getbuffer().nbytes > 1000

    # Verify geometry: uniform arrow shafts and 0 label collision
    canvas_w, canvas_h = 900 * _SCALE, 900 * _SCALE
    cx, cy = canvas_w // 2, canvas_h // 2
    dummy_img = Image.new("RGBA", (canvas_w, canvas_h))
    draw = ImageDraw.Draw(dummy_img)

    hint_layouts = []
    for hint in hints:
        hlon = float(hint["lng"])
        hlat = float(hint["lat"])
        hname = str(hint["city"])
        iso2 = str(hint["iso2"])
        dist_km = hint["_dist_km"]

        bearing = _bearing_deg(mystery_lon, mystery_lat, hlon, hlat)
        b_rad = math.radians(bearing)
        sin_b = math.sin(b_rad)
        cos_b = math.cos(b_rad)

        sx = cx + sin_b * _CIRCLE_R
        sy = cy - cos_b * _CIRCLE_R
        tx = cx + sin_b * (_CIRCLE_R + _ARROW_LEN)
        ty = cy - cos_b * (_CIRCLE_R + _ARROW_LEN)

        # Shaft length must equal _ARROW_LEN
        shaft_len = math.hypot(tx - sx, ty - sy)
        assert abs(shaft_len - _ARROW_LEN) < 1e-4

        flag_img = _load_flag(iso2)
        fw = (flag_img.width + _LABEL_PAD) if flag_img else 0
        fh = flag_img.height if flag_img else 0

        name_bb = draw.textbbox((0, 0), hname, font=_FONT_BOLD)
        name_w = name_bb[2] - name_bb[0]
        name_h = name_bb[3] - name_bb[1]

        dist_text = f"{int(dist_km):,} km"
        dist_bb = draw.textbbox((0, 0), dist_text, font=_FONT_DIST)
        dist_w = dist_bb[2] - dist_bb[0]
        dist_h = dist_bb[3] - dist_bb[1]

        group_w = fw + max(name_w, dist_w)
        gap_between_name_and_dist = 6 * _SCALE
        group_h = max(fh, name_h + gap_between_name_and_dist + dist_h)

        gap = 18 * _SCALE
        lx = tx + sin_b * gap
        ly = ty - cos_b * gap

        hint_layouts.append(
            {
                "bearing": bearing,
                "sin_b": sin_b,
                "cos_b": cos_b,
                "lx": lx,
                "ly": ly,
                "group_w": group_w,
                "group_h": group_h,
            }
        )

    for i, item in enumerate(hint_layouts):
        bearing = item["bearing"]
        sin_b, cos_b = item["sin_b"], item["cos_b"]
        lx, ly = item["lx"], item["ly"]
        group_w, group_h = item["group_w"], item["group_h"]

        min_abs_diff = 360.0
        c_diff = 0.0
        for j, other in enumerate(hint_layouts):
            if i == j:
                continue
            diff = (other["bearing"] - bearing + 180.0) % 360.0 - 180.0
            if abs(diff) < min_abs_diff:
                min_abs_diff = abs(diff)
                c_diff = diff

        if min_abs_diff <= 35.0:
            if abs(cos_b) >= abs(sin_b):
                gx = lx - group_w if c_diff > 0 else lx
                gy = ly - group_h if cos_b >= 0.15 else (ly if cos_b <= -0.15 else ly - group_h / 2)
            else:
                if sin_b > 0:
                    gx = lx
                    gy = ly - group_h if c_diff > 0 else ly
                else:
                    gx = lx - group_w
                    gy = ly if c_diff > 0 else ly - group_h
        else:
            if sin_b >= 0.15:
                gx = lx
            elif sin_b <= -0.15:
                gx = lx - group_w
            else:
                gx = lx - group_w / 2

            if cos_b >= 0.15:
                gy = ly - group_h
            elif cos_b <= -0.15:
                gy = ly
            else:
                gy = ly - group_h / 2

        item["gx"] = gx
        item["gy"] = gy

    # Collision test
    collisions = 0
    for i in range(len(hint_layouts)):
        for j in range(i + 1, len(hint_layouts)):
            b1, b2 = hint_layouts[i], hint_layouts[j]
            ox = min(b1["gx"] + b1["group_w"], b2["gx"] + b2["group_w"]) - max(b1["gx"], b2["gx"])
            oy = min(b1["gy"] + b1["group_h"], b2["gy"] + b2["group_h"]) - max(b1["gy"], b2["gy"])
            if ox > 0 and oy > 0:
                collisions += 1
    assert collisions == 0


def test_kazakhstan_general_mode_eligibility():
    """In General Mode, only Astana (capital) and Almaty (curated iconic) are eligible for Kazakhstan."""
    cities = Cities(min_population=0)
    kz = cities.df[cities.df["iso2"] == "KZ"]
    assert not kz.empty
    eligible = kz[kz["general_eligible"]]
    assert set(eligible["city"].tolist()) == {"Astana", "Almaty"}

    # Obscure 100k towns must not be eligible in General Mode
    ineligible_names = set(kz[~kz["general_eligible"]]["city"].tolist())
    for town in ["Rudnyy", "Zhangaözen", "Zhezqazghan", "Oral", "Aqtöbe"]:
        assert town in ineligible_names


def test_russia_general_mode_eligibility():
    """Russia allows European Russia >= 100k and only major Asian/Siberian hubs >= 500k."""
    cities = Cities(min_population=0)
    ru = cities.df[cities.df["iso2"] == "RU"]
    assert not ru.empty

    eligible = ru[ru["general_eligible"]]
    eligible_names = set(eligible["city"].tolist())
    # Major hubs must be included
    for hub in ["Moscow", "Saint Petersburg", "Kazan", "Novosibirsk", "Vladivostok"]:
        assert hub in eligible_names

    # Siberian towns with pop < 500k must be excluded from General Mode
    ineligible = ru[~ru["general_eligible"]]
    ineligible_names = set(ineligible["city"].tolist())
    for town in ["Norilsk", "Nizhnevartovsk"]:
        if town in ru["city"].values:
            assert town in ineligible_names


def test_turkey_general_mode_eligibility():
    """Turkey keeps core metropolises and provincial capitals, excluding sub-districts and small towns."""
    cities = Cities(min_population=0)
    tr = cities.df[cities.df["iso2"] == "TR"]
    assert not tr.empty

    eligible = tr[tr["general_eligible"]]
    eligible_names = set(eligible["city"].tolist())
    assert "Istanbul" in eligible_names
    assert "Ankara" in eligible_names
    assert "Antalya" in eligible_names
    assert "İzmir" in eligible_names

    # Municipal borough duplicates in Istanbul/Ankara must be excluded
    ineligible = tr[~tr["general_eligible"]]
    ineligible_names = set(ineligible["city"].tolist())
    for subdistrict in ["Esenyurt", "Çankaya", "Küçükçekmece"]:
        if subdistrict in tr["city"].values:
            assert subdistrict in ineligible_names


def test_generate_diagram_horizontal_long_names_no_arrow_label_overlap():
    """Verify that horizontal East/West hint cities with long names (like Zduńska Wola) never overlap arrow tips."""
    import math

    from PIL import Image, ImageDraw

    from src.models.quizzes.city_quiz import (
        _CANVAS_H,
        _CANVAS_W,
        _CIRCLE_R,
        _FONT_BOLD,
        _LABEL_PAD,
        _SCALE,
        _bearing_deg,
        _clip_arrow_to_box,
        _load_flag,
    )

    mystery_lon, mystery_lat = 17.81, 51.65  # Ostrów Wielkopolski area
    hints = [
        {"city": "Września", "iso2": "PL", "lng": 17.57, "lat": 52.33},
        {"city": "Kalisz", "iso2": "PL", "lng": 18.08, "lat": 51.76},
        {"city": "Zduńska Wola", "iso2": "PL", "lng": 18.93, "lat": 51.60},
        {"city": "Oleśnica", "iso2": "PL", "lng": 17.38, "lat": 51.21},
        {"city": "Legnica", "iso2": "PL", "lng": 16.16, "lat": 51.21},
    ]

    # Render diagram and assert it succeeds
    buf = generate_diagram(mystery_lon, mystery_lat, hints)
    assert buf is not None
    assert buf.getbuffer().nbytes > 1000

    # Verify geometric clearance
    cx, cy = _CANVAS_W // 2, _CANVAS_H // 2
    margin = 24 * _SCALE
    gap = 18 * _SCALE

    img = Image.new("RGBA", (_CANVAS_W, _CANVAS_H))
    draw = ImageDraw.Draw(img)

    hint_layouts = []
    for h in hints:
        hlon, hlat = float(h["lng"]), float(h["lat"])
        b = _bearing_deg(mystery_lon, mystery_lat, hlon, hlat)
        b_rad = math.radians(b)
        sin_b, cos_b = math.sin(b_rad), math.cos(b_rad)
        flag_img = _load_flag(h["iso2"])
        fw = (flag_img.width + _LABEL_PAD) if flag_img else 0
        fh = flag_img.height if flag_img else 0
        name_bb = draw.textbbox((0, 0), h["city"], font=_FONT_BOLD)
        group_w = fw + (name_bb[2] - name_bb[0])
        group_h = max(fh, name_bb[3] - name_bb[1])
        hint_layouts.append(
            {
                "city": h["city"],
                "bearing": b,
                "sin_b": sin_b,
                "cos_b": cos_b,
                "group_w": group_w,
                "group_h": group_h,
            }
        )

    # Dynamic uniform shaft calculation
    limits = []
    for item in hint_layouts:
        sb, cb = item["sin_b"], item["cos_b"]
        gw, gh = item["group_w"], item["group_h"]
        if abs(sb) > 0.05:
            avail_x = cx - margin - gw
            if avail_x > 0:
                limits.append(avail_x / abs(sb) - gap - _CIRCLE_R)
        if abs(cb) > 0.05:
            avail_y = cy - margin - gh
            if avail_y > 0:
                limits.append(avail_y / abs(cb) - gap - _CIRCLE_R)

    arrow_len = min(180 * _SCALE, max(110 * _SCALE, min(limits))) if limits else 180 * _SCALE

    for item in hint_layouts:
        sb, cb = item["sin_b"], item["cos_b"]
        sx = cx + sb * _CIRCLE_R
        sy = cy - cb * _CIRCLE_R
        tx = cx + sb * (_CIRCLE_R + arrow_len)
        ty = cy - cb * (_CIRCLE_R + arrow_len)
        lx = tx + sb * gap
        ly = ty - cb * gap
        item.update({"sx": sx, "sy": sy, "tx": tx, "ty": ty, "lx": lx, "ly": ly})

    # Directional placement
    for item in hint_layouts:
        b = item["bearing"]
        sb, cb = item["sin_b"], item["cos_b"]
        lx, ly = item["lx"], item["ly"]
        gw, gh = item["group_w"], item["group_h"]
        if sb >= 0.15:
            gx = lx
        elif sb <= -0.15:
            gx = lx - gw
        else:
            gx = lx - gw / 2
        if cb >= 0.15:
            gy = ly - gh
        elif cb <= -0.15:
            gy = ly
        else:
            gy = ly - gh / 2
        item["gx"] = max(margin, min(_CANVAS_W - gw - margin, gx))
        item["gy"] = max(margin, min(_CANVAS_H - gh - margin, gy))
        clipped_tx, clipped_ty = _clip_arrow_to_box(item["sx"], item["sy"], item["tx"], item["ty"], item["gx"], item["gy"], gw, gh, gap)
        item["tx"], item["ty"] = clipped_tx, clipped_ty

    # Verify that Zduńska Wola and all hint arrows do NOT penetrate label bounding boxes
    for item in hint_layouts:
        gx, gy = item["gx"], item["gy"]
        gw, gh = item["group_w"], item["group_h"]
        tx, ty = item["tx"], item["ty"]
        # Tip must not be inside the label bounding box
        assert not (gx < tx < gx + gw and gy < ty < gy + gh)
        if item["city"] == "Zduńska Wola":
            # East-pointing arrow tip must terminate before the left edge of the label
            assert tx <= gx
