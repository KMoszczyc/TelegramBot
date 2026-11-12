"""Unit tests for top cities quiz model and helpers."""

import random

import pandas as pd
import pytest

from src.config.constants import (
    TOP_CITIES_BASE_REWARDS,
    TOP_CITIES_MAX_STRIKES,
    TOP_CITIES_MAX_TIPS,
)
from src.models.quizzes import top_cities_quiz as tcq
from src.models.quizzes.top_cities_quiz import (
    GameStatus,
    GuessResult,
    Pool,
    PoolKind,
    TopCitiesGame,
)


def make_city_df(rows: list[dict] | None = None) -> pd.DataFrame:
    if rows is not None:
        return pd.DataFrame(rows)

    cities_data = []
    # 15 cities for Poland
    for i in range(1, 16):
        pop = 2_000_000 - i * 100_000  # 1.9M down to 500k
        cities_data.append(
            {
                "city": f"PolishCity{i}",
                "city_ascii": f"PolishCity{i}",
                "country": "Poland",
                "iso2": "PL",
                "admin_name": f"Voivodeship{i}",
                "population": pop,
                "continent": "Europa",
            }
        )
    # 15 cities for Brazil
    for i in range(1, 16):
        pop = 3_000_000 - i * 150_000
        cities_data.append(
            {
                "city": f"BrazilianCity{i}",
                "city_ascii": f"BrazilianCity{i}",
                "country": "Brazil",
                "iso2": "BR",
                "admin_name": f"State{i}",
                "population": pop,
                "continent": "Ameryka Południowa",
            }
        )
    return pd.DataFrame(cities_data)


# ── Flag & Sizing Tests ───────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "iso2,expected",
    [
        ("PL", "🇵🇱"),
        ("pl", "🇵🇱"),
        ("US", "🇺🇸"),
        ("BR", "🇧🇷"),
        ("JP", "🇯🇵"),
        ("", ""),
        ("P", ""),
        ("POL", ""),
        (None, ""),
    ],
)
def test_iso2_to_flag(iso2, expected):
    assert tcq.iso2_to_flag(iso2) == expected


@pytest.mark.parametrize(
    "populations,expected_size",
    [
        # 12 cities >= 250k -> Top 10
        ([300_000] * 12, 10),
        # 12 cities, but 10th is 240k (< 250k); 5th is 220k (>= 200k) -> Top 5
        ([500_000, 400_000, 300_000, 250_000, 220_000] + [240_000] * 7, 5),
        # 7 cities, 5th is 190k (< 200k); 3rd is 150k (>= 100k) -> Top 3
        ([300_000, 200_000, 150_000, 120_000, 110_000, 100_000, 90_000], 3),
        # Not enough buffer (< size + 2)
        ([500_000] * 3, None),
        # 3rd city < 100k -> None
        ([300_000, 200_000, 80_000, 70_000, 60_000], None),
    ],
)
def test_get_list_size(populations, expected_size):
    df = pd.DataFrame({"population": populations})
    assert tcq.get_list_size(df) == expected_size


# ── String & Parsing Helpers ─────────────────────────────────────────────────


@pytest.mark.parametrize(
    "text,expected",
    [
        ("Tokyo, Delhi, Shanghai", ["Tokyo", "Delhi", "Shanghai"]),
        ("  Warsaw ,  , Krakow  ", ["Warsaw", "Krakow"]),
        ("", []),
        ("   ", []),
    ],
)
def test_parse_guesses(text, expected):
    assert tcq.parse_guesses(text) == expected


@pytest.mark.parametrize(
    "text,expected",
    [
        ("NormalCity", "NormalCity"),
        ("*Bold* `Code` \\Slash", "Bold Code Slash"),
        ("A" * 40, "A" * 30),
    ],
)
def test_sanitize_guess(text, expected):
    assert tcq.sanitize_guess(text) == expected


def test_build_valid_answers():
    row = {"city": "Kraków", "city_ascii": "Krakow"}
    valids = tcq.build_valid_answers(row)
    assert "kraków" in valids
    assert "krakow" in valids


# ── Pool Discovery & Filtering ────────────────────────────────────────────────


def test_get_eligible_pools_and_pick_pool():
    df = make_city_df()
    pools = tcq.get_eligible_pools(df)
    kinds = {p.kind for p in pools}
    assert PoolKind.WORLD in kinds
    assert PoolKind.CONTINENT in kinds
    assert PoolKind.COUNTRY in kinds

    picked = tcq.pick_pool(df, rng=random.Random(42))
    assert isinstance(picked, Pool)
    assert picked in pools


@pytest.mark.parametrize(
    "pool_kind,target,extra,expected_count",
    [
        (PoolKind.WORLD, "World", {}, 30),
        (PoolKind.CONTINENT, "Europa", {}, 15),
        (PoolKind.COUNTRY, "Poland", {}, 15),
        (PoolKind.LETTER, "P", {}, 15),
        (PoolKind.CUSTOM, "Europa (P)", {"continent": "Europa", "letter": "P"}, 15),
    ],
)
def test_get_pool_df(pool_kind, target, extra, expected_count):
    df = make_city_df()
    pool = Pool(pool_kind, target, 10, extra)
    pdf = tcq.get_pool_df(df, pool)
    assert len(pdf) == expected_count


# ── Matching Tests ────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "user_answer,expected_index",
    [
        ("Kraków", 0),
        ("krakow", 0),
        ("KRAKOW", 0),
        ("Warsaw", 1),
        ("Wroclaw", 2),
        ("Wrocław", 2),
        ("Barcellona", None),  # not in list
    ],
)
def test_find_best_top_city_match(user_answer, expected_index):
    entries_valid = [
        {"kraków", "krakow"},
        {"warsaw", "warszawa"},
        {"wrocław", "wroclaw"},
    ]
    indices = [0, 1, 2]
    match = tcq.find_best_top_city_match(user_answer, entries_valid, indices)
    assert match == expected_index


def test_find_best_top_city_match_fuzzy():
    entries_valid = [
        {"barcelona"},
        {"budapest"},
    ]
    # Typo: Barcellona -> Barcelona
    match = tcq.find_best_top_city_match("Barcellona", entries_valid, [0, 1])
    assert match == 0


def test_find_best_top_city_match_short_rejects_fuzzy():
    entries_valid = [{"rio"}]
    # 3 letters should not fuzzy match another 3-letter word
    match = tcq.find_best_top_city_match("rim", entries_valid, [0])
    assert match is None


# ── Rewards & Callbacks ───────────────────────────────────────────────────────


def test_calculate_reward():
    # Below minimum (less than ceil(10/2) = 5 correct)
    assert tcq.calculate_reward(10, 4, 0, 0) == 0

    # 10 correct, 0 tips, 0 strikes -> full base reward
    full = tcq.calculate_reward(10, 10, 0, 0)
    assert full == TOP_CITIES_BASE_REWARDS[10]

    # Tips and strikes penalize reward
    with_penalties = tcq.calculate_reward(10, 10, 2, 1)
    assert 0 < with_penalties < full


def test_tip_callback_data_roundtrip():
    user_id = 123456789
    cb = tcq.build_tip_callback_data(user_id)
    assert tcq.parse_tip_callback_data(cb) == user_id
    assert tcq.parse_tip_callback_data("invalid:123") is None
    assert tcq.parse_tip_callback_data("") is None


# ── TopCitiesGame Logic Tests ─────────────────────────────────────────────────


@pytest.fixture
def test_game() -> TopCitiesGame:
    df = make_city_df()
    pool = Pool(PoolKind.COUNTRY, "Poland", 10)
    return TopCitiesGame.create(df, pool)


def test_game_initial_state(test_game):
    assert test_game.list_size == 10
    assert test_game.correct_count == 0
    assert test_game.strikes == 0
    assert test_game.status == GameStatus.IN_PROGRESS
    assert not test_game.is_over
    assert test_game.can_give_tip


def test_game_submit_guess_correct(test_game):
    res = test_game.submit_guess("PolishCity1")
    assert res == GuessResult.CORRECT
    assert test_game.correct_count == 1
    assert 0 in test_game.guessed
    assert "PolishCity1" in test_game.last_guess


def test_game_submit_guess_duplicate(test_game):
    test_game.submit_guess("PolishCity1")
    res = test_game.submit_guess("PolishCity1")
    assert res == GuessResult.DUPLICATE
    assert test_game.correct_count == 1
    assert test_game.strikes == 0


def test_game_submit_guess_wrong(test_game):
    res = test_game.submit_guess("NonExistentCity")
    assert res == GuessResult.WRONG
    assert test_game.strikes == 1
    assert test_game.correct_count == 0


def test_game_submit_guesses_batch(test_game):
    results = test_game.submit_guesses("PolishCity1, PolishCity2, NonExistent")
    assert results == [GuessResult.CORRECT, GuessResult.CORRECT, GuessResult.WRONG]
    assert test_game.correct_count == 2
    assert test_game.strikes == 1


def test_game_three_strikes_loses(test_game):
    test_game.submit_guess("Wrong1")
    test_game.submit_guess("Wrong2")
    assert not test_game.is_over
    test_game.submit_guess("Wrong3")
    assert test_game.strikes == TOP_CITIES_MAX_STRIKES
    assert test_game.status == GameStatus.LOST
    assert test_game.is_over


def test_game_guess_all_wins(test_game):
    for i in range(1, 11):
        test_game.submit_guess(f"PolishCity{i}")
    assert test_game.status == GameStatus.WON
    assert test_game.is_over


def test_game_reveal_tip_country_pool(test_game):
    tip_idx = test_game.reveal_tip()
    assert tip_idx is not None
    assert tip_idx in test_game.tips_revealed
    assert test_game.tips_given == 1

    # Render slot with tip for country pool shows admin_name
    rendered = test_game._render_slot(tip_idx)
    assert "Voivodeship" in rendered or "starts with" in rendered


def test_game_reveal_tip_world_pool():
    df = make_city_df()
    pool = Pool(PoolKind.WORLD, "World", 10)
    game = TopCitiesGame.create(df, pool)
    tip_idx = game.reveal_tip()
    assert tip_idx is not None
    rendered = game._render_slot(tip_idx)
    # World pool tip reveals Country Name & Flag
    assert "Poland" in rendered or "Brazil" in rendered


def test_game_tips_exhaust():
    df = make_city_df()
    pool = Pool(PoolKind.COUNTRY, "Poland", 10)
    game = TopCitiesGame.create(df, pool)
    for _ in range(TOP_CITIES_MAX_TIPS):
        assert game.reveal_tip() is not None
    assert not game.can_give_tip
    assert game.reveal_tip() is None


def test_game_rendering(test_game):
    test_game.submit_guess("PolishCity1")
    rendered = test_game.render()
    assert "Top 10 cities in Poland" in rendered
    assert "1. *PolishCity1* - 1,900,000" in rendered
    assert "2. ???" in rendered
    assert "Strikes: ✖️ ✖️ ✖️" in rendered

    kb = test_game.build_keyboard(12345)
    assert kb is not None
    assert len(kb.inline_keyboard) == 1


def test_game_timeout_status(test_game):
    test_game.timed_out = True
    assert test_game.status == GameStatus.TIMEOUT
    assert test_game.is_over
    result_line = test_game.format_result_line(total_credits=500_000)
    assert "Time's up" in result_line


@pytest.mark.parametrize(
    "pool_kind, target, guess_city, expected_guessed_line, expected_missed_line",
    [
        (
            PoolKind.COUNTRY,
            "Poland",
            "PolishCity1",
            "1. *PolishCity1* - 1,900,000",
            "2. ❌ *PolishCity2* - 1,800,000",
        ),
        (
            PoolKind.WORLD,
            "World",
            "BrazilianCity1",
            "1. *BrazilianCity1* - 2,850,000 🇧🇷",
            "2. ❌ *BrazilianCity2* - 2,700,000 🇧🇷",
        ),
    ],
)
def test_render_end_reveals_missing_with_cross(pool_kind, target, guess_city, expected_guessed_line, expected_missed_line):
    df = make_city_df()
    game = TopCitiesGame.create(df, Pool(pool_kind, target, 10))
    game.submit_guess(guess_city)
    game.timed_out = True
    rendered = game.render(game.format_result_line())
    assert expected_guessed_line in rendered
    assert expected_missed_line in rendered


# ── Poland & District Quiz Tests ───────────────────────────────────────────────


@pytest.mark.parametrize(
    "num_cities, expected_size",
    [
        (12, 10),
        (10, 10),
        (9, 5),
        (5, 5),
        (4, 3),
        (3, 3),
        (2, None),
        (0, None),
    ],
)
def test_get_list_size_poland(num_cities, expected_size):
    # Polish cities with small populations (e.g. 10k-20k) should waive population floors
    rows = [
        {
            "city": f"Miasto{i}",
            "city_ascii": f"Miasto{i}",
            "country": "Poland",
            "iso2": "PL",
            "continent": "Europe",
            "admin_name": "Podkarpackie",
            "population": 10_000 + i * 1_000,
        }
        for i in range(num_cities)
    ]
    if rows:
        df = pd.DataFrame(rows).sort_values("population", ascending=False).reset_index(drop=True)
    else:
        df = pd.DataFrame(columns=["population"])
    assert tcq.get_list_size(df, is_poland=True) == expected_size
    # For non-poland, population < 100k would return None
    assert tcq.get_list_size(df, is_poland=False) is None


def test_get_pool_df_district():
    rows = [
        {
            "city": "Kraków",
            "city_ascii": "Krakow",
            "country": "Poland",
            "iso2": "PL",
            "continent": "Europe",
            "admin_name": "Małopolskie",
            "population": 800_000,
        },
        {
            "city": "Tarnów",
            "city_ascii": "Tarnow",
            "country": "Poland",
            "iso2": "PL",
            "continent": "Europe",
            "admin_name": "Małopolskie",
            "population": 100_000,
        },
        {
            "city": "Warszawa",
            "city_ascii": "Warszawa",
            "country": "Poland",
            "iso2": "PL",
            "continent": "Europe",
            "admin_name": "Mazowieckie",
            "population": 1_800_000,
        },
    ]
    df = pd.DataFrame(rows)

    pool = Pool(PoolKind.DISTRICT, "Małopolskie", 2, extra=(("country", "Poland"),))
    filtered = tcq.get_pool_df(df, pool)
    assert len(filtered) == 2
    assert list(filtered["city"]) == ["Kraków", "Tarnów"]

    # Test with lenient/normalized district target
    pool_norm = Pool(PoolKind.DISTRICT, "malopolskie", 2)
    filtered_norm = tcq.get_pool_df(df, pool_norm)
    assert len(filtered_norm) == 2


def test_get_pool_df_custom_district_and_letter():
    rows = [
        {
            "city": "Kraków",
            "city_ascii": "Krakow",
            "country": "Poland",
            "iso2": "PL",
            "continent": "Europe",
            "admin_name": "Małopolskie",
            "population": 800_000,
        },
        {
            "city": "Kęty",
            "city_ascii": "Kety",
            "country": "Poland",
            "iso2": "PL",
            "continent": "Europe",
            "admin_name": "Małopolskie",
            "population": 18_000,
        },
        {
            "city": "Tarnów",
            "city_ascii": "Tarnow",
            "country": "Poland",
            "iso2": "PL",
            "continent": "Europe",
            "admin_name": "Małopolskie",
            "population": 100_000,
        },
    ]
    df = pd.DataFrame(rows)

    pool = Pool(
        PoolKind.CUSTOM,
        "Małopolskie (K)",
        2,
        extra=(("district", "Małopolskie"), ("letter", "K"), ("country", "Poland")),
    )
    filtered = tcq.get_pool_df(df, pool)
    assert len(filtered) == 2
    assert list(filtered["city"]) == ["Kraków", "Kęty"]


def test_pick_random_letter():
    rows = [
        {"city": "Kraków", "population": 800_000},
        {"city": "Katowice", "population": 300_000},
        {"city": "Kielce", "population": 200_000},
        {"city": "Warszawa", "population": 1_800_000},
        {"city": "Wrocław", "population": 600_000},
        {"city": "Łódź", "population": 650_000},
        {"city": "Łomża", "population": 60_000},
        {"city": "Łowicz", "population": 25_000},
        {"city": "Gdańsk", "population": 500_000},
        {"city": "Gdynia", "population": 250_000},
        {"city": "Gliwice", "population": 180_000},
        {"city": "Gorzów", "population": 120_000},
        {"city": "Gniezno", "population": 65_000},
    ]
    df = pd.DataFrame(rows)

    # For Poland, letters with >= 3 cities are K (3), Ł (3), G (5). W has only 2 cities.
    chosen = tcq.pick_random_letter(df, is_poland=True, rng=random.Random(42))
    assert chosen in ("G", "K", "Ł")

    # For non-poland, only G has enough population & count (5 cities >= 200k/buffer)
    chosen_non_pl = tcq.pick_random_letter(df, is_poland=False, rng=random.Random(42))
    assert chosen_non_pl == "G"


def test_district_hints_in_render_slot():
    rows = [
        {
            "city": "Kraków",
            "city_ascii": "Krakow",
            "country": "Poland",
            "iso2": "PL",
            "continent": "Europe",
            "admin_name": "Małopolskie",
            "population": 800_000,
        },
        {
            "city": "Tarnów",
            "city_ascii": "Tarnow",
            "country": "Poland",
            "iso2": "PL",
            "continent": "Europe",
            "admin_name": "Małopolskie",
            "population": 100_000,
        },
        {
            "city": "Nowy Sącz",
            "city_ascii": "Nowy Sacz",
            "country": "Poland",
            "iso2": "PL",
            "continent": "Europe",
            "admin_name": "Małopolskie",
            "population": 80_000,
        },
    ]
    df = pd.DataFrame(rows)

    # 1. District quiz without letter filter -> hint shows first letter
    district_pool = Pool(PoolKind.DISTRICT, "Małopolskie", 3, extra=(("country", "Poland"),))
    game1 = TopCitiesGame.create(df, district_pool)
    game1.reveal_tip(rng=random.Random(0))
    rendered1 = game1.render()
    assert "starts with 'K'" in rendered1 or "starts with 'T'" in rendered1 or "starts with 'N'" in rendered1

    # 2. District quiz WITH letter filter -> hint shows first 2 letters
    custom_pool = Pool(
        PoolKind.CUSTOM,
        "Małopolskie (K)",
        1,
        extra=(("district", "Małopolskie"), ("letter", "K"), ("country", "Poland")),
    )
    game2 = TopCitiesGame.create(df, custom_pool)
    game2.reveal_tip(rng=random.Random(0))
    rendered2 = game2.render()
    assert "starts with 'Kr'" in rendered2


def test_format_title_district():
    rows = [
        {
            "city": "Kraków",
            "city_ascii": "Krakow",
            "country": "Poland",
            "iso2": "PL",
            "continent": "Europe",
            "admin_name": "Małopolskie",
            "population": 800_000,
        },
        {
            "city": "Tarnów",
            "city_ascii": "Tarnow",
            "country": "Poland",
            "iso2": "PL",
            "continent": "Europe",
            "admin_name": "Małopolskie",
            "population": 100_000,
        },
        {
            "city": "Nowy Sącz",
            "city_ascii": "Nowy Sacz",
            "country": "Poland",
            "iso2": "PL",
            "continent": "Europe",
            "admin_name": "Małopolskie",
            "population": 80_000,
        },
    ]
    df = pd.DataFrame(rows)
    pool = Pool(PoolKind.DISTRICT, "Małopolskie", 3, extra=(("country", "Poland"),))
    game = TopCitiesGame.create(df, pool)
    title = game.format_title()
    assert title == "🌍 Top 3 cities in Małopolskie 🇵🇱 by population"


@pytest.fixture()
def mock_cities_model():
    from unittest.mock import MagicMock

    cities = MagicMock()
    # 10 Polish cities in Małopolskie with low population (< 50k)
    pl_rows = [
        {
            "city": f"Kraków{i}",
            "city_ascii": f"Krakow{i}",
            "country": "Poland",
            "iso2": "PL",
            "continent": "Europe",
            "admin_name": "Małopolskie",
            "population": 40_000 - i * 1_000,
        }
        for i in range(10)
    ]
    # 5 Polish cities in Podkarpackie with low population
    pl_podk = [
        {
            "city": f"Rzeszów{i}",
            "city_ascii": f"Rzeszow{i}",
            "country": "Poland",
            "iso2": "PL",
            "continent": "Europe",
            "admin_name": "Podkarpackie",
            "population": 30_000 - i * 1_000,
        }
        for i in range(5)
    ]
    # 12 German cities in Bavaria with high population (>= 250k)
    de_rows = [
        {
            "city": f"München{i}",
            "city_ascii": f"Munchen{i}",
            "country": "Germany",
            "iso2": "DE",
            "continent": "Europe",
            "admin_name": "Bavaria",
            "population": 1_000_000 - i * 50_000,
        }
        for i in range(12)
    ]
    df = pd.DataFrame(pl_rows + pl_podk + de_rows)
    cities.df = df

    def resolve_c(q):
        ql = q.strip().lower()
        if ql in ("poland", "pl"):
            return ("PL", "Poland")
        if ql in ("germany", "de"):
            return ("DE", "Germany")
        return None

    def norm_cont(q):
        ql = q.strip().lower()
        if ql in ("europe", "europa"):
            return "Europe"
        return None

    def get_districts(iso2):
        sub = df[df["iso2"].str.upper() == iso2.upper()]
        return sorted(list(sub["admin_name"].unique()))

    def find_country(dist):
        norm = dist.strip().lower()
        matches = df[df["admin_name"].str.lower() == norm]
        if not matches.empty:
            return str(matches.iloc[0]["iso2"]).upper()
        return None

    cities.resolve_country.side_effect = resolve_c
    cities._normalize_continent.side_effect = norm_cont
    cities.get_districts_for_country.side_effect = get_districts
    cities.find_country_for_district.side_effect = find_country
    return cities


@pytest.mark.parametrize(
    "kwargs, expected_kind, allowed_targets, allowed_sizes",
    [
        # Poland district: 10 cities with low population (< 50k) still qualifies for Top 10!
        ({"district_raw": "Małopolskie"}, PoolKind.DISTRICT, {"Małopolskie"}, {10}),
        # Poland district with 5 cities qualifies for Top 5
        ({"district_raw": "Podkarpackie"}, PoolKind.DISTRICT, {"Podkarpackie"}, {5}),
        # Bare district defaults to random Polish district
        ({"district_raw": ""}, PoolKind.DISTRICT, {"Małopolskie", "Podkarpackie"}, {5, 10}),
        # Non-Poland district with 12 large cities qualifies for Top 10 under normal rules
        ({"country_raw": "Germany", "district_raw": "Bavaria"}, PoolKind.DISTRICT, {"Bavaria"}, {10}),
        # Bare letter in Poland selects K (10 cities) or R (5 cities)
        ({"country_raw": "Poland", "letter_raw": ""}, PoolKind.CUSTOM, {"Poland (K)", "Poland (R)"}, {5, 10}),
        # Scope district picks Polish district
        ({"scope_raw": "district"}, PoolKind.DISTRICT, {"Małopolskie", "Podkarpackie"}, {5, 10}),
        # Country + scope country strictly picks Country pool
        ({"country_raw": "Poland", "scope_raw": "country"}, PoolKind.COUNTRY, {"Poland"}, {10}),
        # Country + scope district picks a district in that country
        ({"country_raw": "Poland", "scope_raw": "district"}, PoolKind.DISTRICT, {"Małopolskie", "Podkarpackie"}, {5, 10}),
        # Country + scope letter picks a qualifying letter in that country
        ({"country_raw": "Poland", "scope_raw": "letter"}, PoolKind.CUSTOM, {"Poland (K)", "Poland (R)"}, {5, 10}),
        # Worldwide scope letter picks qualifying letter
        ({"scope_raw": "letter"}, PoolKind.LETTER, {"K", "M", "R"}, {5, 10}),
    ],
)
def test_resolve_top_cities_pool_success(mock_cities_model, kwargs, expected_kind, allowed_targets, allowed_sizes):
    pool, error = tcq.resolve_top_cities_pool(
        mock_cities_model,
        rng=random.Random(0),
        **kwargs,
    )
    assert error is None
    assert pool is not None
    assert pool.kind == expected_kind
    assert pool.target in allowed_targets
    assert pool.size in allowed_sizes


@pytest.mark.parametrize(
    "kwargs, error_substring",
    [
        ({"district_raw": "UnknownDistrict"}, "Unknown district"),
        ({"country_raw": "UnknownCountry"}, "Unknown country"),
        ({"continent_raw": "UnknownContinent"}, "Invalid --continent"),
        ({"letter_raw": "1"}, "Invalid --letter"),
        ({"scope_raw": "universe"}, "Invalid --scope"),
        ({"country_raw": "Poland", "scope_raw": "world"}, "No qualifying cities found for scope"),
    ],
)
def test_resolve_top_cities_pool_errors(mock_cities_model, kwargs, error_substring):
    pool, error = tcq.resolve_top_cities_pool(
        mock_cities_model,
        **kwargs,
    )
    assert pool is None
    assert error is not None
    assert error_substring in error


@pytest.mark.parametrize(
    "iso2, country_name, is_poland, expected_counts",
    [
        ("PL", "Poland", True, {PoolKind.COUNTRY: 1, PoolKind.DISTRICT: 2, PoolKind.CUSTOM: 2}),
        ("DE", "Germany", False, {PoolKind.COUNTRY: 1, PoolKind.DISTRICT: 1, PoolKind.CUSTOM: 1}),
        ("FR", "France", False, {PoolKind.COUNTRY: 0, PoolKind.DISTRICT: 0, PoolKind.CUSTOM: 0}),
    ],
)
def test_get_eligible_pools_for_country(mock_cities_model, iso2, country_name, is_poland, expected_counts):
    by_kind = tcq.get_eligible_pools_for_country(
        mock_cities_model,
        country_iso2=iso2,
        country_name=country_name,
        is_poland=is_poland,
    )
    for kind, expected_count in expected_counts.items():
        assert len(by_kind[kind]) == expected_count


@pytest.mark.parametrize(
    "scope, expected_kind",
    [
        ("country", PoolKind.COUNTRY),
        ("countries", PoolKind.COUNTRY),
        ("district", PoolKind.DISTRICT),
        ("districts", PoolKind.DISTRICT),
        ("letter", PoolKind.CUSTOM),
        ("letters", PoolKind.CUSTOM),
    ],
)
def test_pick_country_pool_scoped(mock_cities_model, scope, expected_kind):
    pool = tcq.pick_country_pool(
        mock_cities_model,
        country_iso2="PL",
        country_name="Poland",
        is_poland=True,
        rng=random.Random(42),
        scope=scope,
    )
    assert pool is not None
    assert pool.kind == expected_kind


def test_pick_country_pool_invalid_scope(mock_cities_model):
    pool = tcq.pick_country_pool(
        mock_cities_model,
        country_iso2="PL",
        country_name="Poland",
        is_poland=True,
        scope="world",
    )
    assert pool is None


def test_pick_country_pool_random_variety(mock_cities_model):
    picked_kinds = set()
    for seed in range(50):
        pool = tcq.pick_country_pool(
            mock_cities_model,
            country_iso2="PL",
            country_name="Poland",
            is_poland=True,
            rng=random.Random(seed),
        )
        assert pool is not None
        picked_kinds.add(pool.kind)
    assert picked_kinds == {PoolKind.COUNTRY, PoolKind.DISTRICT, PoolKind.CUSTOM}
