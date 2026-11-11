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
