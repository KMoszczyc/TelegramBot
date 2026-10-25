import random

import pandas as pd
import pytest

from src.models.quizzes import top_countries_quiz as tcq
from src.models.quizzes.top_countries_quiz import (
    GameStatus,
    GuessResult,
    Metric,
    Pool,
    PoolKind,
    TopCountriesGame,
)

NAMES = ["Alfa", "Bravo", "Charlie", "Delta", "Echo", "Foxtrot", "Golf", "Hotel", "India", "Juliet", "Kilo", "Lima"]


def make_df(count: int, continent: str = "Europa", prefix_letter: bool = False) -> pd.DataFrame:
    rows = []
    for i in range(count):
        name = f"A{NAMES[i % len(NAMES)].lower()}{i}" if prefix_letter else f"{NAMES[i % len(NAMES)]}{i}"
        rows.append(
            {
                "country_name": name,
                "country_name_en": name,
                "country_code": f"x{i}",
                "continent": continent,
                "population": 1000 * (count - i),
                "area_sq_km": 10 * (i + 1),
            }
        )
    return pd.DataFrame(rows)


@pytest.fixture()
def letter_df() -> pd.DataFrame:
    return make_df(12, continent="Europa", prefix_letter=True)


@pytest.fixture()
def game() -> TopCountriesGame:
    df = make_df(12)
    return TopCountriesGame.create(df, Metric.POPULATION, Pool(PoolKind.CONTINENT, "Europa", 10))


@pytest.mark.parametrize(
    "pool_count, expected",
    [(0, None), (4, None), (5, 3), (6, 3), (7, 5), (11, 5), (12, 10), (50, 10)],
)
def test_get_list_size(pool_count, expected):
    assert tcq.get_list_size(pool_count) == expected


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("population", Metric.POPULATION),
        ("POP", Metric.POPULATION),
        ("area", Metric.AREA),
        (" size ", Metric.AREA),
        ("gdp", None),
        (None, None),
    ],
)
def test_parse_metric(raw, expected):
    assert tcq.parse_metric(raw) == expected


def test_get_eligible_pools_covers_continents_and_letters():
    df = pd.concat([make_df(12, "Europa", prefix_letter=True), make_df(6, "Azja"), make_df(4, "Oceania")], ignore_index=True)
    pools = tcq.get_eligible_pools(df)
    continents = {p.key: p.size for p in pools if p.kind == PoolKind.CONTINENT}
    assert continents == {"Europa": 10, "Azja": 3}
    letters = {p.key: p.size for p in pools if p.kind == PoolKind.LETTER}
    assert letters == {"A": 10}


def test_get_pool_df_filters_by_continent_and_letter():
    df = pd.concat([make_df(3, "Europa"), make_df(2, "Azja")], ignore_index=True)
    assert len(tcq.get_pool_df(df, Pool(PoolKind.CONTINENT, "Azja", 3))) == 2
    assert set(tcq.get_pool_df(df, Pool(PoolKind.LETTER, "B", 3))["country_name"].str[0]) == {"B"}


def test_pick_pool_is_uniform_over_eligible_pools():
    df = pd.concat([make_df(12, "Europa"), make_df(5, "Azja")], ignore_index=True)
    eligible = tcq.get_eligible_pools(df)
    picked = {tcq.pick_pool(df, random.Random(seed)) for seed in range(200)}
    assert picked == set(eligible)


def test_create_picks_sorted_top_entries(game):
    assert game.list_size == 10
    assert [e.value for e in game.entries] == sorted([e.value for e in game.entries], reverse=True)
    assert game.entries[0].value == 12000


def test_create_by_area_uses_area_column():
    df = make_df(12)
    created = TopCountriesGame.create(df, Metric.AREA, Pool(PoolKind.CONTINENT, "Europa", 10))
    assert created.entries[0].value == 120


def test_create_random_metric_and_pool():
    created = TopCountriesGame.create(make_df(12), rng=random.Random(1))
    assert created.metric in list(Metric)
    assert created.list_size in (10, 5, 3)


@pytest.mark.parametrize(
    "list_size, correct, tips, strikes, expected",
    [
        (10, 4, 0, 0, 0),
        (10, 5, 0, 0, 5103),
        (10, 6, 0, 0, 14434),
        (10, 7, 0, 0, 26517),
        (10, 8, 0, 0, 40825),
        (10, 9, 0, 0, 57054),
        (10, 10, 0, 0, 75000),
        (10, 10, 5, 0, 52177),
        (10, 10, 0, 2, 60750),
        (10, 8, 0, 1, 36742),
        (5, 2, 0, 0, 0),
        (5, 3, 0, 0, 5774),
        (5, 5, 0, 0, 30000),
        (3, 1, 0, 0, 0),
        (3, 2, 0, 0, 3536),
        (3, 3, 0, 0, 10000),
    ],
)
def test_calculate_reward(list_size, correct, tips, strikes, expected):
    assert tcq.calculate_reward(list_size, correct, tips, strikes) == pytest.approx(expected, abs=2)


def test_reward_increases_with_correct_count():
    rewards = [tcq.calculate_reward(10, c, 0, 0) for c in range(5, 11)]
    assert rewards == sorted(rewards)
    assert len(set(rewards)) == len(rewards)


def test_next_country_is_worth_more_than_a_strike():
    for correct in range(5, 10):
        gain = tcq.calculate_reward(10, correct + 1, 0, 1) - tcq.calculate_reward(10, correct, 0, 0)
        assert gain > 0


@pytest.mark.parametrize(
    "guess, expected_result, expected_guessed, expected_strikes",
    [
        ("Kilo10", GuessResult.WRONG, 0, 1),
        ("alfa0", GuessResult.CORRECT, 1, 0),
        ("alfa", GuessResult.CORRECT, 1, 0),
        ("Echo4", GuessResult.CORRECT, 1, 0),
        ("  echo4  ", GuessResult.CORRECT, 1, 0),
        ("totally unrelated text", GuessResult.WRONG, 0, 1),
        ("x4", GuessResult.CORRECT, 1, 0),
    ],
)
def test_submit_guess(game, guess, expected_result, expected_guessed, expected_strikes):
    result = game.submit_guess(guess)
    assert result == expected_result
    assert game.correct_count == expected_guessed
    assert game.strikes == expected_strikes


def test_submit_guess_duplicate_is_ignored(game):
    assert game.submit_guess("Bravo1") == GuessResult.CORRECT
    assert game.submit_guess("Bravo1") == GuessResult.DUPLICATE
    assert game.strikes == 0
    assert game.correct_count == 1


def test_submit_guess_places_country_at_its_rank(game):
    game.submit_guess("Echo4")
    assert game.guessed == {4}
    assert "5. *Echo4* - 8,000" in game.render()


def test_three_strikes_lose_and_game_is_over(game):
    for _ in range(3):
        assert not game.is_over
        game.submit_guess("wrong guess")
    assert game.status == GameStatus.LOST
    assert game.is_over


def test_guessing_all_wins(game):
    for entry in game.entries:
        game.submit_guess(entry.name)
    assert game.status == GameStatus.WON
    assert game.reward == 75000


def test_timeout_status_only_when_not_won_or_lost(game):
    game.timed_out = True
    assert game.status == GameStatus.TIMEOUT
    for entry in game.entries:
        game.submit_guess(entry.name)
    assert game.status == GameStatus.WON


def test_sanitize_guess_strips_markdown_and_truncates():
    assert tcq.sanitize_guess("*bold* `code` \\x") == "bold code x"
    assert len(tcq.sanitize_guess("a" * 100)) == tcq.MAX_LAST_GUESS_LENGTH


def test_wrong_guess_last_guess_is_sanitized(game):
    game.submit_guess("*evil*")
    assert game.last_guess == "*evil* ❌"


def test_reveal_tip_continent_reveals_first_letter_once_per_country(game):
    revealed = [game.reveal_tip(random.Random(i)) for i in range(5)]
    assert len(set(revealed)) == 5
    assert game.tips_given == 5
    assert game.reveal_tip() is None
    for index in revealed:
        assert game.tip_letters[index] == 1


def test_reveal_tip_letter_pool_reveals_second_letter(letter_df):
    letter_game = TopCountriesGame.create(letter_df, Metric.POPULATION, Pool(PoolKind.LETTER, "A", 10))
    index = letter_game.reveal_tip()
    assert letter_game.tip_letters[index] == 1
    name = letter_game.entries[index].name
    assert f"{index + 1}. ?{name[1]}???" in letter_game.render()


def test_reveal_tip_all_countries_got_tip_reveals_next_letter():
    small = TopCountriesGame.create(make_df(5), Metric.POPULATION, Pool(PoolKind.CONTINENT, "Europa", 3))
    # 3 countries, first 3 tips give 1st letter to each
    indices = [small.reveal_tip() for _ in range(3)]
    assert len(set(indices)) == 3
    assert all(small.tip_letters[i] == 1 for i in range(3))
    # 4th tip gives 2nd letter to one of the countries
    idx4 = small.reveal_tip()
    assert idx4 in range(3)
    assert small.tip_letters[idx4] == 2
    entry = small.entries[idx4]
    assert f"{idx4 + 1}. {entry.name[:2]}???" in small.render()
    # 5th tip gives 2nd letter to another country
    idx5 = small.reveal_tip()
    assert idx5 in range(3)
    assert small.tip_letters[idx5] == 2
    assert small.tips_given == 5
    # 6th tip cannot be given (max 5 tips reached)
    assert small.reveal_tip() is None


def test_reveal_tip_letter_pool_gives_next_letter():
    df = make_df(5, prefix_letter=True)
    small = TopCountriesGame.create(df, Metric.POPULATION, Pool(PoolKind.LETTER, "A", 3))
    for _ in range(3):
        small.reveal_tip()
    assert all(small.tip_letters[i] == 1 for i in range(3))
    idx4 = small.reveal_tip()
    assert small.tip_letters[idx4] == 2
    entry = small.entries[idx4]
    assert f"{idx4 + 1}. ?{entry.name[1:3]}???" in small.render()


def test_reveal_tip_skips_guessed_countries(game):
    for entry in game.entries[:9]:
        game.submit_guess(entry.name)
    assert game.reveal_tip() == 9
    assert game.reveal_tip() == 9
    assert game.tip_letters[9] == 2


def test_reveal_tip_none_when_game_over(game):
    for _ in range(3):
        game.submit_guess("nope")
    assert game.reveal_tip() is None
    assert not game.can_give_tip()


@pytest.mark.parametrize("strikes, expected", [(0, "✖️✖️✖️"), (1, "❌✖️✖️"), (2, "❌❌✖️"), (3, "❌❌❌")])
def test_render_strikes(game, strikes, expected):
    game.strikes = strikes
    assert game.render_strikes() == expected


def test_render_initial_state(game):
    text = game.render()
    assert "Guess Top 10 most populated countries in *Europa*" in text
    assert "1. ???" in text
    assert "10. ???" in text
    assert "Strikes: ✖️✖️✖️" in text
    assert "Tips: 0/5" in text
    assert "Reward: 0 credits" in text
    assert "At least 5 correct" in text
    assert "Last guess" not in text


def test_render_area_formats_km(game):
    area_game = TopCountriesGame.create(make_df(12), Metric.AREA, Pool(PoolKind.CONTINENT, "Europa", 10))
    area_game.submit_guess(area_game.entries[0].name)
    assert f"1. *{area_game.entries[0].name}* - 120 km²" in area_game.render()
    assert "Guess Top 10 largest countries in *Europa*" in area_game.render()


def test_render_first_letter_tip(game):
    index = game.reveal_tip()
    assert f"{index + 1}. {game.entries[index].name[0].upper()}???" in game.render()


def test_render_end_reveals_missing_with_cross(game):
    game.submit_guess(game.entries[0].name)
    game.timed_out = True
    text = game.render(game.format_result_line())
    assert f"2. ❌ *{game.entries[1].name}* - " in text
    assert "Type a country name" not in text


@pytest.mark.parametrize(
    "guessed, strikes, timed_out, total, expected_parts",
    [
        (10, 0, False, 5, ["Perfect", "*75000* credits", "[*5* in total]"]),
        (6, 3, False, None, ["3 strikes", "6/10", "credits!"]),
        (3, 3, False, None, ["3 strikes", "No reward"]),
        (7, 0, True, 9, ["Time's up", "7/10", "[*9* in total]"]),
        (2, 0, True, None, ["Time's up", "No reward"]),
    ],
)
def test_format_result_line(game, guessed, strikes, timed_out, total, expected_parts):
    game.guessed = set(range(guessed))
    game.strikes = strikes
    game.timed_out = timed_out
    line = game.format_result_line(total)
    for part in expected_parts:
        assert part in line


def test_tip_label_shows_reward_after_next_tip(game):
    for entry in game.entries[:5]:
        game.submit_guess(entry.name)
    assert game.build_tip_label() == f"💡 Tip 1/5 (reward → {tcq.calculate_reward(10, 5, 1, 0)})"


def test_keyboard_has_tip_button_until_tips_run_out(game):
    keyboard = game.build_keyboard(111)
    assert keyboard.inline_keyboard[0][0].callback_data == "toptip:111"
    for _ in range(5):
        game.reveal_tip()
    assert game.build_keyboard(111).inline_keyboard == ()


@pytest.mark.parametrize(
    "data, expected",
    [("toptip:111", 111), ("toptip:abc", None), ("quiztip:111", None), (None, None), ("", None)],
)
def test_parse_tip_callback_data(data, expected):
    assert tcq.parse_tip_callback_data(data) == expected


def test_build_tip_callback_data_round_trips():
    assert tcq.parse_tip_callback_data(tcq.build_tip_callback_data(42)) == 42


@pytest.mark.parametrize(
    "value, metric, expected",
    [(1404890000, Metric.POPULATION, "1,404,890,000"), (9984670, Metric.AREA, "9,984,670 km²")],
)
def test_format_value(value, metric, expected):
    assert tcq.format_value(value, metric) == expected


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("en", tcq.Language.EN),
        ("EN", tcq.Language.EN),
        ("english", tcq.Language.EN),
        ("pl", tcq.Language.PL),
        ("PL", tcq.Language.PL),
        ("polish", tcq.Language.PL),
        ("de", None),
        ("", None),
        (None, None),
    ],
)
def test_parse_language(raw, expected):
    assert tcq.parse_language(raw) == expected


def test_format_title_letter_pool(letter_df):
    game = TopCountriesGame.create(letter_df, Metric.POPULATION, pool=Pool(PoolKind.LETTER, "A", 10), language=tcq.Language.EN)
    assert game.format_title() == 'Guess Top 10 most populated countries that start with letter *"A"*'
    assert 'Guess Top 10 most populated countries that start with letter *"A"*' in game.render()

    area_game = TopCountriesGame.create(letter_df, Metric.AREA, pool=Pool(PoolKind.LETTER, "A", 5), language=tcq.Language.EN)
    assert area_game.format_title() == 'Guess Top 5 largest countries that start with letter *"A"*'


def test_both_english_and_polish_names_accepted_as_guesses():
    df = pd.DataFrame(
        [
            {
                "country_name": "Hiszpania",
                "country_name_en": "Spain",
                "country_code": "es",
                "continent": "Europa",
                "population": 47000000,
                "area_sq_km": 505990,
            }
        ]
        * 12
    )
    # Modify one entry
    df.loc[0, "country_name"] = "Polska"
    df.loc[0, "country_name_en"] = "Poland"
    df.loc[0, "country_code"] = "pl"

    en_game = TopCountriesGame.create(df, Metric.POPULATION, pool=Pool(PoolKind.CONTINENT, "Europa", 10), language=tcq.Language.EN)
    # Guessing with Polish name works in English game
    assert en_game.submit_guess("polska") == GuessResult.CORRECT
    # The entry name displayed is in English
    assert en_game.entries[0].name == "Poland"
    assert "*Poland* ✅" in en_game.last_guess

    pl_game = TopCountriesGame.create(df, Metric.POPULATION, pool=Pool(PoolKind.CONTINENT, "Europa", 10), language=tcq.Language.PL)
    # Guessing with English name works in Polish game
    assert pl_game.submit_guess("poland") == GuessResult.CORRECT
    # The entry name displayed is in Polish
    assert pl_game.entries[0].name == "Polska"
    assert "*Polska* ✅" in pl_game.last_guess
