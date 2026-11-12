import random

import pytest

from src.models.quizzes.base_top_list_quiz import (
    EMPTY_STRIKE_ICON,
    MAX_LAST_GUESS_LENGTH,
    STRIKE_ICON,
    UNKNOWN_SLOT,
    BaseTopListGame,
    GameStatus,
    GuessResult,
    Pool,
    PoolKind,
    build_tip_callback_data,
    calculate_top_reward,
    parse_guesses,
    parse_tip_callback_data,
    render_strikes,
    sanitize_guess,
)

# ── Concrete subclass for testing BaseTopListGame ────────────────────────────


class DummyTopGame(BaseTopListGame):
    def __init__(self, pool: Pool, items: list[str], max_strikes: int = 3, max_tips: int = 2, strike_separator: str = ""):
        super().__init__(
            pool=pool,
            entries=items,
            max_strikes=max_strikes,
            max_tips=max_tips,
            tip_callback_prefix="dummy_tip_",
            strike_separator=strike_separator,
        )

    @property
    def reward(self) -> int:
        return 100 if self.correct_count >= self.min_correct else 0

    def submit_guess(self, text: str) -> GuessResult:
        clean = text.strip().lower()
        for idx, item in enumerate(self.entries):
            if item.lower() == clean:
                if idx in self.guessed:
                    return GuessResult.DUPLICATE
                self.guessed.add(idx)
                self.last_guess = f"*{item}* ✅"
                return GuessResult.CORRECT
        self.strikes += 1
        clean_g = sanitize_guess(text)
        self.last_guess = f"*{clean_g}* {STRIKE_ICON}" if clean_g else STRIKE_ICON
        return GuessResult.WRONG

    def reveal_tip(self, rng: random.Random | None = None) -> int | None:
        if not self.can_give_tip:
            return None
        missing = self._missing_indices()
        chosen = missing[0]
        self.tips_given += 1
        return chosen

    def format_title(self) -> str:
        return f"Dummy Quiz: {self.pool.description}"

    def _render_slot(self, index: int) -> str:
        num = index + 1
        if index in self.guessed or self.is_over:
            return f"{num}. {self.entries[index]}"
        return f"{num}. {UNKNOWN_SLOT}"


# ── Pool Tests ───────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "kind,target,extra,expected_key,expected_desc",
    [
        (PoolKind.WORLD, "World", (), "World", "Scope: World"),
        (PoolKind.CONTINENT, "Europe", (), "Europe", "Continent: Europe"),
        (PoolKind.COUNTRY, "Poland", (), "Poland", "Country: Poland"),
        (PoolKind.DISTRICT, "Małopolskie", (), "Małopolskie", "District: Małopolskie"),
        (PoolKind.LETTER, "B", (), "B", "Starting letter: B"),
        (
            PoolKind.CUSTOM,
            "Europe (B)",
            (("continent", "Europe"), ("letter", "B")),
            "Europe (B)",
            "Scope: Europe (B)",
        ),
    ],
)
def test_pool_properties(kind, target, extra, expected_key, expected_desc):
    pool = Pool(kind=kind, target=target, size=10, extra=extra)
    assert pool.key == expected_key
    assert pool.description == expected_desc
    if extra:
        assert pool.extra_dict == dict(extra)
    else:
        assert pool.extra_dict == {}


def test_pool_hashable():
    pool1 = Pool(PoolKind.CONTINENT, "Asia", 5)
    pool2 = Pool(PoolKind.CONTINENT, "Asia", 5)
    pool_set = {pool1, pool2}
    assert len(pool_set) == 1


# ── Utility Function Tests ───────────────────────────────────────────────────


@pytest.mark.parametrize(
    "text,expected",
    [
        ("", []),
        ("   ", []),
        ("Berlin", ["Berlin"]),
        ("Berlin, Paris, Madrid", ["Berlin", "Paris", "Madrid"]),
        ("  Rome ,  , Tokyo  ", ["Rome", "Tokyo"]),
    ],
)
def test_parse_guesses(text, expected):
    assert parse_guesses(text) == expected


@pytest.mark.parametrize(
    "text,expected",
    [
        ("Normal text", "Normal text"),
        ("*bold* `code` \\slash", "bold code slash"),
        ("a" * 50, "a" * MAX_LAST_GUESS_LENGTH),
        ("   padded   ", "padded"),
    ],
)
def test_sanitize_guess(text, expected):
    assert sanitize_guess(text) == expected


@pytest.mark.parametrize(
    "strikes,max_strikes,sep,expected",
    [
        (0, 3, "", f"{EMPTY_STRIKE_ICON * 3}"),
        (1, 3, "", f"{STRIKE_ICON}{EMPTY_STRIKE_ICON * 2}"),
        (3, 3, "", f"{STRIKE_ICON * 3}"),
        (1, 3, " ", f"{STRIKE_ICON} {EMPTY_STRIKE_ICON} {EMPTY_STRIKE_ICON}"),
    ],
)
def test_render_strikes(strikes, max_strikes, sep, expected):
    assert render_strikes(strikes, max_strikes, separator=sep) == expected


def test_calculate_top_reward():
    base = {10: 500, 5: 250, 3: 100}
    # Below min_correct (needs ceil(10/2) = 5)
    assert calculate_top_reward(10, 4, 0, 0, base, 0.75, 0.7, 1.3) == 0
    # Minimum qualifying
    reward_min = calculate_top_reward(10, 5, 0, 0, base, 0.75, 0.7, 1.3)
    assert reward_min > 0
    # Perfect score yields max base reward
    reward_full = calculate_top_reward(10, 10, 0, 0, base, 0.75, 0.7, 1.3)
    assert reward_full == 500
    # Penalties reduce reward
    reward_with_penalties = calculate_top_reward(10, 10, 1, 1, base, 0.75, 0.7, 1.3)
    assert 0 < reward_with_penalties < reward_full


@pytest.mark.parametrize(
    "prefix,user_id,raw_data,expected_parsed",
    [
        ("prefix_", 12345, "prefix_12345", 12345),
        ("prefix_", 12345, "wrong_12345", None),
        ("prefix_", 12345, None, None),
        ("prefix_", 12345, "prefix_abc", None),
    ],
)
def test_tip_callback_data(prefix, user_id, raw_data, expected_parsed):
    if raw_data == f"{prefix}{user_id}":
        assert build_tip_callback_data(prefix, user_id) == raw_data
    assert parse_tip_callback_data(prefix, raw_data) == expected_parsed


# ── BaseTopListGame Tests ────────────────────────────────────────────────────


@pytest.fixture
def dummy_game():
    pool = Pool(PoolKind.CONTINENT, "Europe", 4)
    items = ["Alpha", "Beta", "Gamma", "Delta"]
    return DummyTopGame(pool, items, max_strikes=3, max_tips=2)


def test_game_initial_state(dummy_game):
    assert dummy_game.list_size == 4
    assert dummy_game.correct_count == 0
    assert dummy_game.min_correct == 2
    assert dummy_game.status == GameStatus.IN_PROGRESS
    assert not dummy_game.is_over
    assert dummy_game.can_give_tip
    assert dummy_game._missing_indices() == [0, 1, 2, 3]


def test_game_win_flow(dummy_game):
    results = dummy_game.submit_guesses("Alpha, Beta, Gamma, Delta")
    assert results == [GuessResult.CORRECT] * 4
    assert dummy_game.status == GameStatus.WON
    assert dummy_game.is_over
    assert not dummy_game.can_give_tip
    assert "Congratulations" in dummy_game.format_result_line(total_credits=1000)


def test_game_lose_flow(dummy_game):
    results = dummy_game.submit_guesses("Wrong1, Wrong2, Wrong3")
    assert results == [GuessResult.WRONG] * 3
    assert dummy_game.status == GameStatus.LOST
    assert dummy_game.is_over
    assert not dummy_game.can_give_tip
    assert "Game Over" in dummy_game.format_result_line()


def test_game_timeout_flow(dummy_game):
    dummy_game.timed_out = True
    assert dummy_game.status == GameStatus.TIMEOUT
    assert dummy_game.is_over
    assert "Time's up" in dummy_game.format_result_line()


def test_game_batch_guesses_stops_on_over(dummy_game):
    results = dummy_game.submit_guesses("Wrong1, Wrong2, Wrong3, ShouldNotBeEvaluated")
    assert len(results) == 3
    assert dummy_game.is_over


def test_game_duplicate_guess(dummy_game):
    res1 = dummy_game.submit_guess("Alpha")
    res2 = dummy_game.submit_guess("Alpha")
    assert res1 == GuessResult.CORRECT
    assert res2 == GuessResult.DUPLICATE


def test_game_tips_flow(dummy_game):
    assert dummy_game.can_give_tip
    t1 = dummy_game.reveal_tip()
    assert t1 == 0
    assert dummy_game.tips_given == 1
    assert dummy_game.can_give_tip

    t2 = dummy_game.reveal_tip()
    assert t2 == 0
    assert dummy_game.tips_given == 2
    assert not dummy_game.can_give_tip
    assert dummy_game.reveal_tip() is None


def test_game_render(dummy_game):
    dummy_game.submit_guess("Alpha")
    rendered = dummy_game.render()
    assert "Dummy Quiz: Continent: Europe" in rendered
    assert "1. Alpha" in rendered
    assert "2. ???" in rendered
    assert "Strikes: ✖️✖️✖️" in rendered
    assert "Last guess: *Alpha* ✅" in rendered


def test_game_build_keyboard(dummy_game):
    kb = dummy_game.build_keyboard(user_id=123)
    assert kb is not None
    assert len(kb.inline_keyboard) == 1
    assert kb.inline_keyboard[0][0].callback_data == "dummy_tip_123"

    dummy_game.tips_given = dummy_game.max_tips
    assert dummy_game.build_keyboard(user_id=123) is None
