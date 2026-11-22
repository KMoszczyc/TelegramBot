from unittest.mock import MagicMock

import pytest

from src.models.quizzes.wordle_game import (
    DATASET_NAMES,
    SUPPORTED_DATASETS,
    LetterState,
    WordleDatasetLoader,
    WordleGame,
    WordleStatus,
)


@pytest.mark.parametrize(
    ("guess", "target", "expected"),
    [
        (
            "pies",
            "pies",
            [LetterState.CORRECT, LetterState.CORRECT, LetterState.CORRECT, LetterState.CORRECT],
        ),
        (
            "kota",
            "pies",
            [LetterState.ABSENT, LetterState.ABSENT, LetterState.ABSENT, LetterState.ABSENT],
        ),
        (
            "kosa",
            "soka",
            [LetterState.PRESENT, LetterState.CORRECT, LetterState.PRESENT, LetterState.CORRECT],
        ),
        (
            "civic",
            "light",
            [LetterState.ABSENT, LetterState.CORRECT, LetterState.ABSENT, LetterState.ABSENT, LetterState.ABSENT],
        ),
        (
            "bloom",
            "robot",
            [LetterState.PRESENT, LetterState.ABSENT, LetterState.PRESENT, LetterState.CORRECT, LetterState.ABSENT],
        ),
        (
            "łódź",
            "lodz",
            [LetterState.ABSENT, LetterState.ABSENT, LetterState.CORRECT, LetterState.ABSENT],
        ),
        (
            "łódź",
            "kasa",
            [LetterState.ABSENT, LetterState.ABSENT, LetterState.ABSENT, LetterState.ABSENT],
        ),
        (
            "łódź",
            "łódź",
            [LetterState.CORRECT, LetterState.CORRECT, LetterState.CORRECT, LetterState.CORRECT],
        ),
        (
            "PIES",
            "pies",
            [LetterState.CORRECT, LetterState.CORRECT, LetterState.CORRECT, LetterState.CORRECT],
        ),
        (
            "radar",
            "karma",
            [LetterState.PRESENT, LetterState.CORRECT, LetterState.ABSENT, LetterState.PRESENT, LetterState.ABSENT],
        ),
    ],
)
def test_evaluate_guess(guess: str, target: str, expected: list[LetterState]):
    result = WordleGame.evaluate_guess(guess, target)
    assert result == expected


def test_game_initial_state():
    game = WordleGame(target_word="świat", dataset_key="bible")
    assert game.target_word == "świat"
    assert game.word_length == 5
    assert game.max_tries == 6
    assert game.remaining_tries == 6
    assert not game.is_over
    assert not game.is_won
    assert game.guesses == []
    assert game.evaluations == []
    assert game.dataset_display_name == "Bible"
    assert game.source_quote == ""


def test_game_stores_source_quote():
    quote = "Na początku było słowo i słowo było u Boga."
    game = WordleGame(target_word="słowo", dataset_key="bible", source_quote=quote)
    assert game.source_quote == quote


def test_game_submit_guess_length_validation():
    game = WordleGame(target_word="świat", dataset_key="bible")
    success, err = game.submit_guess("za")
    assert not success
    assert "exactly 5 letters" in err
    assert len(game.guesses) == 0

    success, err = game.submit_guess("za_dużo_liter")
    assert not success
    assert len(game.guesses) == 0


def test_game_submit_guess_character_validation():
    game = WordleGame(target_word="świat", dataset_key="bible")
    success, err = game.submit_guess("12345")
    assert not success
    assert "only contain letters" in err
    assert len(game.guesses) == 0


def test_game_winning_flow():
    game = WordleGame(target_word="świat", dataset_key="bible")
    success, err = game.submit_guess("kwiat")
    assert success
    assert err is None
    assert len(game.guesses) == 1
    assert game.remaining_tries == 5
    assert not game.is_over
    assert not game.is_won

    success, err = game.submit_guess("świat")
    assert success
    assert game.is_over
    assert game.is_won
    assert game.status == WordleStatus.WON

    # Submitting after game over
    success, err = game.submit_guess("świat")
    assert not success
    assert "already over" in err


def test_game_losing_flow():
    game = WordleGame(target_word="koty", dataset_key="bartosiak", max_tries=2)
    game.submit_guess("pasy")
    assert not game.is_over
    game.submit_guess("domy")
    assert game.is_over
    assert not game.is_won
    assert game.status == WordleStatus.LOST


def test_game_surrender():
    game = WordleGame(target_word="traktat", dataset_key="bartosiak")
    game.surrender()
    assert game.is_over
    assert not game.is_won
    assert game.status == WordleStatus.SURRENDERED
    assert game.calculate_reward() == 0


@pytest.mark.parametrize(
    ("word_length", "remaining_tries", "is_won", "expected_reward"),
    [
        (5, 4, True, 8000 + 4 * 1500 + (5 - 4) * 1000),  # 15000
        (4, 0, True, 8000 + 0 * 1500 + 0),  # 8000
        (8, 5, True, 8000 + 5 * 1500 + 4 * 1000),  # 23500
        (5, 2, False, 0),  # 0 (lost)
    ],
)
def test_calculate_reward(word_length: int, remaining_tries: int, is_won: bool, expected_reward: int):
    dummy_word = "a" * word_length
    game = WordleGame(target_word=dummy_word, dataset_key="ozjasz", max_tries=remaining_tries + 1)
    if is_won:
        game.status = WordleStatus.WON
        # Simulate having used 1 try so remaining_tries matches
        game.guesses = [dummy_word]
    else:
        game.status = WordleStatus.LOST

    assert game.calculate_reward() == expected_reward


def test_dataset_loader_with_mock_assets():
    mock_assets = MagicMock()
    mock_assets.polish_stopwords = ["i", "w", "nie", "się"]
    mock_assets.bible_df = None
    mock_assets.quran_df = None
    mock_assets.ozjasz_phrases = ["Wolny rynek jest wspaniały i niezastąpiony."]
    mock_assets.bartosiak_phrases = ["Geopolityka tworzy lewar i teatr działań."]
    mock_assets.europejskafirma_phrases = ["Przedsiębiorca zarabia kapitał."]

    # Reset cached pools for isolation
    WordleDatasetLoader._cached_pools = None
    pools = WordleDatasetLoader.load_words(mock_assets)

    assert "ozjasz" in pools
    assert "bartosiak" in pools
    for ds in SUPPORTED_DATASETS:
        assert ds in DATASET_NAMES

    # Check stopwords are filtered and each entry is a (word, source_line, attribution) triplet
    for ds in pools:
        for _length, triplets in pools[ds].items():
            words = [triplet[0] for triplet in triplets]
            assert "i" not in words
            assert "w" not in words
            assert "nie" not in words
            for w in words:
                assert 4 <= len(w) <= 8


def test_get_keyboard_states():
    game = WordleGame(target_word="robot", dataset_key="bible")
    assert game.get_keyboard_states() == {}

    game.submit_guess("bloom")
    states = game.get_keyboard_states()
    assert states["B"] == LetterState.PRESENT
    assert states["L"] == LetterState.ABSENT
    assert states["O"] == LetterState.CORRECT
    assert states["M"] == LetterState.ABSENT

    game.submit_guess("robot")
    states = game.get_keyboard_states()
    assert states["R"] == LetterState.CORRECT
    assert states["O"] == LetterState.CORRECT
    assert states["B"] == LetterState.CORRECT  # upgraded from PRESENT to CORRECT
    assert states["T"] == LetterState.CORRECT


def test_get_random_word():
    mock_assets = MagicMock()
    mock_assets.polish_stopwords = ["i"]
    mock_assets.bible_df = None
    mock_assets.quran_df = None
    mock_assets.ozjasz_phrases = ["korwin wolny rynek własność"]
    mock_assets.bartosiak_phrases = ["drabina eskalacji teatr"]
    mock_assets.europejskafirma_phrases = ["firma kapitał"]

    WordleDatasetLoader._cached_pools = None

    word, quote, ds, attr = WordleDatasetLoader.get_random_word(dataset="bartosiak", length=7, assets=mock_assets)
    assert ds == "bartosiak"
    assert len(word) == 7
    assert word == "drabina"
    assert "drabina" in quote
    assert attr == "Bartosiak"

    # Default length is 5
    word2, quote2, ds2, attr2 = WordleDatasetLoader.get_random_word(assets=mock_assets)
    assert len(word2) == 5
    assert ds2 in SUPPORTED_DATASETS
    assert quote2  # source quote must be non-empty
    assert attr2


@pytest.mark.parametrize(
    ("row", "expected"),
    [
        ({"abbreviation": "Mt", "chapter": 5, "verse": "3"}, "Biblia (Mt 5, 3)"),
        ({"abbreviation": "Rdz", "chapter": 1, "verse": "1"}, "Biblia (Rdz 1, 1)"),
        ({"invalid": "data"}, "Biblia"),
        (None, "Biblia"),
    ],
)
def test_format_bible_attribution(row, expected: str):
    from src.models.quizzes.wordle_game import _format_bible_attribution

    assert _format_bible_attribution(row) == expected


@pytest.mark.parametrize(
    ("row", "expected"),
    [
        ({"chapter_nr": 1, "verse": "2"}, "Koran (1:2)"),
        ({"chapter_nr": 114, "verse": "6"}, "Koran (114:6)"),
        ({"invalid": "data"}, "Koran"),
        (None, "Koran"),
    ],
)
def test_format_quran_attribution(row, expected: str):
    from src.models.quizzes.wordle_game import _format_quran_attribution

    assert _format_quran_attribution(row) == expected


@pytest.mark.parametrize(
    ("quote", "word", "tier", "expected_pattern"),
    [
        (
            "Gotuję ryby zbieram szumowinę dorzucam pokrojone buraki marchewkę pietruszkę",
            "buraki",
            1,
            "... dorzucam pokrojone [_____] marchewkę pietruszkę",
        ),
        (
            "Na początku Bóg stworzył niebo i ziemię.",
            "początku",
            1,
            "Na [_____] Bóg stworzył ...",
        ),
        (
            "Idę kupić świeże buraki.",
            "buraki",
            1,
            "... kupić świeże [_____].",
        ),
        (
            "Gdybym miał buraki, to bym zjadł.",
            "buraki",
            1,
            "Gdybym miał [_____], to bym ...",
        ),
        (
            "Na początku Bóg stworzył niebo i ziemię.",
            "początku",
            2,
            "Na [_____] Bóg stworzył niebo i ziemię.",
        ),
        ("", "słowo", 1, ""),
        ("Krótka sentencja.", "", 1, ""),
    ],
)
def test_generate_tip(quote: str, word: str, tier: int, expected_pattern: str):
    from src.models.quizzes.wordle_game import generate_tip

    result = generate_tip(quote, word, tier)
    assert result == expected_pattern


@pytest.mark.parametrize(
    ("word_len", "expected_max_tries", "tier1_unlock_guess", "tier2_unlock_guess"),
    [
        (4, 5, 2, 4),  # midpoint 5 // 2 = 2 guesses made; 1 try remaining at 4 guesses made
        (5, 6, 3, 5),  # midpoint 6 // 2 = 3 guesses made; 1 try remaining at 5 guesses made
        (6, 7, 3, 6),  # midpoint 7 // 2 = 3 guesses made; 1 try remaining at 6 guesses made
        (7, 8, 4, 7),  # midpoint 8 // 2 = 4 guesses made; 1 try remaining at 7 guesses made
    ],
)
def test_dynamic_tip_unlock_thresholds(word_len: int, expected_max_tries: int, tier1_unlock_guess: int, tier2_unlock_guess: int):
    target = "a" * word_len
    dummy_guess = "b" * word_len
    game = WordleGame(target_word=target, dataset_key="bible", source_quote="test quote here")
    assert game.max_tries == expected_max_tries
    assert game.available_tip_tier == 0
    assert not game.can_reveal_tip

    # Submit guesses up to tier 1 unlock
    for _ in range(tier1_unlock_guess - 1):
        game.submit_guess(dummy_guess)
        assert game.available_tip_tier == 0

    game.submit_guess(dummy_guess)
    assert game.available_tip_tier == 1
    assert game.can_reveal_tip

    # Reveal tier 1
    tip = game.reveal_tip()
    assert tip is not None
    assert game.revealed_tip_tier == 1
    assert not game.can_reveal_tip  # already revealed tier 1

    # Submit guesses until final guess (tier 2 unlock)
    while len(game.guesses) < tier2_unlock_guess:
        game.submit_guess(dummy_guess)

    assert game.available_tip_tier == 2
    assert game.can_reveal_tip

    # Reveal tier 2
    final_tip = game.reveal_tip()
    assert final_tip is not None
    assert game.revealed_tip_tier == 2
    assert not game.can_reveal_tip


@pytest.mark.parametrize(
    ("revealed_tier", "expected_multiplier"),
    [
        (0, 1.0),
        (1, 0.75),
        (2, 0.50),
    ],
)
def test_tip_reward_penalty(revealed_tier: int, expected_multiplier: float):
    game = WordleGame(target_word="robot", dataset_key="bible")
    game.submit_guess("robot")  # win on first try
    assert game.is_won

    # Calculate unpenalized reward
    game.revealed_tip_tier = 0
    base_reward = game.calculate_reward()

    game.revealed_tip_tier = revealed_tier
    reward = game.calculate_reward()
    assert reward == int(base_reward * expected_multiplier)
