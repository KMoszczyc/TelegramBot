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

    # Check stopwords are filtered and each entry is a (word, source_line) pair
    for ds in pools:
        for _length, pairs in pools[ds].items():
            words = [w for w, _line in pairs]
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

    word, quote, ds = WordleDatasetLoader.get_random_word(dataset="bartosiak", length=7, assets=mock_assets)
    assert ds == "bartosiak"
    assert len(word) == 7
    assert word == "drabina"
    assert "drabina" in quote

    # Default length is 5
    word2, quote2, ds2 = WordleDatasetLoader.get_random_word(assets=mock_assets)
    assert len(word2) == 5
    assert ds2 in SUPPORTED_DATASETS
    assert quote2  # source quote must be non-empty
