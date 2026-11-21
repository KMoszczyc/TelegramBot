import pytest
from PIL import Image

from src.models.quizzes.wordle_game import WordleGame
from src.models.quizzes.wordle_renderer import render_wordle_board


@pytest.mark.parametrize(
    ("word", "max_tries"),
    [
        ("koty", 5),
        ("świat", 6),
        ("konflikt", 9),
    ],
)
def test_render_wordle_board_dimensions(word: str, max_tries: int):
    game = WordleGame(target_word=word, dataset_key="bible", source_quote="test quote", max_tries=max_tries)
    buf = render_wordle_board(game)
    assert buf is not None

    img = Image.open(buf)
    assert img.format == "PNG"

    grid_width = len(word) * 64 + (len(word) - 1) * 8
    grid_height = max_tries * 64 + (max_tries - 1) * 8
    kb_width = 10 * 34 + 9 * 4
    kb_height = 4 * 42 + 3 * 6
    expected_w = 24 * 2 + max(grid_width, kb_width)
    expected_h = 24 * 2 + grid_height + 24 + kb_height

    assert img.size == (expected_w, expected_h)


def test_render_wordle_board_with_guesses():
    game = WordleGame(target_word="świat", dataset_key="bible", source_quote="Na początku było słowo")
    game.submit_guess("kwiat")
    game.submit_guess("świat")

    buf = render_wordle_board(game)
    img = Image.open(buf)
    assert img.format == "PNG"
    assert img.size[0] > 0
    assert img.size[1] > 0
