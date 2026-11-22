import pytest

from src.models.quizzes.wordle_game import WordleGame
from src.models.quizzes.wordle_presenter import WordlePresenter


def test_build_initial_caption():
    game = WordleGame(target_word="robot", dataset_key="bible")
    caption = WordlePresenter.build_initial_caption(game)
    assert "WORDLE" in caption
    assert "Bible" in caption
    assert len(caption) <= 1024


def test_build_in_progress_caption_without_tips():
    game = WordleGame(target_word="robot", dataset_key="bible", source_quote="krótki cytat")
    caption = WordlePresenter.build_in_progress_caption(game)
    assert "WORDLE" in caption
    assert "Type your next guess" in caption
    assert "Tip" not in caption
    assert len(caption) <= 1024


def test_build_in_progress_caption_with_tips():
    game = WordleGame(target_word="robot", dataset_key="bible", source_quote="krótki cytat z robotem w treści")
    game.revealed_tip_tier = 1
    caption = WordlePresenter.build_in_progress_caption(game)
    assert "💡 *Tip:*" in caption
    assert "\\[\\_\\_\\_\\_\\_\\]" in caption
    assert len(caption) <= 1024


@pytest.mark.parametrize(
    "caption_method",
    [
        "build_win_caption",
        "build_game_over_caption",
        "build_surrender_caption",
        "build_timeout_caption",
    ],
)
def test_long_quote_caption_clamped_below_1024(caption_method: str):
    # Construct a massive 1500+ character quote with special characters
    long_quote = (
        ("Początek długiego cytatu o wielu sprawach. " * 25)
        + "Wreszcie pojawia się tutaj nasz poszukiwany robot i działa. "
        + ("Koniec długiego cytatu o wielu sprawach i zagadnieniach. " * 25)
    )
    assert len(long_quote) > 1500

    game = WordleGame(
        target_word="robot",
        dataset_key="europejskafirma",
        source_quote=long_quote,
        source_attribution="Jerzy Krajewski",
    )

    if caption_method == "build_win_caption":
        caption = WordlePresenter.build_win_caption(game, reward=8000, user_credits=15000)
    elif caption_method == "build_game_over_caption":
        caption = WordlePresenter.build_game_over_caption(game)
    elif caption_method == "build_surrender_caption":
        caption = WordlePresenter.build_surrender_caption(game)
    else:
        caption = WordlePresenter.build_timeout_caption(game)

    assert len(caption) <= 1024
    # Ensure target word was highlighted and preserved in the centered window
    assert "*robot*" in caption
    assert "Jerzy Krajewski" in caption


def test_long_quote_final_tip_clamped():
    long_quote = (
        ("Początek długiego cytatu o wielu sprawach. " * 25)
        + "Wreszcie pojawia się tutaj nasz poszukiwany robot i działa. "
        + ("Koniec długiego cytatu o wielu sprawach i zagadnieniach. " * 25)
    )
    game = WordleGame(
        target_word="robot",
        dataset_key="europejskafirma",
        source_quote=long_quote,
        source_attribution="Jerzy Krajewski",
    )
    game.revealed_tip_tier = 2
    caption = WordlePresenter.build_in_progress_caption(game)
    assert len(caption) <= 1024
    assert "Final Tip" in caption
    assert "\\[\\_\\_\\_\\_\\_\\]" in caption


def test_keyboard_transitions():
    user_id = 999
    game = WordleGame(target_word="robot", dataset_key="bible", source_quote="test quote")

    # Initial locked
    kb0 = WordlePresenter.build_keyboard(user_id, game)
    assert "🔒 Tip (guess 4)" in kb0.inline_keyboard[0][0].text

    # Unlocked tier 1
    for _ in range(3):
        game.submit_guess("testy")
    kb1 = WordlePresenter.build_keyboard(user_id, game)
    assert kb1.inline_keyboard[0][0].text == "💡 Tip"

    # Revealed tier 1 -> Tier 2 locked
    game.reveal_tip()
    kb2 = WordlePresenter.build_keyboard(user_id, game)
    assert "🔒 Final Tip (last guess)" in kb2.inline_keyboard[0][0].text

    # Last attempt -> Tier 2 unlocked
    for _ in range(2):
        game.submit_guess("testy")
    assert game.remaining_tries == 1
    kb3 = WordlePresenter.build_keyboard(user_id, game)
    assert kb3.inline_keyboard[0][0].text == "💡 Final Tip"

    # Revealed tier 2 -> No tip button
    game.reveal_tip()
    kb4 = WordlePresenter.build_keyboard(user_id, game)
    assert len(kb4.inline_keyboard[0]) == 1
    assert kb4.inline_keyboard[0][0].text == "🏳️ Surrender"
