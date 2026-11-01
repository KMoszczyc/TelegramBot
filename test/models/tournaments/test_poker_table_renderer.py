import random
from io import BytesIO
from unittest.mock import MagicMock

import pytest
from PIL import Image

from src.config.enums import PokerAction
from src.models.tournaments.poker_table_renderer import (
    CANVAS_H,
    CANVAS_W,
    POD_H,
    POD_W,
    _draw_fitted_text,
    _get_seat_positions,
    render_poker_table,
)
from src.models.tournaments.poker_tournament import PokerTournament


@pytest.fixture
def credits_mock():
    mock = MagicMock()
    mock.credits = {i: 10_000 for i in range(1, 20)}
    return mock


@pytest.mark.parametrize("player_count", [2, 3, 5, 8])
def test_render_poker_table_player_counts(credits_mock, player_count):
    t = PokerTournament(1, 10, 1, "Player1", credits_mock, 100, 5, rng=random.Random(10))
    for i in range(2, player_count + 1):
        t.add_player(i, f"Player{i}")
    t.start_betting_round()

    png_bytes = render_poker_table(t)
    assert isinstance(png_bytes, bytes)
    assert len(png_bytes) > 1000

    img = Image.open(BytesIO(png_bytes))
    assert img.size == (CANVAS_W, CANVAS_H)
    assert img.format == "PNG"


def test_render_poker_table_across_streets_and_showdown(credits_mock):
    t = PokerTournament(1, 10, 1, "Alice", credits_mock, 100, 5, rng=random.Random(99))
    t.add_player(2, "Bob")
    t.start_betting_round()

    t.process_action(1, PokerAction.CALL)
    t.process_action(2, PokerAction.CHECK)
    flop_bytes = render_poker_table(t)
    assert len(flop_bytes) > 1000

    t.process_action(2, PokerAction.CHECK)
    t.process_action(1, PokerAction.CHECK)
    turn_bytes = render_poker_table(t)
    assert len(turn_bytes) > 1000

    t.process_action(2, PokerAction.CHECK)
    t.process_action(1, PokerAction.CHECK)
    river_bytes = render_poker_table(t)
    assert len(river_bytes) > 1000

    t.process_action(2, PokerAction.CHECK)
    t.process_action(1, PokerAction.CHECK)
    showdown_bytes = render_poker_table(t)
    assert len(showdown_bytes) > 1000


def test_render_poker_table_fold_and_flex_and_busted(credits_mock):
    t = PokerTournament(1, 10, 1, "Alice", credits_mock, 100, 5, rng=random.Random(7))
    t.add_player(2, "Bob")
    t.add_player(3, "Charlie")
    t.players[3].tournament_credits = 0
    t.start_betting_round()

    t.process_action(1, PokerAction.FOLD)
    t.reveal_winner_cards(2)
    flex_bytes = render_poker_table(t)
    assert len(flex_bytes) > 1000


@pytest.mark.parametrize("player_count", [2, 3, 4, 5, 6, 7, 8])
def test_seat_positions_no_overlap(player_count):
    positions = _get_seat_positions(player_count)
    assert len(positions) == player_count

    boxes = []
    for cx, cy in positions:
        x0, y0 = cx - POD_W // 2, cy - POD_H // 2
        x1, y1 = x0 + POD_W, y0 + POD_H
        assert x0 >= 0 and x1 <= CANVAS_W
        assert y0 >= 0 and y1 <= CANVAS_H
        # Must not overlap the center board zone (y = 225..565)
        assert y1 <= 225 or y0 >= 565
        boxes.append((x0, y0, x1, y1))

    for i in range(len(boxes)):
        for j in range(i + 1, len(boxes)):
            ax0, ay0, ax1, ay1 = boxes[i]
            bx0, by0, bx1, by1 = boxes[j]
            overlap_x = ax0 < bx1 and ax1 > bx0
            overlap_y = ay0 < by1 and ay1 > by0
            assert not (overlap_x and overlap_y), f"Pods {i} and {j} overlap for {player_count} players"


@pytest.mark.parametrize(
    "text,max_w",
    [
        ("SHORT", 168),
        ("RAISE +100000 (TOTAL: 250000)", 168),
        ("SUPER_LONG_PLAYER_USERNAME_TESTING_TRUNCATION", 120),
    ],
)
def test_draw_fitted_text_stays_within_bounds(text, max_w):
    from PIL import ImageDraw

    img = Image.new("RGB", (400, 100))
    draw = ImageDraw.Draw(img)
    _draw_fitted_text(draw, 10, 10, text, max_w, max_size=22, min_size=13, fill=(255, 255, 255), bold=True)
