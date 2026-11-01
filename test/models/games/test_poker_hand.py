import random

import pytest

from src.config.enums import PokerHandRank
from src.models.games.poker_hand import Card, create_shuffled_deck, evaluate_best_hand


def _parse_cards(spec: str) -> list[Card]:
    rank_map = {"2": 2, "3": 3, "4": 4, "5": 5, "6": 6, "7": 7, "8": 8, "9": 9, "10": 10, "J": 11, "Q": 12, "K": 13, "A": 14}
    cards = []
    for token in spec.split():
        suit = token[-1]
        rank_str = token[:-1]
        cards.append(Card(rank=rank_map[rank_str], suit=suit))
    return cards


def test_create_shuffled_deck():
    rng = random.Random(42)
    deck = create_shuffled_deck(rng)
    assert len(deck) == 52
    assert len(set(deck)) == 52


@pytest.mark.parametrize(
    "card,expected_rank_str,expected_emoji,expected_short,expected_red",
    [
        (Card(14, "♠"), "A", "A♠️", "A♠", False),
        (Card(10, "♥"), "10", "10♥️", "10♥", True),
        (Card(12, "♦"), "Q", "Q♦️", "Q♦", True),
        (Card(2, "♣"), "2", "2♣️", "2♣", False),
    ],
)
def test_card_properties(card, expected_rank_str, expected_emoji, expected_short, expected_red):
    assert card.rank_str == expected_rank_str
    assert card.emoji_str == expected_emoji
    assert card.short_str == expected_short
    assert card.is_red is expected_red


@pytest.mark.parametrize(
    "cards_spec,expected_rank,expected_desc_substr",
    [
        ("A♠ K♠ Q♠ J♠ 10♠ 2♥ 3♦", PokerHandRank.ROYAL_FLUSH, "Royal Flush"),
        ("9♥ 8♥ 7♥ 6♥ 5♥ A♠ K♦", PokerHandRank.STRAIGHT_FLUSH, "Straight Flush (Nine high)"),
        ("A♣ 2♣ 3♣ 4♣ 5♣ K♠ Q♦", PokerHandRank.STRAIGHT_FLUSH, "Straight Flush (Five high)"),
        ("A♠ A♥ A♦ A♣ K♠ 2♥ 3♦", PokerHandRank.FOUR_OF_A_KIND, "Four of a Kind (Aces)"),
        ("K♠ K♥ K♦ 10♠ 10♥ 2♣ 3♦", PokerHandRank.FULL_HOUSE, "Full House (Kings full of Tens)"),
        ("A♦ J♦ 9♦ 6♦ 2♦ K♠ Q♥", PokerHandRank.FLUSH, "Flush (Ace high)"),
        ("10♠ 9♥ 8♦ 7♣ 6♠ 2♥ 2♦", PokerHandRank.STRAIGHT, "Straight (Ten high)"),
        ("A♠ 2♥ 3♦ 4♣ 5♠ K♥ Q♦", PokerHandRank.STRAIGHT, "Straight (Five high)"),
        ("Q♠ Q♥ Q♦ 9♣ 4♠ 2♥ 3♦", PokerHandRank.THREE_OF_A_KIND, "Three of a Kind (Queens)"),
        ("A♠ A♥ K♦ K♣ Q♠ 2♥ 3♦", PokerHandRank.TWO_PAIR, "Two Pair (Aces and Kings)"),
        ("J♠ J♥ 9♦ 7♣ 4♠ 2♥ 3♦", PokerHandRank.ONE_PAIR, "One Pair (Jacks)"),
        ("A♠ K♥ 10♦ 7♣ 5♠ 3♥ 2♦", PokerHandRank.HIGH_CARD, "High Card (Ace)"),
    ],
)
def test_evaluate_best_hand_categories(cards_spec, expected_rank, expected_desc_substr):
    hand = evaluate_best_hand(_parse_cards(cards_spec))
    assert hand.rank_category == expected_rank
    assert expected_desc_substr in hand.description
    assert len(hand.best_five) == 5


@pytest.mark.parametrize(
    "stronger_spec,weaker_spec",
    [
        ("A♠ K♠ Q♠ J♠ 10♠", "K♠ Q♠ J♠ 10♠ 9♠"),
        ("8♠ 8♥ 8♦ 8♣ 2♠", "A♠ A♥ A♦ K♠ K♥"),
        ("Q♠ Q♥ Q♦ 4♠ 4♥", "J♠ J♥ J♦ A♠ A♥"),
        ("A♠ K♠ 9♠ 5♠ 2♠", "A♥ Q♥ J♥ 10♥ 8♥"),
        ("10♠ 9♥ 8♦ 7♣ 6♠", "A♠ 2♥ 3♦ 4♣ 5♠"),
        ("A♠ A♥ K♦ Q♣ 9♠", "A♦ A♣ K♠ J♥ 10♦"),
    ],
)
def test_hand_comparisons_and_tiebreakers(stronger_spec, weaker_spec):
    stronger = evaluate_best_hand(_parse_cards(stronger_spec))
    weaker = evaluate_best_hand(_parse_cards(weaker_spec))
    assert stronger > weaker
    assert weaker < stronger
    assert stronger != weaker


def test_hand_equality_split_pot():
    h1 = evaluate_best_hand(_parse_cards("A♠ K♠ 10♥ 9♦ 8♣ 7♠ 2♥"))
    h2 = evaluate_best_hand(_parse_cards("A♥ K♦ 10♥ 9♦ 8♣ 7♠ 3♣"))
    assert h1 == h2


@pytest.mark.parametrize(
    "hole_spec,expected_rank,expected_desc",
    [
        ("A♠ A♥", PokerHandRank.ONE_PAIR, "Pocket Aces"),
        ("A♠ K♠", PokerHandRank.HIGH_CARD, "Ace-King (Suited)"),
        ("Q♥ 10♦", PokerHandRank.HIGH_CARD, "Queen-Ten (Offsuit)"),
    ],
)
def test_evaluate_partial_preflop_hand(hole_spec, expected_rank, expected_desc):
    res = evaluate_best_hand(_parse_cards(hole_spec))
    assert res.rank_category == expected_rank
    assert res.description == expected_desc
