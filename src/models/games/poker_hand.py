import random
from collections import Counter
from dataclasses import dataclass
from functools import total_ordering
from itertools import combinations

from src.config.enums import PokerHandRank

SUITS: tuple[str, ...] = ("♠", "♥", "♦", "♣")
SUIT_EMOJIS: dict[str, str] = {"♠": "♠️", "♥": "♥️", "♦": "♦️", "♣": "♣️"}

RANK_TO_STR: dict[int, str] = {
    2: "2",
    3: "3",
    4: "4",
    5: "5",
    6: "6",
    7: "7",
    8: "8",
    9: "9",
    10: "10",
    11: "J",
    12: "Q",
    13: "K",
    14: "A",
}

RANK_NAMES_SINGULAR: dict[int, str] = {
    2: "Two",
    3: "Three",
    4: "Four",
    5: "Five",
    6: "Six",
    7: "Seven",
    8: "Eight",
    9: "Nine",
    10: "Ten",
    11: "Jack",
    12: "Queen",
    13: "King",
    14: "Ace",
}

RANK_NAMES_PLURAL: dict[int, str] = {
    2: "Twos",
    3: "Threes",
    4: "Fours",
    5: "Fives",
    6: "Sixes",
    7: "Sevens",
    8: "Eights",
    9: "Nines",
    10: "Tens",
    11: "Jacks",
    12: "Queens",
    13: "Kings",
    14: "Aces",
}


@dataclass(frozen=True)
class Card:
    rank: int
    suit: str

    @property
    def rank_str(self) -> str:
        return RANK_TO_STR[self.rank]

    @property
    def emoji_str(self) -> str:
        return f"{self.rank_str}{SUIT_EMOJIS.get(self.suit, self.suit)}"

    @property
    def short_str(self) -> str:
        return f"{self.rank_str}{self.suit}"

    @property
    def is_red(self) -> bool:
        return self.suit in ("♥", "♦")


def create_shuffled_deck(rng: random.Random | None = None) -> list[Card]:
    """Create and shuffle a standard 52-card deck."""
    deck = [Card(rank=r, suit=s) for s in SUITS for r in range(2, 15)]
    if rng is not None:
        rng.shuffle(deck)
    else:
        random.shuffle(deck)
    return deck


@total_ordering
@dataclass(frozen=True)
class EvaluatedHand:
    rank_category: PokerHandRank
    tiebreakers: tuple[int, ...]
    best_five: tuple[Card, ...]
    description: str

    @property
    def sort_key(self) -> tuple[int, tuple[int, ...]]:
        return (int(self.rank_category), self.tiebreakers)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, EvaluatedHand):
            return NotImplemented
        return self.sort_key == other.sort_key

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, EvaluatedHand):
            return NotImplemented
        return self.sort_key < other.sort_key


def _check_straight(ranks_desc: list[int]) -> int | None:
    """Return the high card of a 5-card straight, or None if not a straight."""
    if len(set(ranks_desc)) != 5:
        return None
    if ranks_desc[0] - ranks_desc[4] == 4:
        return ranks_desc[0]
    if ranks_desc == [14, 5, 4, 3, 2]:
        return 5
    return None


def _evaluate_five_cards(cards: tuple[Card, ...]) -> EvaluatedHand:
    """Evaluate an exact 5-card poker hand."""
    sorted_cards = tuple(sorted(cards, key=lambda c: c.rank, reverse=True))
    ranks_desc = [c.rank for c in sorted_cards]
    is_flush = len({c.suit for c in sorted_cards}) == 1
    straight_high = _check_straight(ranks_desc)

    if is_flush and straight_high is not None:
        if straight_high == 5:
            ordered = tuple([c for c in sorted_cards if c.rank != 14] + [c for c in sorted_cards if c.rank == 14])
        else:
            ordered = sorted_cards
        if straight_high == 14:
            return EvaluatedHand(
                rank_category=PokerHandRank.ROYAL_FLUSH,
                tiebreakers=(14,),
                best_five=ordered,
                description="Royal Flush",
            )
        return EvaluatedHand(
            rank_category=PokerHandRank.STRAIGHT_FLUSH,
            tiebreakers=(straight_high,),
            best_five=ordered,
            description=f"Straight Flush ({RANK_NAMES_SINGULAR[straight_high]} high)",
        )

    counts = Counter(ranks_desc)
    groups = sorted(counts.items(), key=lambda kv: (kv[1], kv[0]), reverse=True)
    freq_pattern = tuple(cnt for _, cnt in groups)
    tiebreakers = tuple(rank for rank, _ in groups)
    ordered_by_group = tuple(sorted(sorted_cards, key=lambda c: (counts[c.rank], c.rank), reverse=True))

    if freq_pattern == (4, 1):
        quad_rank = groups[0][0]
        return EvaluatedHand(
            rank_category=PokerHandRank.FOUR_OF_A_KIND,
            tiebreakers=tiebreakers,
            best_five=ordered_by_group,
            description=f"Four of a Kind ({RANK_NAMES_PLURAL[quad_rank]})",
        )

    if freq_pattern == (3, 2):
        trip_rank, pair_rank = groups[0][0], groups[1][0]
        return EvaluatedHand(
            rank_category=PokerHandRank.FULL_HOUSE,
            tiebreakers=tiebreakers,
            best_five=ordered_by_group,
            description=f"Full House ({RANK_NAMES_PLURAL[trip_rank]} full of {RANK_NAMES_PLURAL[pair_rank]})",
        )

    if is_flush:
        return EvaluatedHand(
            rank_category=PokerHandRank.FLUSH,
            tiebreakers=tuple(ranks_desc),
            best_five=sorted_cards,
            description=f"Flush ({RANK_NAMES_SINGULAR[ranks_desc[0]]} high)",
        )

    if straight_high is not None:
        if straight_high == 5:
            ordered = tuple([c for c in sorted_cards if c.rank != 14] + [c for c in sorted_cards if c.rank == 14])
        else:
            ordered = sorted_cards
        return EvaluatedHand(
            rank_category=PokerHandRank.STRAIGHT,
            tiebreakers=(straight_high,),
            best_five=ordered,
            description=f"Straight ({RANK_NAMES_SINGULAR[straight_high]} high)",
        )

    if freq_pattern == (3, 1, 1):
        trip_rank = groups[0][0]
        return EvaluatedHand(
            rank_category=PokerHandRank.THREE_OF_A_KIND,
            tiebreakers=tiebreakers,
            best_five=ordered_by_group,
            description=f"Three of a Kind ({RANK_NAMES_PLURAL[trip_rank]})",
        )

    if freq_pattern == (2, 2, 1):
        high_pair, low_pair = groups[0][0], groups[1][0]
        return EvaluatedHand(
            rank_category=PokerHandRank.TWO_PAIR,
            tiebreakers=tiebreakers,
            best_five=ordered_by_group,
            description=f"Two Pair ({RANK_NAMES_PLURAL[high_pair]} and {RANK_NAMES_PLURAL[low_pair]})",
        )

    if freq_pattern == (2, 1, 1, 1):
        pair_rank = groups[0][0]
        return EvaluatedHand(
            rank_category=PokerHandRank.ONE_PAIR,
            tiebreakers=tiebreakers,
            best_five=ordered_by_group,
            description=f"One Pair ({RANK_NAMES_PLURAL[pair_rank]})",
        )

    return EvaluatedHand(
        rank_category=PokerHandRank.HIGH_CARD,
        tiebreakers=tuple(ranks_desc),
        best_five=sorted_cards,
        description=f"High Card ({RANK_NAMES_SINGULAR[ranks_desc[0]]})",
    )


def _evaluate_partial_hand(cards: list[Card]) -> EvaluatedHand:
    """Evaluate fewer than 5 cards (e.g. 2 hole cards during Pre-Flop)."""
    if not cards:
        return EvaluatedHand(PokerHandRank.HIGH_CARD, (), (), "No Cards")
    sorted_cards = tuple(sorted(cards, key=lambda c: c.rank, reverse=True))
    counts = Counter(c.rank for c in sorted_cards)
    groups = sorted(counts.items(), key=lambda kv: (kv[1], kv[0]), reverse=True)
    tiebreakers = tuple(rank for rank, _ in groups)

    if groups[0][1] == 2:
        pair_rank = groups[0][0]
        return EvaluatedHand(
            rank_category=PokerHandRank.ONE_PAIR,
            tiebreakers=tiebreakers,
            best_five=sorted_cards,
            description=f"Pocket {RANK_NAMES_PLURAL[pair_rank]}",
        )

    if len(sorted_cards) == 2:
        suited = "Suited" if sorted_cards[0].suit == sorted_cards[1].suit else "Offsuit"
        return EvaluatedHand(
            rank_category=PokerHandRank.HIGH_CARD,
            tiebreakers=tiebreakers,
            best_five=sorted_cards,
            description=f"{RANK_NAMES_SINGULAR[sorted_cards[0].rank]}-{RANK_NAMES_SINGULAR[sorted_cards[1].rank]} ({suited})",
        )

    return EvaluatedHand(
        rank_category=PokerHandRank.HIGH_CARD,
        tiebreakers=tiebreakers,
        best_five=sorted_cards,
        description=f"High Card ({RANK_NAMES_SINGULAR[sorted_cards[0].rank]})",
    )


def evaluate_best_hand(cards: list[Card]) -> EvaluatedHand:
    """Evaluate the best 5-card Texas Hold'em hand from 2 to 7 cards."""
    if len(cards) < 5:
        return _evaluate_partial_hand(cards)
    return max(_evaluate_five_cards(combo) for combo in combinations(cards, 5))
