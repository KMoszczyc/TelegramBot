import random
import re
from collections import Counter
from enum import Enum
from pathlib import Path
from typing import Any

import pandas as pd


class LetterState(str, Enum):
    ABSENT = "ABSENT"
    PRESENT = "PRESENT"
    CORRECT = "CORRECT"


class WordleStatus(str, Enum):
    IN_PROGRESS = "IN_PROGRESS"
    WON = "WON"
    LOST = "LOST"
    SURRENDERED = "SURRENDERED"


DATASET_NAMES: dict[str, str] = {
    "bible": "Bible",
    "quran": "Quran",
    "ozjasz": "Ozjasz",
    "bartosiak": "Bartosiak",
    "europejskafirma": "Europejska Firma",
}

SUPPORTED_DATASETS: list[str] = list(DATASET_NAMES.keys())
DEFAULT_MIN_LENGTH: int = 4
DEFAULT_MAX_LENGTH: int = 8
POLISH_WORD_REGEX = re.compile(r"^[a-ząćęłńóśźż]+$")


class WordleDatasetLoader:
    """Loads and caches word pools for Wordle games partitioned by dataset and length."""

    # Maps dataset_key -> length -> list of (word, source_line) pairs
    _cached_pools: dict[str, dict[int, list[tuple[str, str]]]] | None = None

    @classmethod
    def load_words(cls, assets: Any | None = None) -> dict[str, dict[int, list[tuple[str, str]]]]:
        """Return word pools as {dataset: {length: [(word, source_line), ...]}}."""
        cached = getattr(assets, "_wordle_cached_words", None)
        if isinstance(cached, dict):
            return cached

        if assets is None and cls._cached_pools is not None:
            return cls._cached_pools

        stopwords = cls._load_stopwords(assets)
        pools: dict[str, dict[int, list[tuple[str, str]]]] = {
            ds: {length: [] for length in range(DEFAULT_MIN_LENGTH, DEFAULT_MAX_LENGTH + 1)} for ds in SUPPORTED_DATASETS
        }

        raw_texts = cls._load_raw_texts(assets)

        for ds_key, text_items in raw_texts.items():
            # word -> list of source lines containing it (preserving duplicates per line)
            word_lines: dict[str, list[str]] = {}
            for text in text_items:
                if not text or not isinstance(text, str):
                    continue
                tokens = re.findall(r"[a-zA-ZąćęłńóśźżĄĆĘŁŃÓŚŹŻ]+", text)
                for token in tokens:
                    w = token.lower()
                    if DEFAULT_MIN_LENGTH <= len(w) <= DEFAULT_MAX_LENGTH and POLISH_WORD_REGEX.match(w) and w not in stopwords:
                        word_lines.setdefault(w, []).append(text)

            for w, lines in word_lines.items():
                pools[ds_key][len(w)].append((w, lines[0] if len(lines) == 1 else lines[random.randint(0, len(lines) - 1)]))

            for length in pools[ds_key]:
                pools[ds_key][length].sort(key=lambda pair: pair[0])

        if assets is not None:
            assets._wordle_cached_words = pools
        else:
            cls._cached_pools = pools
        return pools

    @classmethod
    def _load_stopwords(cls, assets: Any | None) -> set[str]:
        if assets is not None and getattr(assets, "polish_stopwords", None):
            return {w.strip().lower() for w in assets.polish_stopwords if w.strip()}

        stopwords_path = Path("data/misc/polish.stopwords.txt")
        if stopwords_path.exists():
            lines = stopwords_path.read_text(encoding="utf-8").splitlines()
            return {w.strip().lower() for w in lines if w.strip()}
        return set()

    @classmethod
    def _load_raw_texts(cls, assets: Any | None) -> dict[str, list[str]]:
        raw: dict[str, list[str]] = {ds: [] for ds in SUPPORTED_DATASETS}

        # Bible
        if assets is not None and getattr(assets, "bible_df", None) is not None:
            raw["bible"] = assets.bible_df["text"].dropna().tolist()
        elif Path("data/misc/bible.parquet").exists():
            raw["bible"] = pd.read_parquet("data/misc/bible.parquet")["text"].dropna().tolist()

        # Quran
        if assets is not None and getattr(assets, "quran_df", None) is not None:
            raw["quran"] = assets.quran_df["text"].dropna().tolist()
        elif Path("data/misc/quran.parquet").exists():
            raw["quran"] = pd.read_parquet("data/misc/quran.parquet")["text"].dropna().tolist()

        # Ozjasz
        if assets is not None and getattr(assets, "ozjasz_phrases", None) is not None:
            raw["ozjasz"] = list(assets.ozjasz_phrases)
        elif Path("data/misc/ozjasz-wypowiedzi.txt").exists():
            raw["ozjasz"] = Path("data/misc/ozjasz-wypowiedzi.txt").read_text(encoding="utf-8").splitlines()

        # Bartosiak
        if assets is not None and getattr(assets, "bartosiak_phrases", None) is not None:
            raw["bartosiak"] = list(assets.bartosiak_phrases)
        elif Path("data/misc/bartosiak.txt").exists():
            raw["bartosiak"] = Path("data/misc/bartosiak.txt").read_text(encoding="utf-8").splitlines()

        # Europejska Firma
        if assets is not None and getattr(assets, "europejskafirma_phrases", None) is not None:
            raw["europejskafirma"] = list(assets.europejskafirma_phrases)
        elif Path("data/misc/europejskafirma.txt").exists():
            raw["europejskafirma"] = Path("data/misc/europejskafirma.txt").read_text(encoding="utf-8").splitlines()

        return raw

    @classmethod
    def get_random_word(
        cls,
        dataset: str | None = None,
        length: int | None = 5,
        assets: Any | None = None,
    ) -> tuple[str, str, str]:
        """Select a random (word, source_quote, dataset_key) based on optional constraints."""
        pools = cls.load_words(assets)

        chosen_dataset = dataset.lower().strip() if dataset else None
        if not chosen_dataset or chosen_dataset not in pools:
            chosen_dataset = random.choice(SUPPORTED_DATASETS)

        dataset_pools = pools[chosen_dataset]

        if length is not None and length in dataset_pools and dataset_pools[length]:
            chosen_length = length
        else:
            chosen_length = 5 if 5 in dataset_pools and dataset_pools[5] else None
            if chosen_length is None:
                available_lengths = [w_len for w_len, pairs in dataset_pools.items() if pairs]
                chosen_length = random.choice(available_lengths) if available_lengths else 5

        pair_list = dataset_pools.get(chosen_length, [])
        if not pair_list:
            fallback_pairs = [pair for pairs in dataset_pools.values() for pair in pairs]
            target_word, source_quote = random.choice(fallback_pairs) if fallback_pairs else ("słowo", "")
        else:
            target_word, source_quote = random.choice(pair_list)

        return target_word, source_quote, chosen_dataset


class WordleGame:
    """Maintains the state and evaluation logic of an active Wordle game."""

    def __init__(
        self,
        target_word: str,
        dataset_key: str,
        source_quote: str = "",
        max_tries: int | None = None,
    ) -> None:
        self.target_word: str = target_word.lower().strip()
        self.word_length: int = len(self.target_word)
        self.dataset_key: str = dataset_key
        self.dataset_display_name: str = DATASET_NAMES.get(dataset_key, dataset_key.capitalize())
        self.source_quote: str = source_quote
        self.max_tries: int = max_tries if max_tries is not None else (self.word_length + 1)
        self.guesses: list[str] = []
        self.evaluations: list[list[LetterState]] = []
        self.status: WordleStatus = WordleStatus.IN_PROGRESS

    @property
    def is_over(self) -> bool:
        return self.status != WordleStatus.IN_PROGRESS

    @property
    def is_won(self) -> bool:
        return self.status == WordleStatus.WON

    @property
    def remaining_tries(self) -> int:
        return max(0, self.max_tries - len(self.guesses))

    @staticmethod
    def evaluate_guess(guess: str, target: str) -> list[LetterState]:
        """Two-pass Wordle matching algorithm supporting duplicate letters."""
        guess = guess.lower()
        target = target.lower()
        n = len(target)
        result: list[LetterState] = [LetterState.ABSENT] * n
        target_counts = Counter(target)

        # Pass 1: exact matches (CORRECT)
        for i in range(n):
            if i < len(guess) and guess[i] == target[i]:
                result[i] = LetterState.CORRECT
                target_counts[guess[i]] -= 1

        # Pass 2: present letters in wrong positions (PRESENT)
        for i in range(n):
            if result[i] == LetterState.CORRECT:
                continue
            if i < len(guess):
                char = guess[i]
                if target_counts.get(char, 0) > 0:
                    result[i] = LetterState.PRESENT
                    target_counts[char] -= 1
                else:
                    result[i] = LetterState.ABSENT

        return result

    def submit_guess(self, guess: str) -> tuple[bool, str | None]:
        """Validates and applies a player's guess to the active game."""
        if self.is_over:
            return False, "Game is already over."

        guess_clean = guess.lower().strip()
        if len(guess_clean) != self.word_length:
            return False, f"Word must be exactly {self.word_length} letters long!"

        if not POLISH_WORD_REGEX.match(guess_clean):
            return False, "Word can only contain letters!"

        evaluation = self.evaluate_guess(guess_clean, self.target_word)
        self.guesses.append(guess_clean)
        self.evaluations.append(evaluation)

        if all(state == LetterState.CORRECT for state in evaluation):
            self.status = WordleStatus.WON
        elif len(self.guesses) >= self.max_tries:
            self.status = WordleStatus.LOST

        return True, None

    def surrender(self) -> None:
        """Surrender the game immediately."""
        self.status = WordleStatus.SURRENDERED

    def calculate_reward(self) -> int:
        """Calculate credit reward for a winning game."""
        if self.status != WordleStatus.WON:
            return 0
        base_reward = 8000
        tries_bonus = self.remaining_tries * 1500
        length_bonus = (self.word_length - 4) * 1000
        return base_reward + tries_bonus + length_bonus

    def get_keyboard_states(self) -> dict[str, LetterState]:
        """Computes the highest state (CORRECT > PRESENT > ABSENT) achieved for each guessed letter."""
        states: dict[str, LetterState] = {}
        rank = {LetterState.ABSENT: 1, LetterState.PRESENT: 2, LetterState.CORRECT: 3}
        for guess, evals in zip(self.guesses, self.evaluations, strict=False):
            for ch, st in zip(guess, evals, strict=False):
                upper_ch = ch.upper()
                if upper_ch not in states or rank[st] > rank[states[upper_ch]]:
                    states[upper_ch] = st
        return states
