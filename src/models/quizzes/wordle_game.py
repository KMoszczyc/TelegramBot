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
WORDLE_TIP_PLACEHOLDER = "[_____]"


def _format_bible_attribution(row: Any) -> str:
    """Formats Bible verse attribution as 'Biblia (Abbr Chapter, Verse)'."""
    if hasattr(row, "__getitem__"):
        try:
            abbr = row["abbreviation"]
            ch = row["chapter"]
            v = row["verse"]
            return f"Biblia ({abbr} {ch}, {v})"
        except (KeyError, TypeError):
            pass
    return "Biblia"


def _format_quran_attribution(row: Any) -> str:
    """Formats Quran verse attribution as 'Koran (Chapter:Verse)'."""
    if hasattr(row, "__getitem__"):
        try:
            ch_nr = row["chapter_nr"]
            v = row["verse"]
            return f"Koran ({ch_nr}:{v})"
        except (KeyError, TypeError):
            pass
    return "Koran"


def generate_tip(source_quote: str, target_word: str, tier: int, max_quote_chars: int = 350) -> str:
    """Generate a masked tip string for a Wordle game based on the requested tier.

    Tier 1: Context window (~2 words before and after target word) with the target word replaced by '[_____]'.
    Tier 2: The full quote (or centered window if quote is long) with the target word replaced by '[_____]'.
    """
    if not source_quote or not target_word:
        return ""

    escaped_target = re.escape(target_word)
    pattern = re.compile(rf"\b{escaped_target}\b", re.IGNORECASE)

    if tier >= 2:
        quote = source_quote
        if len(quote) > max_quote_chars:
            m = pattern.search(quote)
            if m:
                t_start, t_end = m.span()
                target_len = t_end - t_start
                half = max(0, (max_quote_chars - target_len) // 2)
                start_char = max(0, t_start - half)
                end_char = min(len(quote), t_end + half)
                if start_char > 0:
                    prev_space = quote.find(" ", start_char)
                    if prev_space != -1 and prev_space < t_start:
                        start_char = prev_space + 1
                if end_char < len(quote):
                    next_space = quote.rfind(" ", t_end, end_char)
                    if next_space != -1 and next_space > t_start:
                        end_char = next_space
                snippet = quote[start_char:end_char]
                if start_char > 0:
                    snippet = f"... {snippet}"
                if end_char < len(quote):
                    snippet = f"{snippet} ..."
                quote = snippet
        return pattern.sub(WORDLE_TIP_PLACEHOLDER, quote)

    tokens = source_quote.strip().split()
    if not tokens:
        return ""

    target_idx = -1
    for idx, tok in enumerate(tokens):
        clean_tok = re.sub(r"^[^\w]+|[^\w]+$", "", tok)
        if clean_tok.lower() == target_word.lower():
            target_idx = idx
            break

    if target_idx == -1:
        for idx, tok in enumerate(tokens):
            if target_word.lower() in tok.lower():
                target_idx = idx
                break

    if target_idx == -1:
        return pattern.sub(WORDLE_TIP_PLACEHOLDER, source_quote)

    start_idx = max(0, target_idx - 2)
    end_idx = min(len(tokens), target_idx + 3)
    masked_window = []
    for idx in range(start_idx, end_idx):
        tok = tokens[idx]
        if idx == target_idx:
            m = re.match(r"^(.*?)([.,;:!?\'\")]*)$", tok)
            trailing_punct = m.group(2) if m else ""
            masked_window.append(f"{WORDLE_TIP_PLACEHOLDER}{trailing_punct}")
        else:
            masked_window.append(tok)

    snippet = " ".join(masked_window)
    if start_idx > 0:
        snippet = f"... {snippet}"
    if end_idx < len(tokens):
        snippet = f"{snippet} ..."

    return snippet


class WordleDatasetLoader:
    """Loads and caches word pools for Wordle games partitioned by dataset and length."""

    # Maps dataset_key -> length -> list of (word, source_line, attribution) tuples
    _cached_pools: dict[str, dict[int, list[tuple[str, str, str]]]] | None = None

    @classmethod
    def load_words(cls, assets: Any | None = None) -> dict[str, dict[int, list[tuple[str, str, str]]]]:
        """Return word pools as {dataset: {length: [(word, source_line, attribution), ...]}}."""
        cached = getattr(assets, "_wordle_cached_words", None)
        if isinstance(cached, dict):
            return cached

        if assets is None and cls._cached_pools is not None:
            return cls._cached_pools

        stopwords = cls._load_stopwords(assets)
        pools: dict[str, dict[int, list[tuple[str, str, str]]]] = {
            ds: {length: [] for length in range(DEFAULT_MIN_LENGTH, DEFAULT_MAX_LENGTH + 1)} for ds in SUPPORTED_DATASETS
        }

        raw_texts = cls._load_raw_texts(assets)

        for ds_key, text_items in raw_texts.items():
            # word -> list of (source_line, attribution) pairs containing it
            word_entries: dict[str, list[tuple[str, str]]] = {}
            for item in text_items:
                if isinstance(item, tuple):
                    text, attr = item
                else:
                    text = item
                    attr = "Jerzy Krajewski" if ds_key == "europejskafirma" else DATASET_NAMES.get(ds_key, ds_key.capitalize())

                if not text or not isinstance(text, str):
                    continue
                tokens = re.findall(r"[a-zA-ZąćęłńóśźżĄĆĘŁŃÓŚŹŻ]+", text)
                for token in tokens:
                    w = token.lower()
                    if DEFAULT_MIN_LENGTH <= len(w) <= DEFAULT_MAX_LENGTH and POLISH_WORD_REGEX.match(w) and w not in stopwords:
                        word_entries.setdefault(w, []).append((text, attr))

            for w, entries in word_entries.items():
                chosen_text, chosen_attr = entries[0] if len(entries) == 1 else entries[random.randint(0, len(entries) - 1)]
                pools[ds_key][len(w)].append((w, chosen_text, chosen_attr))

            for length in pools[ds_key]:
                pools[ds_key][length].sort(key=lambda triplet: triplet[0])

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
    def _load_raw_texts(cls, assets: Any | None) -> dict[str, list[tuple[str, str]]]:
        raw: dict[str, list[tuple[str, str]]] = {ds: [] for ds in SUPPORTED_DATASETS}

        # Bible
        bible_df = None
        if assets is not None and getattr(assets, "bible_df", None) is not None:
            bible_df = assets.bible_df
        elif Path("data/misc/bible.parquet").exists():
            bible_df = pd.read_parquet("data/misc/bible.parquet")

        if bible_df is not None:
            for _, row in bible_df.dropna(subset=["text"]).iterrows():
                raw["bible"].append((str(row["text"]), _format_bible_attribution(row)))

        # Quran
        quran_df = None
        if assets is not None and getattr(assets, "quran_df", None) is not None:
            quran_df = assets.quran_df
        elif Path("data/misc/quran.parquet").exists():
            quran_df = pd.read_parquet("data/misc/quran.parquet")

        if quran_df is not None:
            for _, row in quran_df.dropna(subset=["text"]).iterrows():
                raw["quran"].append((str(row["text"]), _format_quran_attribution(row)))

        # Ozjasz
        ozjasz_lines = []
        if assets is not None and getattr(assets, "ozjasz_phrases", None) is not None:
            ozjasz_lines = list(assets.ozjasz_phrases)
        elif Path("data/misc/ozjasz-wypowiedzi.txt").exists():
            ozjasz_lines = Path("data/misc/ozjasz-wypowiedzi.txt").read_text(encoding="utf-8").splitlines()
        raw["ozjasz"] = [(line, DATASET_NAMES["ozjasz"]) for line in ozjasz_lines if line]

        # Bartosiak
        bartosiak_lines = []
        if assets is not None and getattr(assets, "bartosiak_phrases", None) is not None:
            bartosiak_lines = list(assets.bartosiak_phrases)
        elif Path("data/misc/bartosiak.txt").exists():
            bartosiak_lines = Path("data/misc/bartosiak.txt").read_text(encoding="utf-8").splitlines()
        raw["bartosiak"] = [(line, DATASET_NAMES["bartosiak"]) for line in bartosiak_lines if line]

        # Europejska Firma
        europejskafirma_lines = []
        if assets is not None and getattr(assets, "europejskafirma_phrases", None) is not None:
            europejskafirma_lines = list(assets.europejskafirma_phrases)
        elif Path("data/misc/europejskafirma.txt").exists():
            europejskafirma_lines = Path("data/misc/europejskafirma.txt").read_text(encoding="utf-8").splitlines()
        raw["europejskafirma"] = [(line, "Jerzy Krajewski") for line in europejskafirma_lines if line]

        return raw

    @classmethod
    def get_random_word(
        cls,
        dataset: str | None = None,
        length: int | None = 5,
        assets: Any | None = None,
    ) -> tuple[str, str, str, str]:
        """Select a random (word, source_quote, dataset_key, source_attribution) based on optional constraints."""
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
            chosen_entry = random.choice(fallback_pairs) if fallback_pairs else ("słowo", "", "")
        else:
            chosen_entry = random.choice(pair_list)

        if len(chosen_entry) == 3:
            target_word, source_quote, source_attribution = chosen_entry
        else:
            target_word, source_quote = chosen_entry[:2]
            source_attribution = (
                "Jerzy Krajewski" if chosen_dataset == "europejskafirma" else DATASET_NAMES.get(chosen_dataset, chosen_dataset.capitalize())
            )

        return target_word, source_quote, chosen_dataset, source_attribution


class WordleGame:
    """Maintains the state and evaluation logic of an active Wordle game."""

    def __init__(
        self,
        target_word: str,
        dataset_key: str,
        source_quote: str = "",
        source_attribution: str = "",
        max_tries: int | None = None,
    ) -> None:
        self.target_word: str = target_word.lower().strip()
        self.word_length: int = len(self.target_word)
        self.dataset_key: str = dataset_key
        self.dataset_display_name: str = DATASET_NAMES.get(dataset_key, dataset_key.capitalize())
        self.source_quote: str = source_quote
        if source_attribution:
            self.source_attribution: str = source_attribution
        elif dataset_key == "europejskafirma":
            self.source_attribution = "Jerzy Krajewski"
        elif dataset_key == "bible":
            self.source_attribution = "Biblia"
        elif dataset_key == "quran":
            self.source_attribution = "Koran"
        else:
            self.source_attribution = self.dataset_display_name

        self.max_tries: int = max_tries if max_tries is not None else (self.word_length + 1)
        self.guesses: list[str] = []
        self.evaluations: list[list[LetterState]] = []
        self.status: WordleStatus = WordleStatus.IN_PROGRESS
        self.revealed_tip_tier: int = 0

    @property
    def is_over(self) -> bool:
        return self.status != WordleStatus.IN_PROGRESS

    @property
    def is_won(self) -> bool:
        return self.status == WordleStatus.WON

    @property
    def remaining_tries(self) -> int:
        return max(0, self.max_tries - len(self.guesses))

    @property
    def available_tip_tier(self) -> int:
        """Returns the highest tip tier currently unlocked (0, 1, or 2)."""
        if self.is_over:
            return 0
        if self.remaining_tries == 1:
            return 2
        if len(self.guesses) >= (self.max_tries // 2):
            return 1
        return 0

    @property
    def can_reveal_tip(self) -> bool:
        """Returns True if a newly unlocked tip tier is available to be revealed."""
        return self.available_tip_tier > self.revealed_tip_tier

    def reveal_tip(self) -> str | None:
        """Reveals the highest available tip and updates revealed_tip_tier."""
        if not self.can_reveal_tip or not self.source_quote:
            return None
        self.revealed_tip_tier = self.available_tip_tier
        return self.get_active_tip()

    def get_active_tip(self) -> str | None:
        """Returns the currently revealed tip text, or None if no tip has been revealed."""
        if self.revealed_tip_tier == 0 or not self.source_quote:
            return None
        return generate_tip(self.source_quote, self.target_word, self.revealed_tip_tier)

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
        """Calculate credit reward for a winning game, applying tip penalties if used."""
        if self.status != WordleStatus.WON:
            return 0
        base_reward = 8000
        tries_bonus = self.remaining_tries * 1500
        length_bonus = (self.word_length - 4) * 1000
        total = base_reward + tries_bonus + length_bonus
        if self.revealed_tip_tier == 2:
            total = int(total * 0.50)
        elif self.revealed_tip_tier == 1:
            total = int(total * 0.75)
        return total

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
