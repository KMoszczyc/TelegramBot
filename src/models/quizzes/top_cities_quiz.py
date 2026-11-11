"""Top Cities Quiz: guess top N cities by population for World, Continent, Country, or Letter."""

from __future__ import annotations

import random
from dataclasses import dataclass

import pandas as pd

from src.config.constants import (
    TOP_CITIES_BASE_REWARDS,
    TOP_CITIES_COMPLETION_EXPONENT,
    TOP_CITIES_MAX_STRIKES,
    TOP_CITIES_MAX_TIPS,
    TOP_CITIES_MIN_BUFFER,
    TOP_CITIES_STRIKE_MULTIPLIER,
    TOP_CITIES_TIP_CALLBACK_PREFIX,
    TOP_CITIES_TIP_MULTIPLIER,
)
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
    build_tip_callback_data as base_build_tip_callback_data,
    calculate_top_reward,
    parse_guesses,
    parse_tip_callback_data as base_parse_tip_callback_data,
    render_strikes,
    sanitize_guess,
)
from src.models.quizzes.quiz_matcher import normalize_text, phrase_similarity

# Re-exports for backward compatibility
__all__ = [
    "EMPTY_STRIKE_ICON",
    "LIST_SIZES",
    "MAX_LAST_GUESS_LENGTH",
    "POPULATION_THRESHOLDS",
    "QUIZ_TYPE",
    "STRIKE_ICON",
    "UNKNOWN_SLOT",
    "BaseTopListGame",
    "Entry",
    "GameStatus",
    "GuessResult",
    "Pool",
    "PoolKind",
    "TopCitiesGame",
    "build_tip_callback_data",
    "build_valid_answers",
    "calculate_reward",
    "find_best_top_city_match",
    "find_exact_city_match",
    "find_fuzzy_city_match",
    "get_eligible_pools",
    "get_list_size",
    "get_pool_df",
    "iso2_to_flag",
    "parse_guesses",
    "parse_tip_callback_data",
    "pick_pool",
    "render_strikes",
    "sanitize_guess",
]

QUIZ_TYPE = "topcities"
LIST_SIZES = (10, 5, 3)

POPULATION_THRESHOLDS = {
    10: 250_000,
    5: 200_000,
    3: 100_000,
}

CONTINENT_NAME_MAP = {
    "europe": "Europa",
    "europa": "Europa",
    "asia": "Azja",
    "azja": "Azja",
    "africa": "Afryka",
    "afryka": "Afryka",
    "north america": "Ameryka Północna",
    "ameryka polnocna": "Ameryka Północna",
    "ameryka północna": "Ameryka Północna",
    "south america": "Ameryka Południowa",
    "ameryka poludniowa": "Ameryka Południowa",
    "ameryka południowa": "Ameryka Południowa",
    "oceania": "Oceania",
    "australia": "Oceania",
}


def iso2_to_flag(iso2: str | None) -> str:
    """Convert a 2-letter ISO country code to its Unicode flag emoji."""
    if not iso2 or len(str(iso2).strip()) != 2:
        return ""
    code = str(iso2).strip().upper()
    return "".join(chr(127397 + ord(c)) for c in code if "A" <= c <= "Z")


@dataclass
class Entry:
    rank: int
    city: str
    city_ascii: str
    country: str
    iso2: str
    admin_name: str
    population: int
    valid_answers: set[str]

    @property
    def flag(self) -> str:
        return iso2_to_flag(self.iso2)


def get_list_size(pool_df: pd.DataFrame) -> int | None:
    """Return the largest valid top-N list size (10, 5, 3) that satisfies population floors and buffer."""
    for size in LIST_SIZES:
        if len(pool_df) >= size + TOP_CITIES_MIN_BUFFER:
            city_pop = int(pool_df.iloc[size - 1]["population"])
            if city_pop >= POPULATION_THRESHOLDS[size]:
                return size
    return None


def build_valid_answers(city_row: dict | pd.Series) -> set[str]:
    """Extract valid answer variations from a city row."""
    answers: set[str] = set()
    for col in ("city", "city_ascii"):
        val = str(city_row.get(col, "")).strip()
        if val and val.lower() != "nan":
            answers.add(val.lower())
    return answers


def get_eligible_pools(df: pd.DataFrame) -> list[Pool]:
    """List every eligible World, Continent, Country, and Letter pool."""
    pools: list[Pool] = []

    # World pool
    world_size = get_list_size(df.sort_values("population", ascending=False))
    if world_size is not None:
        pools.append(Pool(PoolKind.WORLD, "World", world_size))

    # Continent pools
    for continent in df["continent"].dropna().unique():
        cont_str = str(continent).strip()
        cont_df = df[df["continent"] == continent].sort_values("population", ascending=False)
        size = get_list_size(cont_df)
        if size is not None:
            pools.append(Pool(PoolKind.CONTINENT, cont_str, size))

    # Country pools
    for country in df["country"].dropna().unique():
        country_str = str(country).strip()
        c_df = df[df["country"] == country].sort_values("population", ascending=False)
        size = get_list_size(c_df)
        if size is not None:
            pools.append(Pool(PoolKind.COUNTRY, country_str, size))

    # Letter pools (Worldwide)
    first_letters = df["city"].astype(str).str.strip().str[0].str.upper()
    for letter in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
        let_df = df[first_letters == letter].sort_values("population", ascending=False)
        size = get_list_size(let_df)
        if size is not None:
            pools.append(Pool(PoolKind.LETTER, letter, size))

    return pools


def pick_pool(df: pd.DataFrame, rng: random.Random | None = None) -> Pool:
    """Pick one eligible pool balancing fairly between World, Continent, Country, and Letter."""
    r = rng or random
    all_pools = get_eligible_pools(df)
    by_kind: dict[PoolKind, list[Pool]] = {}
    for p in all_pools:
        by_kind.setdefault(p.kind, []).append(p)

    available_kinds = [k for k, pool_list in by_kind.items() if pool_list]
    if not available_kinds:
        return Pool(PoolKind.WORLD, "World", 10)

    chosen_kind = r.choice(available_kinds)
    return r.choice(by_kind[chosen_kind])


def get_pool_df(df: pd.DataFrame, pool: Pool) -> pd.DataFrame:
    """Filter and sort the dataframe for the specified pool."""
    sub_df = df
    match pool.kind:
        case PoolKind.WORLD:
            sub_df = df
        case PoolKind.CONTINENT:
            sub_df = df[df["continent"].str.lower() == pool.target.lower()]
        case PoolKind.COUNTRY:
            sub_df = df[df["country"].str.lower() == pool.target.lower()]
        case PoolKind.LETTER:
            first_letters = df["city"].astype(str).str.strip().str[0].str.upper()
            sub_df = df[first_letters == pool.target.upper()]
        case PoolKind.CUSTOM:
            extra = pool.extra_dict
            if "continent" in extra:
                sub_df = sub_df[sub_df["continent"].str.lower() == extra["continent"].lower()]
            if "country" in extra:
                sub_df = sub_df[sub_df["country"].str.lower() == extra["country"].lower()]
            if "letter" in extra:
                first_letters = sub_df["city"].astype(str).str.strip().str[0].str.upper()
                sub_df = sub_df[first_letters == extra["letter"].upper()]

    return sub_df.sort_values("population", ascending=False).reset_index(drop=True)


def calculate_reward(list_size: int, correct: int, tips_given: int, strikes: int) -> int:
    """Calculate credit payout for *correct* cities out of *list_size*, after tip and strike penalties."""
    return calculate_top_reward(
        list_size,
        correct,
        tips_given,
        strikes,
        base_rewards=TOP_CITIES_BASE_REWARDS,
        tip_multiplier=TOP_CITIES_TIP_MULTIPLIER,
        strike_multiplier=TOP_CITIES_STRIKE_MULTIPLIER,
        completion_exponent=TOP_CITIES_COMPLETION_EXPONENT,
    )


def build_tip_callback_data(user_id: int) -> str:
    """Build the callback_data string for the tip button."""
    return base_build_tip_callback_data(TOP_CITIES_TIP_CALLBACK_PREFIX, user_id)


def parse_tip_callback_data(data: str) -> int | None:
    """Extract owner user_id from the tip callback data or None if invalid."""
    return base_parse_tip_callback_data(TOP_CITIES_TIP_CALLBACK_PREFIX, data)


def find_exact_city_match(clean_user: str, entries_valid: list[set[str]], indices: list[int]) -> int | None:
    """Find an exact match (after normalization) among *indices*."""
    for idx in indices:
        norm_valid = {normalize_text(a) for a in entries_valid[idx] if a}
        if clean_user in norm_valid:
            return idx
    return None


def find_fuzzy_city_match(clean_user: str, entries_valid: list[set[str]], indices: list[int], threshold: float = 0.85) -> int | None:
    """Find the best fuzzy matching index among *indices* for *clean_user* if similarity exceeds threshold."""
    if len(clean_user) <= 3:
        return None

    best_index: int | None = None
    best_score: float = 0.0

    for idx in indices:
        norm_valid = {normalize_text(a) for a in entries_valid[idx] if len(normalize_text(a)) > 2}
        cand_score = max((phrase_similarity(clean_user, cand) for cand in norm_valid), default=0.0)
        if cand_score > best_score:
            best_score = cand_score
            best_index = idx

    if best_score >= threshold:
        return best_index
    return None


def find_best_top_city_match(user_answer: str, entries_valid: list[set[str]], indices: list[int]) -> int | None:
    """Find the best matching index among *indices* for *user_answer* (exact first, then fuzzy)."""
    clean_user = normalize_text(user_answer)
    if not clean_user:
        return None
    exact = find_exact_city_match(clean_user, entries_valid, indices)
    if exact is not None:
        return exact
    return find_fuzzy_city_match(clean_user, entries_valid, indices)


class TopCitiesGame(BaseTopListGame):
    """State and rendering logic for an active Top Cities quiz session."""

    def __init__(self, pool: Pool, entries: list[Entry]):
        super().__init__(
            pool=pool,
            entries=entries,
            max_strikes=TOP_CITIES_MAX_STRIKES,
            max_tips=TOP_CITIES_MAX_TIPS,
            tip_callback_prefix=TOP_CITIES_TIP_CALLBACK_PREFIX,
            strike_separator=" ",
        )
        self.tips_revealed: set[int] = set()

    @classmethod
    def create(
        cls,
        df: pd.DataFrame,
        pool: Pool | None = None,
        rng: random.Random | None = None,
    ) -> TopCitiesGame:
        """Create a new game from dataframe with a selected or random pool."""
        chosen_pool = pool or pick_pool(df, rng)
        pool_df = get_pool_df(df, chosen_pool)
        size = chosen_pool.size

        entries = []
        for rank, (_, row) in enumerate(pool_df.head(size).iterrows(), start=1):
            entries.append(
                Entry(
                    rank=rank,
                    city=str(row.get("city", "")),
                    city_ascii=str(row.get("city_ascii", "")),
                    country=str(row.get("country", "")),
                    iso2=str(row.get("iso2", "")),
                    admin_name=str(row.get("admin_name", "")),
                    population=int(row.get("population", 0)),
                    valid_answers=build_valid_answers(row),
                )
            )

        return cls(chosen_pool, entries)

    @property
    def reward(self) -> int:
        return calculate_reward(self.list_size, self.correct_count, self.tips_given, self.strikes)

    def _tip_candidates(self) -> list[int]:
        """Slots that are unguessed and have not received a tip yet."""
        return [i for i in self._missing_indices() if i not in self.tips_revealed]

    @property
    def can_give_tip(self) -> bool:
        return not self.is_over and self.tips_given < self.max_tips and len(self._tip_candidates()) > 0

    def reveal_tip(self, rng: random.Random | None = None) -> int | None:
        """Reveal a contextual tip for one unguessed slot."""
        candidates = self._tip_candidates()
        if not candidates or self.tips_given >= self.max_tips:
            return None
        chosen = (rng or random).choice(candidates)
        self.tips_revealed.add(chosen)
        self.tips_given += 1
        return chosen

    def _render_slot(self, index: int) -> str:
        entry = self.entries[index]
        flag_str = f" {entry.flag}" if entry.flag and self.pool.kind != PoolKind.COUNTRY else ""

        if index in self.guessed:
            return f"{entry.rank}. *{entry.city}* - {entry.population:,}{flag_str}"
        if self.is_over:
            return f"{entry.rank}. {STRIKE_ICON} *{entry.city}* - {entry.population:,}{flag_str}"

        # Unguessed slot with tip
        if index in self.tips_revealed:
            if self.pool.kind == PoolKind.COUNTRY:
                hint = entry.admin_name if entry.admin_name else f"starts with '{entry.city[0]}'"
                return f"{entry.rank}. {UNKNOWN_SLOT} [{hint}]"
            return f"{entry.rank}. {UNKNOWN_SLOT} [{entry.flag} {entry.country}]"

        return f"{entry.rank}. {UNKNOWN_SLOT}"

    def format_title(self) -> str:
        """Generate human-readable quiz title based on pool kind and target."""
        match self.pool.kind:
            case PoolKind.WORLD:
                return f"🌍 Top {self.list_size} cities in the World by population"
            case PoolKind.CONTINENT:
                return f"🌍 Top {self.list_size} cities in {self.pool.target} by population"
            case PoolKind.COUNTRY:
                flag = iso2_to_flag(self.entries[0].iso2) if self.entries else ""
                flag_suffix = f" {flag}" if flag else ""
                return f"🌍 Top {self.list_size} cities in {self.pool.target}{flag_suffix} by population"
            case PoolKind.LETTER:
                return f"🌍 Top {self.list_size} cities starting with '{self.pool.target}' by population (World)"
            case PoolKind.CUSTOM:
                return f"🌍 Top {self.list_size} cities in {self.pool.target} by population"

    def submit_guess(self, text: str) -> GuessResult:
        """Evaluate a single guess against unrevealed and already-revealed cities."""
        clean_user = normalize_text(text)
        if not clean_user:
            return GuessResult.WRONG

        entries_valid = [e.valid_answers for e in self.entries]

        # 1. Exact match on unrevealed slots -> CORRECT
        exact_missing = find_exact_city_match(clean_user, entries_valid, self._missing_indices())
        if exact_missing is not None:
            self.guessed.add(exact_missing)
            self.last_guess = f"*{self.entries[exact_missing].city}* ✅"
            return GuessResult.CORRECT

        # 2. Exact match on already guessed slots -> DUPLICATE
        exact_guessed = find_exact_city_match(clean_user, entries_valid, sorted(self.guessed))
        if exact_guessed is not None:
            return GuessResult.DUPLICATE

        # 3. Fuzzy match on unrevealed slots -> CORRECT
        fuzzy_missing = find_fuzzy_city_match(clean_user, entries_valid, self._missing_indices())
        if fuzzy_missing is not None:
            self.guessed.add(fuzzy_missing)
            self.last_guess = f"*{self.entries[fuzzy_missing].city}* ✅"
            return GuessResult.CORRECT

        # 4. Fuzzy match on already guessed slots -> DUPLICATE
        fuzzy_guessed = find_fuzzy_city_match(clean_user, entries_valid, sorted(self.guessed))
        if fuzzy_guessed is not None:
            return GuessResult.DUPLICATE

        self.strikes += 1
        clean_guess = sanitize_guess(text)
        self.last_guess = f"*{clean_guess}* {STRIKE_ICON}" if clean_guess else STRIKE_ICON
        return GuessResult.WRONG
