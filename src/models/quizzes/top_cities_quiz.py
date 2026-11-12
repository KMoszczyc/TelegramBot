"""Top Cities Quiz: guess top N cities by population for World, Continent, Country, or Letter."""

from __future__ import annotations

import random
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.models.data.cities import Cities

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
    "get_eligible_pools_for_country",
    "get_list_size",
    "get_pool_df",
    "iso2_to_flag",
    "parse_guesses",
    "parse_tip_callback_data",
    "pick_country_pool",
    "pick_pool",
    "pick_random_letter",
    "render_strikes",
    "resolve_top_cities_pool",
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


def get_list_size(pool_df: pd.DataFrame, is_poland: bool = False) -> int | None:
    """Return the largest valid top-N list size (10, 5, 3).

    For Poland, population floors and buffer requirements are waived.
    For other scopes, requires size + buffer and population thresholds.
    """
    if is_poland:
        n = len(pool_df)
        for size in LIST_SIZES:
            if n >= size:
                return size
        return None

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
        # Determine country pool size: force top10 for most countries, but use top5 for very small ones
        total_cities = len(c_df)
        if total_cities >= 10:
            size = 10
        elif total_cities >= 5:
            size = 5
        else:
            size = None
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
        case PoolKind.DISTRICT:
            norm_target = normalize_text(pool.target)
            sub_df = df[df["admin_name"].fillna("").apply(lambda a: normalize_text(str(a)) == norm_target)]
            extra = pool.extra_dict
            if "country" in extra:
                sub_df = sub_df[sub_df["country"].str.lower() == extra["country"].lower()]
        case PoolKind.LETTER:
            first_letters = df["city"].astype(str).str.strip().str[0].str.upper()
            sub_df = df[first_letters == pool.target.upper()]
        case PoolKind.CUSTOM:
            extra = pool.extra_dict
            if "continent" in extra:
                sub_df = sub_df[sub_df["continent"].str.lower() == extra["continent"].lower()]
            if "country" in extra:
                sub_df = sub_df[sub_df["country"].str.lower() == extra["country"].lower()]
            if "district" in extra:
                norm_district = normalize_text(extra["district"])
                sub_df = sub_df[sub_df["admin_name"].fillna("").apply(lambda a: normalize_text(str(a)) == norm_district)]
            if "letter" in extra:
                first_letters = sub_df["city"].astype(str).str.strip().str[0].str.upper()
                sub_df = sub_df[first_letters == extra["letter"].upper()]

    return sub_df.sort_values("population", ascending=False).reset_index(drop=True)


def pick_random_letter(
    df: pd.DataFrame,
    is_poland: bool = False,
    rng: random.Random | None = None,
) -> str | None:
    """Pick a random starting letter that has at least 3 qualifying cities in `df`."""
    r = rng or random
    first_letters = df["city"].astype(str).str.strip().str[0]
    unique_letters = sorted({c.upper() for c in first_letters if c and c.isalpha()})

    eligible_letters: list[str] = []
    for letter in unique_letters:
        sub_df = df[first_letters.str.upper() == letter]
        if is_poland:
            if len(sub_df) >= 3:
                eligible_letters.append(letter)
        else:
            if get_list_size(sub_df, is_poland=False) is not None:
                eligible_letters.append(letter)

    if not eligible_letters:
        return None
    return r.choice(eligible_letters)


def get_eligible_pools_for_country(
    cities: Cities,
    country_iso2: str,
    country_name: str,
    is_poland: bool = False,
) -> dict[PoolKind, list[Pool]]:
    """List all eligible country, district, and letter pools for a specific country."""
    by_kind: dict[PoolKind, list[Pool]] = {
        PoolKind.COUNTRY: [],
        PoolKind.DISTRICT: [],
        PoolKind.CUSTOM: [],
    }

    # 1. Overall Country pool
    country_draft = Pool(PoolKind.COUNTRY, country_name, 0)
    country_size = get_list_size(get_pool_df(cities.df, country_draft), is_poland=is_poland)
    if country_size is not None:
        by_kind[PoolKind.COUNTRY].append(Pool(PoolKind.COUNTRY, country_name, country_size))

    # 2. District pools
    for district in cities.get_districts_for_country(country_iso2):
        dist_draft = Pool(PoolKind.DISTRICT, district, 0, (("country", country_name),))
        dist_size = get_list_size(get_pool_df(cities.df, dist_draft), is_poland=is_poland)
        if dist_size is not None:
            by_kind[PoolKind.DISTRICT].append(Pool(PoolKind.DISTRICT, district, dist_size, dist_draft.extra))

    # 3. Letter pools in this country
    country_df = cities.df[cities.df["country"].str.lower() == country_name.lower()]
    first_letters = country_df["city"].astype(str).str.strip().str[0]
    unique_letters = sorted({c.upper() for c in first_letters if c and c.isalpha()})
    for letter in unique_letters:
        target = f"{country_name} ({letter})"
        let_draft = Pool(
            PoolKind.CUSTOM,
            target,
            0,
            (("country", country_name), ("letter", letter)),
        )
        let_size = get_list_size(get_pool_df(cities.df, let_draft), is_poland=is_poland)
        if let_size is not None:
            by_kind[PoolKind.CUSTOM].append(Pool(PoolKind.CUSTOM, target, let_size, let_draft.extra))

    return by_kind


def pick_country_pool(
    cities: Cities,
    country_iso2: str,
    country_name: str,
    is_poland: bool = False,
    rng: random.Random | None = None,
    scope: str | None = None,
) -> Pool | None:
    """Pick an eligible pool for a country, fairly balancing across Country, District, and Letter kinds."""
    r = rng or random
    pools_by_kind = get_eligible_pools_for_country(cities, country_iso2, country_name, is_poland=is_poland)

    if scope:
        scope_clean = scope.strip().lower()
        if scope_clean in ("country", "countries"):
            candidates = pools_by_kind.get(PoolKind.COUNTRY, [])
            return r.choice(candidates) if candidates else None
        if scope_clean in ("district", "districts"):
            candidates = pools_by_kind.get(PoolKind.DISTRICT, [])
            return r.choice(candidates) if candidates else None
        if scope_clean in ("letter", "letters"):
            candidates = pools_by_kind.get(PoolKind.CUSTOM, [])
            return r.choice(candidates) if candidates else None
        return None

    available_kinds = [k for k, pool_list in pools_by_kind.items() if pool_list]
    if not available_kinds:
        return None

    chosen_kind = r.choice(available_kinds)
    return r.choice(pools_by_kind[chosen_kind])


def resolve_top_cities_pool(
    cities: Cities,
    continent_raw: str | None = None,
    country_raw: str | None = None,
    district_raw: str | None = None,
    letter_raw: str | None = None,
    scope_raw: str | None = None,
    rng: random.Random | None = None,
) -> tuple[Pool | None, str | None]:
    """Resolve CLI arguments into an eligible top cities Pool or an error string."""
    r = rng or random

    resolved_continent = None
    if continent_raw:
        resolved_continent = cities._normalize_continent(continent_raw)
        if not resolved_continent:
            return (
                None,
                "Invalid --continent value. Available: Europe, Asia, Africa, North America, South America, Oceania",
            )

    country_iso2 = None
    resolved_country_name = None
    if country_raw:
        resolved = cities.resolve_country(country_raw)
        if not resolved:
            return None, f"Unknown country: '{country_raw}'."
        country_iso2, resolved_country_name = resolved

    resolved_district = None
    if district_raw is not None:
        if district_raw.strip():
            clean_district = district_raw.strip()
            norm_dist = normalize_text(clean_district)
            matches = cities.df[cities.df["admin_name"].fillna("").apply(lambda a: normalize_text(str(a)) == norm_dist)]
            if country_iso2:
                matches = matches[matches["iso2"].str.upper() == country_iso2.upper()]
            if matches.empty:
                return None, f"Unknown district: '{clean_district}'."
            resolved_district = str(matches.iloc[0]["admin_name"])
            if not country_iso2:
                country_iso2 = str(matches.iloc[0]["iso2"]).upper()
                resolved_country_name = str(matches.iloc[0]["country"])
        else:
            # Bare --district
            if not country_iso2:
                country_iso2 = "PL"
                resolved_country_name = "Poland"
            available_districts = cities.get_districts_for_country(country_iso2)
            if not available_districts:
                return None, f"No districts found for country '{resolved_country_name or country_iso2}'."
            resolved_district = r.choice(available_districts)

    is_poland = bool(
        (country_iso2 and country_iso2.upper() == "PL") or (resolved_country_name and resolved_country_name.lower() in ("poland", "polska"))
    )

    if scope_raw:
        scope_val = scope_raw.strip().lower()
        if scope_val not in (
            "world",
            "continent",
            "continents",
            "country",
            "countries",
            "district",
            "districts",
            "letter",
            "letters",
        ):
            return None, "Invalid --scope value. Available: world, continent, country, district, letter"

    clean_letter = None
    if letter_raw is not None:
        if letter_raw.strip():
            clean_letter = letter_raw.strip().upper()
            if len(clean_letter) != 1 or not clean_letter.isalpha():
                return None, "Invalid --letter value. Must be a single letter."
        else:
            # Bare --letter: pick a random letter from the current candidate scope
            candidate_df = cities.df
            if resolved_district:
                norm_dist = normalize_text(resolved_district)
                candidate_df = candidate_df[candidate_df["admin_name"].fillna("").apply(lambda a: normalize_text(str(a)) == norm_dist)]
                if country_iso2:
                    candidate_df = candidate_df[candidate_df["iso2"].str.upper() == country_iso2.upper()]
            elif resolved_country_name:
                candidate_df = candidate_df[candidate_df["country"].str.lower() == resolved_country_name.lower()]
            elif resolved_continent:
                candidate_df = candidate_df[candidate_df["continent"].str.lower() == resolved_continent.lower()]

            clean_letter = pick_random_letter(candidate_df, is_poland=is_poland, rng=r)
            if clean_letter is None:
                scope_name = resolved_district or resolved_country_name or resolved_continent or "the World"
                return None, f"Not enough qualifying cities found for {scope_name}."

    if resolved_district and clean_letter:
        target = f"{resolved_district} ({clean_letter})"
        draft = Pool(
            PoolKind.CUSTOM,
            target,
            0,
            (("district", resolved_district), ("letter", clean_letter), ("country", resolved_country_name)),
        )
        size = get_list_size(get_pool_df(cities.df, draft), is_poland=is_poland)
        if size is None:
            return None, f"Not enough qualifying cities found for {resolved_district} starting with '{clean_letter}'."
        return Pool(PoolKind.CUSTOM, target, size, draft.extra), None

    if resolved_district:
        extra_tuples = (("country", resolved_country_name),) if resolved_country_name else ()
        draft = Pool(PoolKind.DISTRICT, resolved_district, 0, extra_tuples)
        size = get_list_size(get_pool_df(cities.df, draft), is_poland=is_poland)
        if size is None:
            return None, f"Not enough qualifying cities found for {resolved_district}."
        return Pool(PoolKind.DISTRICT, resolved_district, size, draft.extra), None

    if resolved_country_name and clean_letter:
        target = f"{resolved_country_name} ({clean_letter})"
        draft = Pool(
            PoolKind.CUSTOM,
            target,
            0,
            (("country", resolved_country_name), ("letter", clean_letter)),
        )
        size = get_list_size(get_pool_df(cities.df, draft), is_poland=is_poland)
        if size is None:
            return None, f"Not enough qualifying cities found for {resolved_country_name} starting with '{clean_letter}'."
        return Pool(PoolKind.CUSTOM, target, size, draft.extra), None

    if resolved_continent and clean_letter:
        target = f"{resolved_continent} ({clean_letter})"
        draft = Pool(
            PoolKind.CUSTOM,
            target,
            0,
            (("continent", resolved_continent), ("letter", clean_letter)),
        )
        size = get_list_size(get_pool_df(cities.df, draft), is_poland=False)
        if size is None:
            return None, f"Not enough qualifying cities found for {resolved_continent} starting with '{clean_letter}'."
        return Pool(PoolKind.CUSTOM, target, size, draft.extra), None

    if resolved_continent:
        draft = Pool(PoolKind.CONTINENT, resolved_continent, 0)
        size = get_list_size(get_pool_df(cities.df, draft), is_poland=False)
        if size is None:
            return None, f"Not enough qualifying cities found for {resolved_continent}."
        return Pool(PoolKind.CONTINENT, resolved_continent, size), None

    if resolved_country_name:
        pool = pick_country_pool(
            cities=cities,
            country_iso2=country_iso2 or "",
            country_name=resolved_country_name,
            is_poland=is_poland,
            rng=r,
            scope=scope_raw,
        )
        if not pool:
            if scope_raw:
                return None, f"No qualifying cities found for scope '{scope_raw}' in {resolved_country_name}."
            return None, f"Not enough qualifying cities found for {resolved_country_name}."
        return pool, None

    if clean_letter:
        draft = Pool(PoolKind.LETTER, clean_letter, 0)
        size = get_list_size(get_pool_df(cities.df, draft), is_poland=False)
        if size is None:
            return None, f"Not enough qualifying cities found starting with '{clean_letter}'."
        return Pool(PoolKind.LETTER, clean_letter, size), None

    if scope_raw:
        scope_val = scope_raw.strip().lower()
        eligible = get_eligible_pools(cities.df)
        pool = None
        if scope_val == "world":
            pool = next((p for p in eligible if p.kind == PoolKind.WORLD), None)
        elif scope_val in ("continent", "continents"):
            cont_pools = [p for p in eligible if p.kind == PoolKind.CONTINENT]
            pool = r.choice(cont_pools) if cont_pools else None
        elif scope_val in ("country", "countries"):
            c_pools = [p for p in eligible if p.kind == PoolKind.COUNTRY]
            pool = r.choice(c_pools) if c_pools else None
        elif scope_val in ("district", "districts"):
            available_districts = cities.get_districts_for_country("PL")
            if available_districts:
                picked_district = r.choice(available_districts)
                draft = Pool(PoolKind.DISTRICT, picked_district, 0, (("country", "Poland"),))
                size = get_list_size(get_pool_df(cities.df, draft), is_poland=True)
                if size is not None:
                    pool = Pool(PoolKind.DISTRICT, picked_district, size, draft.extra)
        elif scope_val in ("letter", "letters"):
            let_pools = [p for p in eligible if p.kind == PoolKind.LETTER]
            pool = r.choice(let_pools) if let_pools else None
        if not pool:
            return None, f"No qualifying cities found for scope '{scope_raw}'."
        return pool, None

    pool = pick_pool(cities.df, rng=r)
    return pool, None


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
        flag_str = f" {entry.flag}" if entry.flag and self.pool.kind not in (PoolKind.COUNTRY, PoolKind.DISTRICT) else ""

        if index in self.guessed:
            return f"{entry.rank}. *{entry.city}* - {entry.population:,}{flag_str}"
        if self.is_over:
            return f"{entry.rank}. {STRIKE_ICON} *{entry.city}* - {entry.population:,}{flag_str}"

        # Unguessed slot with tip
        if index in self.tips_revealed:
            is_district = self.pool.kind == PoolKind.DISTRICT or "district" in self.pool.extra_dict
            if is_district:
                if "letter" in self.pool.extra_dict:
                    prefix = entry.city[:2]
                    return f"{entry.rank}. {UNKNOWN_SLOT} [letter: *{prefix}*]"
                return f"{entry.rank}. {UNKNOWN_SLOT} [letter: *{entry.city[0]}*]"
            if self.pool.kind == PoolKind.COUNTRY:
                if entry.admin_name and entry.admin_name.lower() not in ("nan", ""):
                    return f"{entry.rank}. {UNKNOWN_SLOT} [district: *{entry.admin_name}*]"
                return f"{entry.rank}. {UNKNOWN_SLOT} [letter: *{entry.city[0]}*]"
            if self.pool.kind == PoolKind.CUSTOM and "letter" in self.pool.extra_dict and "district" not in self.pool.extra_dict:
                # Country+letter pool: first letter is known, reveal second letter
                second = entry.city[1] if len(entry.city) > 1 else entry.city[0]
                return f"{entry.rank}. {UNKNOWN_SLOT} [letter: *{entry.city[0]}{second}*]"
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
            case PoolKind.DISTRICT:
                flag = iso2_to_flag(self.entries[0].iso2) if self.entries else ""
                flag_suffix = f" {flag}" if flag else ""
                return f"🌍 Top {self.list_size} cities in {self.pool.target}{flag_suffix} by population"
            case PoolKind.LETTER:
                return f"🌍 Top {self.list_size} cities starting with '{self.pool.target}' by population (World)"
            case PoolKind.CUSTOM:
                flag = iso2_to_flag(self.entries[0].iso2) if self.entries else ""
                flag_suffix = f" {flag}" if flag else ""
                extra = self.pool.extra_dict
                country = extra.get("country", self.pool.target)
                if "letter" in extra and "district" not in extra:
                    letter = extra["letter"].upper()
                    return f"🌍 Top {self.list_size} cities in {country} (letter: *{letter}*){flag_suffix} by population"
                if "district" in extra and "letter" in extra:
                    district = extra["district"]
                    letter = extra["letter"].upper()
                    return f"🌍 Top {self.list_size} cities in {district} (letter: *{letter}*){flag_suffix} by population"
                return f"🌍 Top {self.list_size} cities in {self.pool.target}{flag_suffix} by population"

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
