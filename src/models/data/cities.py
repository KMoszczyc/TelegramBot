"""Cities data model: load, filter, and sample from the cities parquet."""

import logging
import unicodedata

import pandas as pd

from src.config.paths import CITIES_PARQUET_PATH

log = logging.getLogger(__name__)

CONTINENT_ALIASES: dict[str, str] = {
    "europe": "Europa",
    "europa": "Europa",
    "africa": "Afryka",
    "afryka": "Afryka",
    "asia": "Azja",
    "azja": "Azja",
    "north america": "Ameryka Północna",
    "ameryka północna": "Ameryka Północna",
    "south america": "Ameryka Południowa",
    "ameryka południowa": "Ameryka Południowa",
    "oceania": "Oceania",
}

DIFFICULTIES = ["easy", "medium", "hard", "very_hard", "crazy"]
MIN_CITY_POPULATION = 30_000


def _normalize_text(text: str) -> str:
    """Strip diacritics and convert to lowercase for lenient matching."""
    text = text.replace("ł", "l").replace("Ł", "L")
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c)).strip().lower()


class Cities:
    """Manages the cities dataset: loading, filtering, and sampling."""

    def __init__(self, parquet_path: str | None = None, min_population: int = MIN_CITY_POPULATION) -> None:
        path = parquet_path or str(CITIES_PARQUET_PATH)
        df = pd.read_parquet(path)
        if min_population > 0:
            df = df[(df["population"] >= min_population) | (df["iso2"].str.upper() == "PL")].reset_index(drop=True)
        # Asian Russia (east of Urals, lng > 60°E) only retains major hubs >= 500k
        ru_siberia_small = (df["iso2"].str.upper() == "RU") & (df["lng"] > 60.0) & (df["population"] < 500_000)
        if ru_siberia_small.any():
            df = df[~ru_siberia_small].reset_index(drop=True)
        self.df = df
        self._init_countries_lookup()
        log.info("Cities loaded: %d rows, difficulties: %s", len(self.df), self.df["difficulty"].value_counts().to_dict())

    def _init_countries_lookup(self) -> None:
        """Precompute country lookup mappings for fast, flexible resolution."""
        from src.config.paths import COUNTRIES_PATH

        c_map: dict[str, dict] = {}
        if COUNTRIES_PATH.exists():
            try:
                cdf = pd.read_parquet(COUNTRIES_PATH)
                c_map = cdf.set_index("country_code")[["country_name", "population"]].to_dict("index")
            except Exception as e:
                log.warning("Could not read countries parquet for country resolution: %s", e)

        countries_list: list[dict] = []
        if "iso2" in self.df.columns:
            has_iso3 = "iso3" in self.df.columns
            has_country = "country" in self.df.columns
            group_cols = ["iso2"]
            if has_iso3:
                group_cols.append("iso3")
            if has_country:
                group_cols.append("country")

            for keys, sub in self.df.groupby(group_cols, observed=True):
                if not isinstance(keys, tuple):
                    keys = (keys,)
                iso2_str = str(keys[0]).strip()
                iso3_str = str(keys[1]).strip() if has_iso3 else ""
                name_en_str = str(keys[2 if has_iso3 else 1]).strip() if has_country else iso2_str
                c_info = c_map.get(iso2_str, {})
                name_pl = str(c_info.get("country_name", "")).strip()
                pop_val = int(c_info.get("population", 0))
                if pop_val <= 0:
                    pop_val = int(pd.to_numeric(sub.get("population", 0), errors="coerce").fillna(0).sum())
                countries_list.append(
                    {
                        "iso2": iso2_str,
                        "iso3": iso3_str,
                        "name_en": name_en_str,
                        "name_pl": name_pl,
                        "norm_en": _normalize_text(name_en_str),
                        "norm_pl": _normalize_text(name_pl),
                        "pop": pop_val,
                    }
                )
        self._countries_lookup = countries_list

    def resolve_country(self, query: str) -> tuple[str, str] | None:
        """
        Resolve a country query (ISO2, ISO3, English or Polish full name, or prefix/substring)
        to (iso2, english_country_name). If ambiguous, picks the country with highest population.
        """
        q_raw = query.strip()
        if not q_raw:
            return None
        q_norm = _normalize_text(q_raw)
        q_upper = q_raw.upper()

        # 1. Exact ISO2
        for c in self._countries_lookup:
            if c["iso2"].upper() == q_upper:
                return c["iso2"], c["name_en"]

        # 2. Exact ISO3
        for c in self._countries_lookup:
            if c["iso3"].upper() == q_upper:
                return c["iso2"], c["name_en"]

        # 3. Exact full name match (English or Polish, diacritics normalized)
        for c in self._countries_lookup:
            if c["norm_en"] == q_norm or (c["norm_pl"] and c["norm_pl"] == q_norm):
                return c["iso2"], c["name_en"]

        # 4. Prefix match (starts with query)
        prefix_matches = [
            c for c in self._countries_lookup if c["norm_en"].startswith(q_norm) or (c["norm_pl"] and c["norm_pl"].startswith(q_norm))
        ]
        if prefix_matches:
            prefix_matches.sort(key=lambda x: x["pop"], reverse=True)
            return prefix_matches[0]["iso2"], prefix_matches[0]["name_en"]

        # 5. Substring match (contains query)
        substr_matches = [c for c in self._countries_lookup if q_norm in c["norm_en"] or (c["norm_pl"] and q_norm in c["norm_pl"])]
        if substr_matches:
            substr_matches.sort(key=lambda x: x["pop"], reverse=True)
            return substr_matches[0]["iso2"], substr_matches[0]["name_en"]

        return None

    # ── Filtering ──────────────────────────────────────────────────────────────

    def _normalize_continent(self, raw: str) -> str | None:
        """Return the canonical continent name or None if unrecognised."""
        key = raw.strip().lower()
        return CONTINENT_ALIASES.get(key)

    def get_cities(
        self,
        difficulty: str | None = None,
        continent: str | None = None,
        country_iso2: str | None = None,
        district: str | None = None,
        general_eligible: bool | None = None,
    ) -> pd.DataFrame:
        """Return a filtered subset of cities. All filters are optional."""
        df = self.df
        if continent:
            canonical = self._normalize_continent(continent)
            if canonical:
                df = df[df["continent"].str.casefold() == canonical.casefold()]
            else:
                df = df[df["continent"].str.contains(continent, case=False, na=False)]
        if country_iso2:
            df = df[df["iso2"].str.upper() == country_iso2.upper()]
        if district:
            norm_district = _normalize_text(district)
            df = df[df["admin_name"].fillna("").apply(lambda a: _normalize_text(str(a)) == norm_district)]
        if difficulty:
            df = df[df["difficulty"] == difficulty.lower()]
        if general_eligible is not None and "general_eligible" in df.columns:
            df = df[df["general_eligible"] == general_eligible]
        return df

    def get_available_difficulties(
        self,
        continent: str | None = None,
        country_iso2: str | None = None,
        district: str | None = None,
        general_eligible: bool | None = None,
    ) -> list[str]:
        df = self.get_cities(
            continent=continent,
            country_iso2=country_iso2,
            district=district,
            general_eligible=general_eligible,
        )
        return [d for d in DIFFICULTIES if not df[df["difficulty"] == d].empty]

    def get_districts_for_country(self, country_iso2: str) -> list[str]:
        """Return sorted list of unique districts (admin_name) for a country ISO2."""
        country_df = self.df[self.df["iso2"].str.upper() == country_iso2.upper()]
        districts = country_df["admin_name"].dropna().unique().tolist()
        return sorted([str(d) for d in districts if str(d).strip()])

    def find_country_for_district(self, district: str) -> str | None:
        """Find the ISO2 code of a country containing a district, or None."""
        norm_district = _normalize_text(district)
        matches = self.df[self.df["admin_name"].fillna("").apply(lambda a: _normalize_text(str(a)) == norm_district)]
        if not matches.empty:
            return str(matches.iloc[0]["iso2"]).upper()
        return None

    def get_continent_for_iso2(self, iso2: str) -> str | None:
        """Return the continent name for a country ISO2 code, or None."""
        rows = self.df[self.df["iso2"].str.upper() == iso2.upper()]["continent"].dropna()
        return rows.iloc[0] if not rows.empty else None

    def get_hint_pool(
        self,
        mystery_iso2: str,
        continent: str | None = None,
        country_iso2: str | None = None,
        district: str | None = None,
    ) -> pd.DataFrame:
        """Return cities suitable as hint arrows (same scope as the game mode)."""
        if district and country_iso2:
            norm_district = _normalize_text(district)
            district_hints = self.df[
                (self.df["iso2"].str.upper() == country_iso2.upper())
                & self.df["admin_name"].fillna("").apply(lambda a: _normalize_text(str(a)) == norm_district)
            ]
            if len(district_hints) >= 4:
                return district_hints
            # Fall back to country-wide if not enough cities in district
            return self.df[self.df["iso2"].str.upper() == country_iso2.upper()]
        if country_iso2:
            return self.df[self.df["iso2"].str.upper() == country_iso2.upper()]
        if continent:
            canonical = self._normalize_continent(continent) or continent
            return self.df[self.df["continent"].str.casefold() == canonical.casefold()]
        # Default: same continent as mystery city
        mystery_continent = self.get_continent_for_iso2(mystery_iso2)
        if mystery_continent:
            return self.df[self.df["continent"].str.casefold() == mystery_continent.casefold()]
        return self.df

    def sample_city(self, filtered_df: pd.DataFrame | None = None) -> dict:
        """Return a random city as a dict."""
        df = filtered_df if filtered_df is not None else self.df
        if df.empty:
            raise ValueError("No cities match the specified filters.")
        return df.sample(n=1).iloc[0].to_dict()

    def sample_mystery_city(
        self,
        difficulty: str | None = None,
        continent: str | None = None,
        country_iso2: str | None = None,
        district: str | None = None,
        exclude_ids: set[int] | None = None,
    ) -> tuple[dict, str]:
        """
        Sample a mystery city and determine the chosen difficulty.
        Returns (city_dict, chosen_difficulty).

        If no geographic filters are specified (General Mode), applies regional weighting:
        33% Poland, 33% Other Europe, 34% Rest of World (restricted to general_eligible).
        Non-Poland cities in General Mode are restricted to easy and medium tiers.
        """
        import random

        has_geo_filter = bool(continent or country_iso2 or district)
        diff_weights = {"easy": 0.10, "medium": 0.25, "hard": 0.35, "very_hard": 0.20, "crazy": 0.10}

        def pick_from(candidate_pool: pd.DataFrame) -> dict:
            if exclude_ids and "id" in candidate_pool.columns:
                unseen = candidate_pool[~candidate_pool["id"].isin(exclude_ids)]
                if not unseen.empty:
                    return self.sample_city(unseen)
            return self.sample_city(candidate_pool)

        if has_geo_filter:
            is_pl = bool((country_iso2 and country_iso2.upper() == "PL") or (district and self.find_country_for_district(district) == "PL"))
            # When --district is used for Poland without an explicit difficulty:
            # sample uniformly from all cities in that district so single-city tiers (like Rzeszów) don't dominate.
            if district and is_pl and not difficulty:
                district_pool = self.get_cities(
                    continent=continent,
                    country_iso2=country_iso2 or "PL",
                    district=district,
                )
                if district_pool.empty:
                    raise ValueError(f"No cities match district '{district}'.")
                picked = pick_from(district_pool)
                return picked, picked.get("difficulty", "medium")

            available_diffs = self.get_available_difficulties(
                continent=continent,
                country_iso2=country_iso2,
                district=district,
            )
            if not available_diffs:
                raise ValueError("No cities match the specified filters.")

            if difficulty:
                chosen_diff = difficulty.lower()
            else:
                active_weights = {d: 0.20 for d in DIFFICULTIES} if is_pl else diff_weights
                weights = [active_weights.get(d, 0.1) for d in available_diffs]
                chosen_diff = random.choices(available_diffs, weights=weights, k=1)[0]

            pool = self.get_cities(
                difficulty=chosen_diff,
                continent=continent,
                country_iso2=country_iso2,
                district=district,
            )
            if pool.empty:
                raise ValueError(f"No cities available for difficulty '{chosen_diff}'.")
            return pick_from(pool), chosen_diff

        # ── General Mode (no geographic flags) ─────────────────────────────────
        # Regional distribution: 33% Poland, 33% Other Europe, 34% Rest of World
        regions = ["PL", "EU", "ROW"]
        region_weights = [0.33, 0.33, 0.34]
        chosen_region = random.choices(regions, weights=region_weights, k=1)[0]

        available_diffs = self.get_available_difficulties(general_eligible=True)
        if difficulty:
            chosen_diff = difficulty.lower()
        elif chosen_region == "PL":
            # Poland in General Mode uses equal weighting across all difficulty tiers
            pl_diffs = self.get_available_difficulties(country_iso2="PL")
            weights = [0.20 for _ in pl_diffs]
            chosen_diff = random.choices(pl_diffs, weights=weights, k=1)[0]
        else:
            # Foreign cities in General Mode are strictly easy & medium (capitals / famous hubs)
            foreign_diffs = [d for d in available_diffs if d in ("easy", "medium")]
            weights = [diff_weights.get(d, 0.5) for d in foreign_diffs]
            chosen_diff = random.choices(foreign_diffs, weights=weights, k=1)[0] if foreign_diffs else "easy"

        if chosen_region == "PL":
            pool = self.df[(self.df["iso2"].str.upper() == "PL") & (self.df["difficulty"] == chosen_diff)]
        elif chosen_region == "EU":
            pool = self.df[
                self.df["continent"].str.lower().isin(["europa", "europe"])
                & (self.df["iso2"].str.upper() != "PL")
                & (self.df["difficulty"] == chosen_diff)
            ]
        else:  # ROW
            gen_col = self.df["general_eligible"] if "general_eligible" in self.df.columns else True
            pool = self.df[
                (~self.df["continent"].str.lower().isin(["europa", "europe"]))
                & (gen_col == True)  # noqa: E712
                & (self.df["difficulty"] == chosen_diff)
            ]

        if pool.empty:
            pool = self.get_cities(difficulty=chosen_diff, general_eligible=True)
            if pool.empty:
                pool = self.get_cities(difficulty=chosen_diff)

        return pick_from(pool), chosen_diff
