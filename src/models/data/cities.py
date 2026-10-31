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
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c)).strip().lower()


class Cities:
    """Manages the cities dataset: loading, filtering, and sampling."""

    def __init__(self, parquet_path: str | None = None, min_population: int = MIN_CITY_POPULATION) -> None:
        path = parquet_path or str(CITIES_PARQUET_PATH)
        df = pd.read_parquet(path)
        if min_population > 0:
            df = df[df["population"] >= min_population].reset_index(drop=True)
        self.df = df
        log.info("Cities loaded: %d rows, difficulties: %s", len(self.df), self.df["difficulty"].value_counts().to_dict())

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
    ) -> tuple[dict, str]:
        """
        Sample a mystery city and determine the chosen difficulty.
        Returns (city_dict, chosen_difficulty).

        If no geographic filters are specified (General Mode), applies regional weighting:
        50% Poland, 35% Other Europe, 15% Rest of World (restricted to general_eligible).
        """
        import random

        has_geo_filter = bool(continent or country_iso2 or district)
        diff_weights = {"easy": 0.25, "medium": 0.35, "hard": 0.25, "very_hard": 0.10, "crazy": 0.05}

        if has_geo_filter:
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
                weights = [diff_weights.get(d, 0.1) for d in available_diffs]
                chosen_diff = random.choices(available_diffs, weights=weights, k=1)[0]

            pool = self.get_cities(
                difficulty=chosen_diff,
                continent=continent,
                country_iso2=country_iso2,
                district=district,
            )
            if pool.empty:
                raise ValueError(f"No cities available for difficulty '{chosen_diff}'.")
            return self.sample_city(pool), chosen_diff

        # ── General Mode (no geographic flags) ─────────────────────────────────
        available_diffs = self.get_available_difficulties(general_eligible=True)
        if difficulty:
            chosen_diff = difficulty.lower()
        else:
            weights = [diff_weights.get(d, 0.1) for d in available_diffs]
            chosen_diff = random.choices(available_diffs, weights=weights, k=1)[0]

        # Regional distribution: 50% PL, 35% Other Europe, 15% Rest of World
        # Non-EU in general mode only features easy and medium tiers (curated capitals/hubs).
        if chosen_diff in ("easy", "medium"):
            regions = ["PL", "EU", "ROW"]
            region_weights = [0.50, 0.35, 0.15]
        else:
            regions = ["PL", "EU"]
            region_weights = [0.55, 0.45]

        chosen_region = random.choices(regions, weights=region_weights, k=1)[0]

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

        return self.sample_city(pool), chosen_diff
