"""ETL: cities.csv → cities.parquet with Silesian population corrections."""

import logging

import pandas as pd

from src.config.paths import CITIES_PARQUET_PATH, DATA_DIR

log = logging.getLogger(__name__)

# Real individual GUS/Wikipedia populations for Silesian agglomeration cities that
# incorrectly show the whole agglomeration total (~2.7M) in the source CSV.
SILESIAN_POPULATION_CORRECTIONS: dict[str, int] = {
    "Zabrze": 177_000,
    "Świętochłowice": 50_000,
    "Sosnowiec": 195_000,
    "Radzionków Nowy": 18_000,
    "Pyskowice": 18_000,
    "Ruda Śląska": 131_000,
    "Orzesze": 22_000,
    "Mikołów": 38_000,
    "Łaziska Górne": 23_000,
    "Knurów": 36_000,
    "Imielin": 9_000,
    "Gliwice": 176_000,
    "Chorzów": 99_000,
    "Czeladź": 32_000,
    "Dąbrowa Górnicza": 114_000,
    "Bieruń": 20_000,
    "Bytom": 152_000,
    "Mysłowice": 72_000,
    "Katowice": 279_000,
    "Halemba": 26_000,
    "Giszowiec": 18_000,
    "Bogucice": 16_000,
    "Kochłowice": 12_000,
    "Chropaczów": 12_000,
    "Zawodzie": 10_000,
    "Załęże": 10_000,
    "Koszutka": 10_000,
    "Komorowice": 10_000,
    "Mikuszowice": 10_000,
}

AGGLOMERATION_POP_VALUES = {2_746_000, 2_710_397, 2_700_000, 1_991_000}


CURATED_ICONIC_CITIES: set[tuple[str, str]] = {
    ("Shanghai", "China"),
    ("Hong Kong", "Hong Kong"),
    ("Guangzhou", "China"),
    ("Shenzhen", "China"),
    ("Mumbai", "India"),
    ("Kolkata", "India"),
    ("Bangalore", "India"),
    ("Chennai", "India"),
    ("Dubai", "United Arab Emirates"),
    ("Singapore", "Singapore"),
    ("Istanbul", "Turkey"),
    ("Almaty", "Kazakhstan"),
    ("Cairo", "Egypt"),
    ("Johannesburg", "South Africa"),
    ("Rio de Janeiro", "Brazil"),
    ("São Paulo", "Brazil"),
    ("Buenos Aires", "Argentina"),
    ("Bangkok", "Thailand"),
    ("Seoul", "South Korea"),
}

WESTERN_MAJOR_ISO2: set[str] = {
    "US",
    "CA",
    "AU",
    "NZ",
    "JP",
    "BR",
    "MX",
    "AR",
    "ZA",
    "CL",
    "KR",
}


def build_cities_parquet(csv_path: str | None = None) -> pd.DataFrame:
    """Load cities.csv, apply corrections, enrich with continent, save as parquet."""
    csv_path = csv_path or str(DATA_DIR / "misc" / "cities.csv")
    log.info("Loading %s …", csv_path)
    df = pd.read_csv(csv_path)

    # ── Fix Silesian agglomeration population bug ──────────────────────────────
    suspicious = df["population"].isin(AGGLOMERATION_POP_VALUES) & (df["country"] == "Poland")
    log.info("Found %d suspicious Silesian rows to correct.", suspicious.sum())
    for city_name, real_pop in SILESIAN_POPULATION_CORRECTIONS.items():
        mask = suspicious & (df["city"] == city_name)
        if mask.any():
            df.loc[mask, "population"] = real_pop
            log.debug("  Corrected %s → %d", city_name, real_pop)

    # ── Enrich with continent via countries parquet ────────────────────────────
    from src.config.paths import COUNTRIES_PATH

    countries_df = pd.read_parquet(str(COUNTRIES_PATH))[["country_code", "continent"]]
    df = df.merge(countries_df, left_on="iso2", right_on="country_code", how="left")
    df.drop(columns=["country_code"], inplace=True)

    df["population"] = pd.to_numeric(df["population"], errors="coerce").fillna(0).astype(int)

    # ── Regional difficulty classification ────────────────────────────────────
    def assign_difficulty(row: pd.Series) -> str:
        pop = row["population"]
        cap = row.get("capital")
        iso2 = str(row.get("iso2", "")).upper()
        continent = str(row.get("continent", "")).lower()

        # Poland: major metropolises are Easy, large hubs Medium, down to small towns
        if iso2 == "PL" or row.get("country") == "Poland":
            if (cap in ("primary", "admin") and pop >= 60_000) or pop >= 100_000:
                return "easy"
            if pop >= 40_000:
                return "medium"
            if pop >= 20_000:
                return "hard"
            if pop >= 10_000:
                return "very_hard"
            return "crazy"

        # Other Europe: major capitals & megacities are Easy, famous hubs & capitals Medium
        if continent in ("europa", "europe"):
            if (cap == "primary" and pop >= 300_000) or pop >= 1_500_000:
                return "easy"
            if pop >= 400_000 or (cap == "primary" and pop >= 50_000):
                return "medium"
            if pop >= 150_000:
                return "hard"
            if pop >= 60_000:
                return "very_hard"
            return "crazy"

        # Rest of the World: national capitals & megacities Easy, regional hubs Medium
        if cap == "primary" or pop >= 3_000_000:
            return "easy"
        if cap == "admin" or pop >= 1_500_000:
            return "medium"
        if pop >= 500_000:
            return "hard"
        if pop >= 150_000:
            return "very_hard"
        return "crazy"

    # ── General Mode eligibility ──────────────────────────────────────────────
    def check_general_eligible(row: pd.Series) -> bool:
        iso2 = str(row.get("iso2", "")).upper()
        continent = str(row.get("continent", "")).lower()
        pop = row["population"]
        cap = row.get("capital")
        city = str(row.get("city", ""))

        # All Poland cities are eligible
        if iso2 == "PL" or row.get("country") == "Poland":
            return True

        # Russia: European Russia (west of Urals, lng <= 60°E) with pop >= 100k or major admin;
        # Asian Russia / Siberia (lng > 60°E) only major metropolises (pop >= 500k)
        if iso2 == "RU":
            lng = float(row.get("lng", 0.0))
            if lng <= 60.0:
                return (pop >= 100_000) or (cap in ("primary", "admin") and pop >= 50_000)
            return pop >= 500_000

        # Turkey: Istanbul, Antalya, Ankara (primary capital), provincial capitals/hubs >= 50k, or cities >= 200k.
        # Exclude Istanbul/Ankara/Izmir municipal sub-district duplicates (e.g. Esenyurt, Çankaya).
        if iso2 == "TR":
            admin_name = str(row.get("admin_name", ""))
            if cap == "primary" or city in ("Istanbul", "Antalya"):
                return True
            if admin_name in ("İstanbul", "Ankara") and city not in ("Istanbul", "Ankara"):
                return False
            if cap == "admin" and pop >= 50_000:
                return True
            return pop >= 200_000

        # Other European countries: all cities eligible
        if continent in ("europa", "europe"):
            return True

        # Rest of World: national capitals >= 100k
        if cap == "primary" and pop >= 100_000:
            return True
        # Western / major countries: major metropolises >= 1.5M
        if iso2 in WESTERN_MAJOR_ISO2 and pop >= 1_500_000:
            return True
        # Iconic non-Western cities
        country = str(row.get("country", ""))
        return (city, country) in CURATED_ICONIC_CITIES and pop >= 500_000

    df["difficulty"] = df.apply(assign_difficulty, axis=1)
    df["general_eligible"] = df.apply(check_general_eligible, axis=1)
    df["easiness_score"] = df["population"].astype(float)

    log.info("Difficulty distribution:\n%s", df["difficulty"].value_counts().to_string())
    log.info("General eligible count: %d / %d", df["general_eligible"].sum(), len(df))

    # ── Save ───────────────────────────────────────────────────────────────────
    import os

    os.makedirs(CITIES_PARQUET_PATH.parent, exist_ok=True)
    df.to_parquet(str(CITIES_PARQUET_PATH), index=False)
    log.info("Saved %d cities → %s", len(df), CITIES_PARQUET_PATH)
    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build_cities_parquet()
