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

    # ── Compute easiness_score for difficulty ranking ──────────────────────────
    EUROPE_MULTIPLIER = 3.0
    multipliers = df["continent"].apply(lambda c: EUROPE_MULTIPLIER if str(c).lower() in ("europa", "europe") else 1.0)
    df["population"] = pd.to_numeric(df["population"], errors="coerce").fillna(0).astype(int)
    df["easiness_score"] = (df["population"] * multipliers).astype(float)

    # Sort by easiness descending, assign difficulty tiers
    df = df.sort_values("easiness_score", ascending=False).reset_index(drop=True)
    thresholds = [(0, 100, "easy"), (100, 1_000, "medium"), (1_000, 5_000, "hard"), (5_000, 20_000, "very_hard")]
    df["difficulty"] = "crazy"
    for start, end, label in thresholds:
        df.loc[start : end - 1, "difficulty"] = label

    log.info("Difficulty distribution:\n%s", df["difficulty"].value_counts().to_string())

    # ── Save ───────────────────────────────────────────────────────────────────
    import os

    os.makedirs(CITIES_PARQUET_PATH.parent, exist_ok=True)
    df.to_parquet(str(CITIES_PARQUET_PATH), index=False)
    log.info("Saved %d cities → %s", len(df), CITIES_PARQUET_PATH)
    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build_cities_parquet()
