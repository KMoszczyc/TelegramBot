import logging
from pathlib import Path

import numpy as np
import pandas as pd

from src.config.paths import COUNTRIES_PATH

log = logging.getLogger(__name__)

CONTINENT_ALIASES = {
    "europe": "Europa",
    "europa": "Europa",
    "eu": "Europa",
    "asia": "Azja",
    "azja": "Azja",
    "as": "Azja",
    "africa": "Afryka",
    "afryka": "Afryka",
    "af": "Afryka",
    "north america": "Ameryka Północna",
    "ameryka północna": "Ameryka Północna",
    "na": "Ameryka Północna",
    "south america": "Ameryka Południowa",
    "ameryka południowa": "Ameryka Południowa",
    "sa": "Ameryka Południowa",
    "oceania": "Oceania",
    "oc": "Oceania",
    "australia": "Oceania",
}


class Countries:
    def __init__(self, data: pd.DataFrame | str | Path | None = None):
        if data is None:
            data = COUNTRIES_PATH

        if isinstance(data, str | Path):
            df = pd.read_parquet(str(data))
        elif isinstance(data, pd.DataFrame):
            df = data.copy()
        else:
            raise ValueError(f"Unsupported data type for Countries initialization: {type(data)}")

        is_eu = df["continent"].astype(str).str.lower().isin(["europa", "europe"])
        multipliers = np.where(is_eu, 5.0, 1.0)
        df["easiness_score"] = (df["population"] * multipliers) / 100_000.0

        df = df.sort_values(by="easiness_score", ascending=False).reset_index(drop=True)

        difficulties = ["easy", "medium", "hard", "crazy"]
        chunks = np.array_split(df.index, len(difficulties))
        df["difficulty"] = "crazy"
        self.difficulty_index_ranges = {}
        for diff, chunk in zip(difficulties, chunks, strict=False):
            if len(chunk) > 0:
                df.loc[chunk, "difficulty"] = diff
                self.difficulty_index_ranges[diff] = (int(chunk[0]), int(chunk[-1]) + 1)
            else:
                self.difficulty_index_ranges[diff] = (0, 0)

        self.df = df
        log.info(
            f"Countries loaded and partitioned: {len(self.df)} countries across difficulties {list(self.difficulty_index_ranges.keys())}."
        )

    def get_countries(self, difficulty: str | None = None, continent: str | None = None) -> pd.DataFrame:
        filtered_df = self.df
        if continent is not None and str(continent).strip() != "":
            norm_cont = str(continent).strip().lower()
            if norm_cont in CONTINENT_ALIASES:
                target_cont = CONTINENT_ALIASES[norm_cont]
                filtered_df = filtered_df[filtered_df["continent"].str.contains(target_cont, case=False, na=False)]
            else:
                filtered_df = filtered_df[filtered_df["continent"].str.contains(continent, case=False, na=False)]

        if difficulty is not None and str(difficulty).strip() != "":
            norm_diff = str(difficulty).strip().lower()
            filtered_df = filtered_df[filtered_df["difficulty"] == norm_diff]

        return filtered_df

    def get_available_difficulties(self, continent: str | None = None) -> list[str]:
        filtered_df = self.get_countries(continent=continent)
        return [diff for diff in ["easy", "medium", "hard", "crazy"] if not filtered_df[filtered_df["difficulty"] == diff].empty]
