import logging
import random
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
        multipliers = np.where(is_eu, 10.0, 1.0)
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
        self._shuffle_queue: list[int] = []
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

    def _refill_shuffle_queue(self) -> None:
        indices = self.df.index.tolist()
        random.shuffle(indices)
        self._shuffle_queue = indices

    def pop_random_country(self, filtered_df: pd.DataFrame | None = None) -> dict:
        """Return one country as a dict.

        When *filtered_df* is given (difficulty / continent filter active) the
        choice is a plain uniform sample from that sub-pool — the pool is
        already constrained so the queue approach would not help much.

        When called without a filter, uses a Fisher-Yates shuffle queue so that
        every country appears exactly once before any country repeats, which
        eliminates the birthday-paradox clustering that makes pure random.sample
        feel non-random.
        """
        if filtered_df is not None:
            return filtered_df.sample(n=1).iloc[0].to_dict()
        if not self._shuffle_queue:
            self._refill_shuffle_queue()
        idx = self._shuffle_queue.pop()
        return self.df.loc[idx].to_dict()
