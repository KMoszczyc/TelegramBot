import os

import cartopy.crs as ccrs
import cartopy.feature as cfeature
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from src.config.paths import TEMP_DIR
from src.core import utils as core_utils

_FEATURE_RESOLUTION = "50m"
_LAND_COLOR = "#d3d0cb"
_WATER_COLOR = "white"
_BORDER_COLOR = "#b0b0b0"
_LON_ASPECT_RATIO = 1.4
_MIN_LAT_LON_RATIO = 0.5
_DOT_SIZE = 3
_RING_SIZE = 14
_RING_WIDTH = 1.5
_LABEL_OFFSET = (12, 4)
_LABEL_FONT_SIZE = 14
_SAVE_JPG_DPI = 200

REWARD_LEVELS = {
    "easy": 1000,
    "medium": 3000,
    "hard": 10000,
    "very_hard": 20000,
    "crazy": 50000,
}

DIFFICULTY_INDEX_RANGES = {
    "easy": (0, 30),
    "medium": (30, 100),
    "hard": (100, 300),
    "very_hard": (300, 500),
    "crazy": (500, 100000),
}


class MapQuiz:
    REWARD_LEVELS = REWARD_LEVELS
    DIFFICULTY_INDEX_RANGES = DIFFICULTY_INDEX_RANGES

    def __init__(self, padding_deg: float = 15.0, figsize: tuple[float, float] = (10, 6)):
        self.padding_deg = padding_deg
        self.figsize = figsize

    @staticmethod
    def get_difficulty_from_index(index: int) -> str:
        for diff, (start, end) in MapQuiz.DIFFICULTY_INDEX_RANGES.items():
            if start <= index < end:
                return diff
        return "crazy"

    @staticmethod
    def _extract_year(date_str) -> str:
        if not date_str or str(date_str).lower() == "nan":
            return "?"

        date_str = str(date_str).strip()

        if date_str.startswith("-"):
            year_part = date_str[1:].split("-")[0]
            year_num = year_part.lstrip("0") or "0"
            return f"{year_num} BC"
        else:
            if date_str.startswith("+"):
                date_str = date_str[1:]
            year_part = date_str.split("-")[0]
            year_num = year_part.lstrip("0") or "0"
            return year_num

    def guess_random_person_on_map(self, people_trivia_df):
        person = people_trivia_df.sample(n=1).iloc[0]
        locations = []
        if not pd.isna(person.get("birth_lon")) and not pd.isna(person.get("birth_lat")):
            dob = self._extract_year(person.get("dob", ""))
            locations.append((float(person["birth_lon"]), float(person["birth_lat"]), dob, "green"))

        if not pd.isna(person.get("death_lon")) and not pd.isna(person.get("death_lat")):
            dod = self._extract_year(person.get("dod", ""))
            locations.append((float(person["death_lon"]), float(person["death_lat"]), dod, "red"))

        image_path = self.generate_image(locations)
        return image_path, person

    def generate_image(self, locations: list[tuple[float, float, str, str]]) -> str:
        """Generate a map quiz image with ring markers at given locations.

        Args:
            locations: List of (longitude, latitude, label, color) tuples.

        Returns:
            Path to the generated JPEG image in the temp directory.
        """
        lon_center, lat_center, lon_half, lat_half = self._compute_extent(locations)
        data_crs = ccrs.PlateCarree()
        projection = ccrs.Mercator(central_longitude=lon_center)

        fig = plt.figure(figsize=self.figsize)
        ax = fig.add_subplot(1, 1, 1, projection=projection)
        # res = "10m" if lon_half <= 15 else _FEATURE_RESOLUTION
        self._add_base_layers(ax, _FEATURE_RESOLUTION)

        lon_min = max(lon_center - lon_half * _LON_ASPECT_RATIO, -180)
        lon_max = min(lon_center + lon_half * _LON_ASPECT_RATIO, 180)
        lat_min = max(lat_center - lat_half, -85)
        lat_max = min(lat_center + lat_half, 85)
        ax.set_extent([lon_min, lon_max, lat_min, lat_max], crs=data_crs)
        ax.axis("off")

        # Smart label offset calculation
        offsets = [_LABEL_OFFSET for _ in locations]
        ha = ["left" for _ in locations]
        va = ["bottom" for _ in locations]

        if len(locations) == 2:
            lon1, lat1, _, _ = locations[0]
            lon2, lat2, _, _ = locations[1]
            dist = np.sqrt((lon1 - lon2) ** 2 + (lat1 - lat2) ** 2)
            if dist < max(lon_half, lat_half) * 0.5:  # Close relative to the map scale
                if lon1 >= lon2:
                    offsets[0] = _LABEL_OFFSET  # top right
                    ha[0], va[0] = "left", "bottom"
                    offsets[1] = (-_LABEL_OFFSET[0], -_LABEL_OFFSET[1])  # bottom left
                    ha[1], va[1] = "right", "top"
                else:
                    offsets[0] = (-_LABEL_OFFSET[0], -_LABEL_OFFSET[1])  # bottom left
                    ha[0], va[0] = "right", "top"
                    offsets[1] = _LABEL_OFFSET  # top right
                    ha[1], va[1] = "left", "bottom"

        for i, (lon, lat, label, color) in enumerate(locations):
            self._draw_marker(ax, lon, lat, label, color, data_crs, offsets[i], ha[i], va[i])

        return self._save(fig)

    def _compute_extent(self, locations: list[tuple[float, float, str, str]]) -> tuple[float, float, float, float]:
        """Returns (lon_center, lat_center, lon_half, lat_half) for the bounding box."""
        lons = [loc[0] for loc in locations]
        lats = [loc[1] for loc in locations]
        lon_spread = max(lons) - min(lons)
        lat_spread = max(lats) - min(lats)
        lon_center = float(np.mean(lons))
        lat_center = float(np.mean(lats))

        # Dynamic padding: scale with the spread, but ensure a wider view (less zoom)
        pad = max(10.0, min(self.padding_deg, max(lon_spread, lat_spread) * 0.4))

        lon_half = lon_spread / 2 + pad
        lat_half = lat_spread / 2 + pad
        lat_half = max(lat_half, lon_half * _MIN_LAT_LON_RATIO)

        return lon_center, lat_center, lon_half, lat_half

    @staticmethod
    def _add_base_layers(ax, res: str = _FEATURE_RESOLUTION) -> None:
        ax.set_facecolor(_WATER_COLOR)
        ax.add_feature(cfeature.NaturalEarthFeature("physical", "ocean", res, facecolor=_WATER_COLOR, edgecolor="none"), zorder=0)
        ax.add_feature(cfeature.NaturalEarthFeature("physical", "land", res, facecolor=_LAND_COLOR, edgecolor="none"), zorder=1)
        ax.add_feature(
            cfeature.NaturalEarthFeature("physical", "lakes", res, facecolor=_WATER_COLOR, edgecolor=_BORDER_COLOR, linewidth=0.3), zorder=2
        )
        ax.add_feature(
            cfeature.NaturalEarthFeature("physical", "coastline", res, edgecolor=_BORDER_COLOR, facecolor="none", linewidth=0.5), zorder=3
        )

    @staticmethod
    def _draw_marker(ax, lon: float, lat: float, label: str, color: str, data_crs, offset: tuple[float, float], ha: str, va: str) -> None:
        ax.plot(lon, lat, marker="o", color=color, markersize=_DOT_SIZE, transform=data_crs, zorder=5)
        ax.plot(
            lon,
            lat,
            marker="o",
            markersize=_RING_SIZE,
            markerfacecolor="none",
            markeredgecolor=color,
            markeredgewidth=_RING_WIDTH,
            transform=data_crs,
            zorder=4,
        )
        ax.annotate(
            label,
            xy=(lon, lat),
            xycoords=data_crs._as_mpl_transform(ax),
            xytext=offset,
            textcoords="offset points",
            color=color,
            fontsize=_LABEL_FONT_SIZE,
            fontweight="bold",
            zorder=5,
            ha=ha,
            va=va,
        )

    def _save(self, fig) -> str:
        core_utils.create_dir(TEMP_DIR)
        path = os.path.join(TEMP_DIR, f"{core_utils.get_random_id()}.jpg")
        fig.patch.set_facecolor(_WATER_COLOR)
        plt.savefig(path, format="jpeg", dpi=_SAVE_JPG_DPI, bbox_inches="tight", pad_inches=0)
        plt.close(fig)
        return path


if __name__ == "__main__":
    data = [
        (-7.9898, 31.6225, "1963", "red"),
        (24.9384, 60.1699, "1917", "green"),
    ]
    result = MapQuiz().generate_image(data)
    print(result)
