import difflib
import os
import re

import cartopy.crs as ccrs
import cartopy.feature as cfeature
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from src.config.paths import MAP_QUIZ_IMAGES_DIR_PATH, TEMP_DIR
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
    def get_reward(difficulty: str, category_specified: bool, tips_given: int = 0) -> tuple[int, int]:
        base_reward = MapQuiz.REWARD_LEVELS.get(difficulty, MapQuiz.REWARD_LEVELS["crazy"])
        if category_specified:
            base_reward = base_reward // 2

        if tips_given == 0:
            return base_reward, 0

        current_reward = int(base_reward * 0.5 * (0.75 ** (tips_given - 1)))
        decrease_pct = int(round((base_reward - current_reward) / base_reward * 100)) if base_reward > 0 else 0
        return current_reward, decrease_pct

    @staticmethod
    def get_person_display_name(person: dict) -> str:
        is_polish = False
        for field in ["citizenship", "birth_country", "death_country"]:
            val = str(person.get(field, "")).lower()
            if any(keyword in val for keyword in ["poland", "polska", "polish", "warsaw"]):
                is_polish = True
                break

        display_name = person.get("name_pl") if is_polish else person.get("name_en")
        if not display_name or str(display_name).lower() == "nan":
            display_name = person.get("name_pl")

        return str(display_name)

    @staticmethod
    def get_person_description(person: dict, max_length: int = 3500, extended: bool = False) -> str:
        desc = str(person.get("description", "")).strip()
        if desc.lower() in ("", "nan", "none"):
            return ""

        def mask_dots(match):
            return match.group(0).replace(".", "<DOT>")

        if not extended:
            temp_desc = re.sub(r"\([^)]+\)", mask_dots, desc)
            temp_desc = re.sub(r"\[[^]]+\]", mask_dots, temp_desc)
            abbrevs = r"\b(ur|zm|ok|in|im|ps|np|tzw|tzn|ul|prof|dr|ks|m|r|w|św|hr|właśc|zw|łac|[A-Za-zŚĆŻŹŁśąćżźł])"
            temp_desc = re.sub(rf"{abbrevs}\.\s+", r"\1<DOT> ", temp_desc)
            temp_desc = re.sub(r"\.\s+(?=[a-z0-9ąćęłńóśźż])", r"<DOT> ", temp_desc)
            parts = re.split(r"(?<=\.)(\s+)", temp_desc)
            if len(parts) > 19:
                desc = "".join(parts[:19]).replace("<DOT>", ".") + " [...]"

        if len(desc) <= max_length:
            return desc

        truncated = desc[:max_length]
        last_period_idx = truncated.rfind(".")
        if last_period_idx != -1:
            return truncated[: last_period_idx + 1]

        return truncated + "..."

    @staticmethod
    def get_tips(person: dict) -> list[str]:
        desc = str(person.get("description", "")).strip()
        if desc.lower() in ("", "nan", "none"):
            return []

        def mask_dots(match):
            return match.group(0).replace(".", "<DOT>")

        temp_desc = re.sub(r"\([^)]+\)", mask_dots, desc)
        temp_desc = re.sub(r"\[[^]]+\]", mask_dots, temp_desc)

        # Prevent splitting on common abbreviations and initials
        abbrevs = r"\b(ur|zm|ok|in|im|ps|np|tzw|tzn|ul|prof|dr|ks|m|r|w|św|hr|właśc|łac|gr|ros|arab|[A-Za-zŚĆŻŹŁśąćżźł])"
        temp_desc = re.sub(rf"{abbrevs}\.\s+", r"\1<DOT> ", temp_desc)
        temp_desc = re.sub(r"\.\s+(?=[a-z0-9ąćęłńóśźż])", r"<DOT> ", temp_desc)
        tips = re.split(r"(?<=\.)\s+", temp_desc)
        tips = [t.replace("<DOT>", ".") for t in tips]

        if not tips or not tips[0]:
            return []

        # 1. Clean up the first sentence (remove name/dates before the dash)
        match = re.search(r"\)\s*[-–—]\s+", tips[0])
        if match:
            first_tip = tips[0][match.end() :].strip()
            tips[0] = first_tip or tips[0]
        else:
            match = re.search(r"^[^()]*?\s+[-–—]\s+", tips[0])
            if match:
                first_tip = tips[0][match.end() :].strip()
                tips[0] = first_tip or tips[0]

        # 2. Pre-compile censorship patterns
        valid_answers = MapQuiz.get_valid_answers(person)
        answers_to_blur = sorted([ans for ans in valid_answers if len(ans) > 2], key=len, reverse=True)

        patterns = []
        for ans in answers_to_blur:
            suffix = r"\w*" if len(ans) > 4 else ""
            patterns.append(re.compile(rf"\b{re.escape(ans)}{suffix}\b", re.IGNORECASE))

        # 3. Apply censorship
        blurred_tips = []
        for tip in tips:
            for pattern in patterns:
                tip = pattern.sub("🤔🤔🤔", tip)
            blurred_tips.append(tip)

        return blurred_tips[:3]

    @staticmethod
    def get_valid_answers(person: dict) -> set[str]:
        valid_answers = set()
        for field in ["name_pl", "name_en"]:
            name = str(person.get(field, "")).lower()
            if name and name != "nan":
                valid_answers.add(name)
                for part in name.split():
                    valid_answers.add(part)
        return valid_answers

    @staticmethod
    def is_answer_correct(user_answer: str, valid_answers: set[str]) -> bool:
        if user_answer in valid_answers:
            return True

        user_words = user_answer.split()
        if not user_words:
            return False

        valid_parts = {w for w in valid_answers if len(w.split()) == 1}
        matched_words = 0
        for word in user_words:
            if word in valid_answers or (
                len(word) >= 3
                and not re.match(r"^m{0,4}(cm|cd|d?c{0,3})(xc|xl|l?x{0,3})(ix|iv|v?i{0,3})$", word)
                and difflib.get_close_matches(word, valid_parts, n=1, cutoff=0.75)
            ):
                matched_words += 1

        return matched_words == len(user_words)

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

    @staticmethod
    def get_image_filename_for_person(person: dict) -> str:
        name = person.get("name_en")
        if not name or str(name).lower() == "nan":
            name = person.get("name_pl")

        name = str(name).lower()
        name = re.sub(r"[^a-z0-9\s]", "", name)
        name = re.sub(r"\s+", "_", name.strip())
        return f"{name}.jpg"

    @staticmethod
    def get_locations_for_person(person: dict) -> list[tuple[float, float, str, str]]:
        locations = []
        if not pd.isna(person.get("birth_lon")) and not pd.isna(person.get("birth_lat")):
            dob = MapQuiz._extract_year(person.get("dob", ""))
            locations.append((float(person["birth_lon"]), float(person["birth_lat"]), dob, "green"))

        if not pd.isna(person.get("death_lon")) and not pd.isna(person.get("death_lat")):
            dod = MapQuiz._extract_year(person.get("dod", ""))
            locations.append((float(person["death_lon"]), float(person["death_lat"]), dod, "red"))
        return locations

    def guess_random_person_on_map(self, people_trivia_df):
        person = people_trivia_df.sample(n=1).iloc[0]

        filename = MapQuiz.get_image_filename_for_person(person)
        image_path = os.path.join(MAP_QUIZ_IMAGES_DIR_PATH, filename)

        if os.path.exists(image_path):
            return image_path, person

        locations = MapQuiz.get_locations_for_person(person)
        fallback_path = self.generate_image(locations)
        return fallback_path, person

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
        res = "10m" if lon_half <= 15 else _FEATURE_RESOLUTION
        self._add_base_layers(ax, res, show_borders=(lon_half <= 15))

        lon_min = max(lon_center - lon_half * _LON_ASPECT_RATIO, -179.9)
        lon_max = min(lon_center + lon_half * _LON_ASPECT_RATIO, 179.9)
        lat_min = max(lat_center - lat_half, -80.0)
        lat_max = min(lat_center + lat_half, 80.0)
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
        if not locations:
            return 0.0, 0.0, 180.0, 85.0

        lons = [loc[0] for loc in locations]
        lats = [loc[1] for loc in locations]
        lon_spread = max(lons) - min(lons)
        lat_spread = max(lats) - min(lats)
        lon_center = float(np.mean(lons))
        lat_center = float(np.mean(lats))

        # Dynamic padding: scale with the spread, but ensure a wider view (less zoom)
        pad = max(6.0, min(self.padding_deg, max(lon_spread, lat_spread) * 0.4))

        lon_half = lon_spread / 2 + pad
        lat_half = lat_spread / 2 + pad
        lat_half = max(lat_half, lon_half * _MIN_LAT_LON_RATIO)

        return lon_center, lat_center, lon_half, lat_half

    @staticmethod
    def _add_base_layers(ax, res: str = _FEATURE_RESOLUTION, show_borders: bool = False) -> None:
        ax.set_facecolor(_WATER_COLOR)
        ax.add_feature(cfeature.NaturalEarthFeature("physical", "ocean", res, facecolor=_WATER_COLOR, edgecolor="none"), zorder=0)
        ax.add_feature(cfeature.NaturalEarthFeature("physical", "land", res, facecolor=_LAND_COLOR, edgecolor="none"), zorder=1)
        ax.add_feature(
            cfeature.NaturalEarthFeature("physical", "lakes", res, facecolor=_WATER_COLOR, edgecolor=_BORDER_COLOR, linewidth=0.3), zorder=2
        )
        ax.add_feature(
            cfeature.NaturalEarthFeature("physical", "coastline", res, edgecolor=_BORDER_COLOR, facecolor="none", linewidth=0.5), zorder=3
        )
        if show_borders:
            ax.add_feature(
                cfeature.NaturalEarthFeature(
                    "cultural", "admin_0_boundary_lines_land", res, edgecolor=_BORDER_COLOR, facecolor="none", linewidth=0.5
                ),
                zorder=4,
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
