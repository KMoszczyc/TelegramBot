"""CityQuiz: compass-rose diagram quiz. Players guess a mystery city from directional arrows."""

import math
import os
import random
from io import BytesIO

import numpy as np
import pandas as pd
from PIL import Image, ImageDraw, ImageFont

from src.config.paths import FLAGS_DIR_PATH
from src.models.quizzes.base_guess_quiz import BaseGuessQuiz

# ── Diagram style constants ────────────────────────────────────────────────────
_OUT_W = 960
_OUT_H = 880
_SCALE = 2
_CANVAS_W = _OUT_W * _SCALE
_CANVAS_H = _OUT_H * _SCALE

_BG = (253, 253, 251)
_CIRCLE_R = 90 * _SCALE
_CIRCLE_FILL = (255, 255, 255)
_CIRCLE_OUTLINE = (40, 40, 40)
_CIRCLE_W = 4 * _SCALE
_ARROW_LEN = 210 * _SCALE
_ARROW_W = 4 * _SCALE
_ARROW_COLOR = (35, 35, 35)
_HEAD_LEN = 20 * _SCALE
_HEAD_WIDTH = 12 * _SCALE

_FLAG_W = 46 * _SCALE
_FLAG_H = 31 * _SCALE
_LABEL_PAD = 12 * _SCALE

_FONT_CITY_SIZE = 24 * _SCALE
_FONT_DIST_SIZE = 18 * _SCALE
_FONT_CENTER_SIZE = 24 * _SCALE
_FONT_Q_SIZE = 36 * _SCALE

_TEXT_COLOR = (20, 20, 20)
_DIST_COLOR = (90, 90, 90)

# ── Hint selection constants ───────────────────────────────────────────────────
_NUM_HINTS = 5
_MIN_ANGLE_GAP_DEG = 28.0  # minimum gap between any two chosen arrows (degrees)
_SECTOR_JITTER_DEG = 14.0  # random jitter applied to the chosen city's actual angle
_MAX_POP_RATIO = 20.0  # prefer hint cities within this ratio of mystery city pop
_SCALE_RANGE_KM = (140.0, 650.0)  # default/legacy fallback scale

DIFFICULTY_HINT_CONFIG: dict[str, dict] = {
    "easy": {
        "scale_range_km": (100.0, 380.0),
        "min_dist_km": 30.0,
        "prominence_tiers": (500_000, 200_000, 80_000),
    },
    "medium": {
        "scale_range_km": (60.0, 220.0),
        "min_dist_km": 20.0,
        "prominence_tiers": (250_000, 100_000, 40_000),
    },
    "hard": {
        "scale_range_km": (30.0, 140.0),
        "min_dist_km": 15.0,
        "prominence_tiers": (150_000, 60_000, 30_000),
    },
    "very_hard": {
        "scale_range_km": (20.0, 90.0),
        "min_dist_km": 15.0,
        "prominence_tiers": (100_000, 40_000, 25_000),
    },
    "crazy": {
        "scale_range_km": (15.0, 70.0),
        "min_dist_km": 12.0,
        "prominence_tiers": (60_000, 30_000, 20_000),
    },
}
_SECTOR_DIST_FACTOR = (0.45, 1.6)  # per-hint distance variation around the game scale
_MIN_HINT_DIST_KM = 50.0  # skip commuter suburbs
_MIN_HINTS_BEFORE_RELAXING = 3  # fewer hints than this -> allow suburbs too
_RADIUS_FACTORS = (1.8, 3.0, 5.0, 10.0, 1000.0)  # progressively widened search radius
_PROMINENCE_TIERS = (700_000, 300_000, 120_000)  # preferred fame floors
_FALLBACK_TIERS = (50_000, 0)  # only when a direction has no notable city
_PROMINENCE_WEIGHT = 0.5  # fame vs. distance-fit trade-off when scoring candidates
_CAPITAL_BOOST = {"primary": 3.0, "admin": 1.4}


def _get_font(size: int) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    for path in [
        "C:/Windows/Fonts/segoeui.ttf",
        "C:/Windows/Fonts/arial.ttf",
        "C:/Windows/Fonts/verdana.ttf",
    ]:
        if os.path.exists(path):
            try:
                return ImageFont.truetype(path, size)
            except Exception:
                pass
    return ImageFont.load_default()


_FONT_BOLD = _get_font(_FONT_CITY_SIZE)
_FONT_DIST = _get_font(_FONT_DIST_SIZE)
_FONT_CENTER = _get_font(_FONT_CENTER_SIZE)
_FONT_Q = _get_font(_FONT_Q_SIZE)


# ── Geometry helpers ───────────────────────────────────────────────────────────


def _haversine(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    """Return great-circle distance in km."""
    R = 6_371.0
    φ1, φ2 = math.radians(lat1), math.radians(lat2)
    dφ = math.radians(lat2 - lat1)
    dλ = math.radians(lon2 - lon1)
    a = math.sin(dφ / 2) ** 2 + math.cos(φ1) * math.cos(φ2) * math.sin(dλ / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def _bearing_deg(mystery_lon: float, mystery_lat: float, hint_lon: float, hint_lat: float) -> float:
    """Compass bearing (0°=N, 90°=E) from mystery city to hint city."""
    dλ = math.radians(hint_lon - mystery_lon)
    φ1 = math.radians(mystery_lat)
    φ2 = math.radians(hint_lat)
    x = math.sin(dλ) * math.cos(φ2)
    y = math.cos(φ1) * math.sin(φ2) - math.sin(φ1) * math.cos(φ2) * math.cos(dλ)
    return (math.degrees(math.atan2(x, y)) + 360) % 360


def _angle_diff(a: float, b: float) -> float:
    """Smallest absolute angular difference between two bearings (0–180)."""
    d = abs(a - b) % 360
    return d if d <= 180 else 360 - d


# ── Hint city selection ────────────────────────────────────────────────────────


def _haversine_np(lon1: float, lat1: float, lons: np.ndarray, lats: np.ndarray) -> np.ndarray:
    """Vectorised great-circle distance (km) from one point to many."""
    p1, p2 = np.radians(lat1), np.radians(lats)
    a = np.sin((p2 - p1) / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(np.radians(lons - lon1) / 2) ** 2
    return 2 * 6_371.0 * np.arcsin(np.sqrt(a))


def _bearing_np(lon1: float, lat1: float, lons: np.ndarray, lats: np.ndarray) -> np.ndarray:
    """Vectorised compass bearing (0°=N, 90°=E) from one point to many."""
    dl, p1, p2 = np.radians(lons - lon1), np.radians(lat1), np.radians(lats)
    x = np.sin(dl) * np.cos(p2)
    y = np.cos(p1) * np.sin(p2) - np.sin(p1) * np.cos(p2) * np.cos(dl)
    return (np.degrees(np.arctan2(x, y)) + 360) % 360


def _angle_diff_np(a: np.ndarray, b: float) -> np.ndarray:
    """Vectorised smallest absolute angular difference (0–180)."""
    d = np.abs(a - b) % 360
    return np.where(d <= 180, d, 360 - d)


def _prominence(pop: pd.Series, capital: pd.Series) -> pd.Series:
    """Recognisability proxy: population boosted for national and regional capitals."""
    boost = capital.map(_CAPITAL_BOOST).fillna(1.0)
    return pop.clip(lower=1_000) * boost


def _hint_attempts(prominence_tiers: tuple[float, ...] = _PROMINENCE_TIERS) -> list[tuple[float, float]]:
    """(min prominence, radius factor) pairs: widen the radius slowly, relax fame only as a last resort."""
    attempts = [(tier, radius) for radius in _RADIUS_FACTORS[:3] for tier in prominence_tiers]
    attempts += [(tier, radius) for radius in _RADIUS_FACTORS[3:] for tier in (*prominence_tiers[-1:], *_FALLBACK_TIERS)]
    return attempts


def _best_in_sector(sector_pool: pd.DataFrame, target_km: float, min_gap_deg: float, taken_bearings: list[float]) -> dict | None:
    """Best-scoring candidate (distance fit minus fame bonus) that keeps the required angular gap."""
    score = np.abs(np.log(sector_pool["_dist_km"] / target_km)) - _PROMINENCE_WEIGHT * np.log(sector_pool["_prom"])
    for idx in score.sort_values().index:
        row = sector_pool.loc[idx]
        if all(_angle_diff(row["_bearing"], tb) >= min_gap_deg for tb in taken_bearings):
            return row.to_dict()
    return None


def _pick_by_sectors(
    pool: pd.DataFrame,
    n: int,
    min_gap_deg: float,
    jitter: float,
    scale_km: float,
    prominence_tiers: tuple[float, ...] = _PROMINENCE_TIERS,
) -> list[dict]:
    """
    Fill up to *n* angular sectors from *pool*. Every sector gets its own random target distance
    (around *scale_km*) so some hints are close and others far; well-known cities are preferred.
    """
    sector_size = 360.0 / n
    bearings = pool["_bearing"].to_numpy()
    attempts = _hint_attempts(prominence_tiers)
    chosen: list[dict] = []
    chosen_bearings: list[float] = []

    for sector_i in range(n):
        centre = (sector_i * sector_size + sector_size / 2 + jitter) % 360
        in_sector = pool[_angle_diff_np(bearings, centre) <= sector_size * 0.7]
        if in_sector.empty:
            continue
        target_km = scale_km * random.uniform(*_SECTOR_DIST_FACTOR)
        for min_prom, radius_factor in attempts:
            sub = in_sector[(in_sector["_prom"] >= min_prom) & (in_sector["_dist_km"] <= scale_km * radius_factor)]
            if sub.empty:
                continue
            pick = _best_in_sector(sub, target_km, min_gap_deg, chosen_bearings)
            if pick is not None:
                chosen.append(pick)
                chosen_bearings.append(float(pick["_bearing"]))
                break
    return chosen


def select_hint_cities(
    mystery: dict,
    pool_df: pd.DataFrame,
    n: int = _NUM_HINTS,
    min_gap_deg: float = _MIN_ANGLE_GAP_DEG,
    difficulty: str | None = None,
) -> list[dict]:
    """
    Select *n* well-known hint cities spread directionally around the mystery city.

    Each game draws a random distance scale scaled to difficulty, so rounds range from
    tight local clusters for smaller cities to broader regional neighbours for world capitals.
    """
    candidates = pool_df
    mystery_id = mystery.get("id")
    if mystery_id is not None:
        candidates = candidates[candidates["id"] != mystery_id]
    if candidates.empty:
        return []

    diff_key = (difficulty or str(mystery.get("difficulty", "medium"))).lower()
    config = DIFFICULTY_HINT_CONFIG.get(diff_key, DIFFICULTY_HINT_CONFIG["medium"])
    scale_range = config["scale_range_km"]
    base_min_dist = config["min_dist_km"]
    prominence_tiers = config["prominence_tiers"]

    m_lon, m_lat = float(mystery["lng"]), float(mystery["lat"])
    lons = candidates["lng"].to_numpy(dtype=float)
    lats = candidates["lat"].to_numpy(dtype=float)
    candidates = candidates.assign(
        _dist_km=_haversine_np(m_lon, m_lat, lons, lats),
        _bearing=_bearing_np(m_lon, m_lat, lons, lats),
        _pop=pd.to_numeric(candidates["population"], errors="coerce").fillna(0).astype(float),
    ).reset_index(drop=True)
    candidates["_prom"] = _prominence(candidates["_pop"], candidates["capital"].astype(str))

    mystery_iso2 = str(mystery.get("iso2", "")).upper()
    if diff_key != "easy" and mystery_iso2:
        same_country = candidates[candidates["iso2"].str.upper() == mystery_iso2]
        if len(same_country) >= n * 2:
            candidates = same_country

    scale_km = math.exp(random.uniform(math.log(scale_range[0]), math.log(scale_range[1])))
    jitter = random.uniform(-10.0, 10.0)

    best: list[dict] = []
    for min_dist in (max(base_min_dist, 0.25 * scale_km), 0.0):
        pool = candidates[candidates["_dist_km"] >= min_dist]
        picked = _pick_by_sectors(pool, n, min_gap_deg, jitter, scale_km, prominence_tiers=prominence_tiers) if len(pool) else []
        if len(picked) > len(best):
            best = picked
        if len(best) >= min(n, _MIN_HINTS_BEFORE_RELAXING):
            break
    return best[:n]


# ── Flag loading ───────────────────────────────────────────────────────────────


def _load_flag(iso2: str) -> Image.Image | None:
    path = FLAGS_DIR_PATH / f"{iso2.upper()}.jpg"
    if not path.exists():
        return None
    try:
        img = Image.open(path).convert("RGBA").resize((_FLAG_W, _FLAG_H), Image.LANCZOS)
        border_w = 2 * _SCALE
        bordered = Image.new("RGBA", (_FLAG_W + border_w * 2, _FLAG_H + border_w * 2), (170, 170, 170, 255))
        bordered.paste(img, (border_w, border_w))
        return bordered
    except Exception:
        return None


# ── Diagram generation ─────────────────────────────────────────────────────────


def generate_diagram(
    mystery_lon: float,
    mystery_lat: float,
    hints: list[dict],
    show_distances: bool = False,
) -> BytesIO:
    """
    Render a compass-rose diagram as a JPEG BytesIO using 2x supersampling for crisp edges.

    Args:
        mystery_lon/lat: coordinates of the mystery city (centre dot).
        hints: list of city row dicts (each must have lng, lat, city, iso2 and optionally _dist_km).
        show_distances: when True, render km distances below each city label.

    Returns:
        BytesIO containing the JPEG image data.
    """
    canvas_w, canvas_h = _CANVAS_W, _CANVAS_H
    cx, cy = canvas_w // 2, canvas_h // 2

    img = Image.new("RGBA", (canvas_w, canvas_h), _BG + (255,))
    draw = ImageDraw.Draw(img)

    # ── Center circle ──────────────────────────────────────────────────────────
    draw.ellipse(
        [cx - _CIRCLE_R, cy - _CIRCLE_R, cx + _CIRCLE_R, cy + _CIRCLE_R],
        fill=_CIRCLE_FILL,
        outline=_CIRCLE_OUTLINE,
        width=_CIRCLE_W,
    )
    draw.text((cx, cy - 16 * _SCALE), "City", fill=_TEXT_COLOR, font=_FONT_CENTER, anchor="mm")
    draw.text((cx, cy + 20 * _SCALE), "?", fill=_TEXT_COLOR, font=_FONT_Q, anchor="mm")

    # ── Per-hint arrows + labels ───────────────────────────────────────────────
    for hint in hints:
        hlon = float(hint["lng"])
        hlat = float(hint["lat"])
        hname = str(hint.get("city", hint.get("city_ascii", "?")))
        iso2 = str(hint.get("iso2", ""))
        dist_km = hint.get("_dist_km") or _haversine(mystery_lon, mystery_lat, hlon, hlat)

        # EXACT compass bearing (0°=N, 90°=E, 180°=S, 270°=W)
        bearing = _bearing_deg(mystery_lon, mystery_lat, hlon, hlat)
        b_rad = math.radians(bearing)

        # Compass on screen: 0° (North) is straight up (dx=0, dy=-1)
        sin_b = math.sin(b_rad)
        cos_b = math.cos(b_rad)

        sx = cx + sin_b * _CIRCLE_R
        sy = cy - cos_b * _CIRCLE_R
        tx = cx + sin_b * (_CIRCLE_R + _ARROW_LEN)
        ty = cy - cos_b * (_CIRCLE_R + _ARROW_LEN)

        # Main shaft
        draw.line([(sx, sy), (tx, ty)], fill=_ARROW_COLOR, width=_ARROW_W)

        # Draw solid, smooth arrowhead polygon
        tip = (tx, ty)
        base_x = tx - sin_b * _HEAD_LEN
        base_y = ty + cos_b * _HEAD_LEN
        norm_x = cos_b * (_HEAD_WIDTH / 2)
        norm_y = sin_b * (_HEAD_WIDTH / 2)

        wing1 = (base_x + norm_x, base_y + norm_y)
        wing2 = (base_x - norm_x, base_y - norm_y)
        draw.polygon([tip, wing1, wing2], fill=_ARROW_COLOR)

        # Label group: flag + city name [+ distance]
        flag_img = _load_flag(iso2)
        fw = (flag_img.width + _LABEL_PAD) if flag_img else 0
        fh = flag_img.height if flag_img else 0

        name_bb = draw.textbbox((0, 0), hname, font=_FONT_BOLD)
        name_w = name_bb[2] - name_bb[0]
        name_h = name_bb[3] - name_bb[1]

        dist_text = f"{int(dist_km):,} km" if show_distances else ""
        dist_h = 0
        dist_w = 0
        if dist_text:
            dist_bb = draw.textbbox((0, 0), dist_text, font=_FONT_DIST)
            dist_w = dist_bb[2] - dist_bb[0]
            dist_h = dist_bb[3] - dist_bb[1]

        group_w = fw + max(name_w, dist_w)
        gap_between_name_and_dist = 6 * _SCALE if dist_text else 0
        group_h = max(fh, name_h + gap_between_name_and_dist + dist_h)

        # Gap beyond arrow tip
        gap = 18 * _SCALE
        lx = tx + sin_b * gap
        ly = ty - cos_b * gap

        # Position box based on direction
        if sin_b >= 0.15:  # East side
            gx = lx
        elif sin_b <= -0.15:  # West side
            gx = lx - group_w
        else:  # Directly North or South
            gx = lx - group_w / 2

        if cos_b >= 0.15:  # North side (above)
            gy = ly - group_h
        elif cos_b <= -0.15:  # South side (below)
            gy = ly
        else:
            gy = ly - group_h / 2

        # Clamp to canvas
        gx = max(10 * _SCALE, min(canvas_w - group_w - 10 * _SCALE, gx))
        gy = max(10 * _SCALE, min(canvas_h - group_h - 10 * _SCALE, gy))

        if flag_img:
            fy = int(gy + max(0, (group_h - fh) // 2))
            img.paste(flag_img, (int(gx), fy), flag_img)

        tx_label = int(gx + fw)
        draw.text((tx_label, int(gy)), hname, fill=_TEXT_COLOR, font=_FONT_BOLD)
        if dist_text:
            draw.text(
                (tx_label, int(gy + name_h + gap_between_name_and_dist)),
                dist_text,
                fill=_DIST_COLOR,
                font=_FONT_DIST,
            )

    # ── Downsample with Lanczos for hardware-grade anti-aliasing ───────────────
    final_img = img.resize((_OUT_W, _OUT_H), Image.Resampling.LANCZOS).convert("RGB")
    buf = BytesIO()
    final_img.save(buf, "JPEG", quality=95)
    buf.seek(0)
    return buf


# ── CityQuiz (BaseGuessQuiz subclass) ─────────────────────────────────────────


class CityQuiz(BaseGuessQuiz):
    """Compass-rose city guessing quiz."""

    QUIZ_TYPE = "city"
    FILTER_LABEL = "Scope"
    FILTER_KEY = "scope"
    ITEM_KEY = "city"
    TOTAL_TIPS = 4

    REWARD_LEVELS = {
        "easy": 5_000,
        "medium": 10_000,
        "hard": 20_000,
        "very_hard": 35_000,
        "crazy": 50_000,
    }
    TIP_PENALTY_PCTS = (0, 10, 25, 45, 70)

    @classmethod
    def get_reward(cls, difficulty: str, continent_specified: bool = False, tips_given: int = 0) -> tuple[int, int]:
        base_reward = cls.REWARD_LEVELS.get(difficulty, cls.REWARD_LEVELS.get("crazy", 0))
        if continent_specified:
            base_reward = base_reward // 2

        if tips_given <= 0:
            return base_reward, 0

        penalty_pct = cls.TIP_PENALTY_PCTS[min(tips_given, len(cls.TIP_PENALTY_PCTS) - 1)]
        current_reward = int(round(base_reward * (1.0 - penalty_pct / 100.0)))
        return current_reward, penalty_pct

    def get_display_name(self, item: dict | pd.Series) -> str:
        city = item if isinstance(item, dict) else item.to_dict()
        return str(city.get("city", city.get("city_ascii", "?")))

    def get_description(self, item: dict | pd.Series) -> str:
        city = item if isinstance(item, dict) else item.to_dict()
        country = city.get("country", "")
        continent = city.get("continent", "")
        pop = city.get("population", 0)
        pop_str = f"{int(pop):,}" if pop else "unknown"
        return f"{country} ({continent}) — population: {pop_str}"

    def get_tips(self, item: dict | pd.Series, continent_specified: bool = False) -> list[str]:
        """
        4 progressive tip descriptions:
        1. Kilometers on the compass diagram
        2. Population of the guessed city
        3. First letter
        4. Multiple-choice buttons
        """
        city = item if isinstance(item, dict) else item.to_dict()
        name = self.get_display_name(city)
        if not name:
            return []
        pop = city.get("population", 0)
        pop_str = f"{int(pop):,}" if pop else "unknown"
        return [
            "📏 Distances revealed on compass diagram",
            f"👥 Population: *{pop_str}*",
            f"The city name starts with: *{name[0].upper()}*",
            "🎯 4 Multiple-choice buttons enabled below",
        ]

    @classmethod
    def generate_mc_options(cls, mystery_city: dict | pd.Series, cities_model, n: int = 4) -> list[str]:
        """
        Generate *n* multiple choice options: the correct city name plus smart decoys.
        Decoys are selected from the same country and must start with the same first letter
        and match a similar population tier so they don't spoil previous clues.
        """
        city_dict = mystery_city if isinstance(mystery_city, dict) else mystery_city.to_dict()
        correct_name = str(city_dict.get("city", city_dict.get("city_ascii", "")))
        first_char = correct_name[0].upper() if correct_name else ""
        target_pop = max(float(city_dict.get("population", 0) or 0), 10_000.0)
        iso2 = str(city_dict.get("iso2", "")).upper()
        continent = str(city_dict.get("continent", "")).lower()

        df = cities_model.df if hasattr(cities_model, "df") else cities_model

        candidates = df[df["id"] != city_dict.get("id")].copy()
        candidates = candidates[candidates["city_ascii"].str.lower() != correct_name.lower()]
        candidates = candidates[candidates["city"].str.lower() != correct_name.lower()]

        # Filter matching first letter
        candidates = candidates[
            candidates["city"].str.startswith(first_char, na=False) | candidates["city_ascii"].str.startswith(first_char, na=False)
        ]

        candidates["_pop"] = pd.to_numeric(candidates["population"], errors="coerce").fillna(0).clip(lower=1_000)
        candidates["_pop_diff"] = (np.log10(candidates["_pop"]) - np.log10(target_pop)).abs()

        needed = n - 1
        picked_names: list[str] = []

        # 1. Same country
        country_pool = candidates[candidates["iso2"].str.upper() == iso2].sort_values("_pop_diff")
        for _, row in country_pool.iterrows():
            cand_name = str(row.get("city") or row.get("city_ascii"))
            if cand_name and cand_name not in picked_names and cand_name != correct_name:
                picked_names.append(cand_name)
                if len(picked_names) == needed:
                    break

        # 2. Same continent
        if len(picked_names) < needed and continent:
            cont_pool = candidates[candidates["continent"].str.lower() == continent].sort_values("_pop_diff")
            for _, row in cont_pool.iterrows():
                cand_name = str(row.get("city") or row.get("city_ascii"))
                if cand_name and cand_name not in picked_names and cand_name != correct_name:
                    picked_names.append(cand_name)
                    if len(picked_names) == needed:
                        break

        # 3. Global matching first letter
        if len(picked_names) < needed:
            global_pool = candidates.sort_values("_pop_diff")
            for _, row in global_pool.iterrows():
                cand_name = str(row.get("city") or row.get("city_ascii"))
                if cand_name and cand_name not in picked_names and cand_name != correct_name:
                    picked_names.append(cand_name)
                    if len(picked_names) == needed:
                        break

        # 4. Fallback if letter was rare
        if len(picked_names) < needed:
            fallback_pool = df[(df["id"] != city_dict.get("id")) & (df["iso2"].str.upper() == iso2)]
            if len(fallback_pool) < needed:
                fallback_pool = df[df["id"] != city_dict.get("id")]
            for _, row in fallback_pool.iterrows():
                cand_name = str(row.get("city") or row.get("city_ascii"))
                if cand_name and cand_name not in picked_names and cand_name != correct_name:
                    picked_names.append(cand_name)
                    if len(picked_names) == needed:
                        break

        options = [correct_name] + picked_names[:needed]
        random.shuffle(options)
        return options

    def get_valid_answers(self, item: dict | pd.Series) -> set[str]:
        city = item if isinstance(item, dict) else item.to_dict()
        answers: set[str] = set()
        for field in ("city", "city_ascii"):
            val = str(city.get(field, "")).strip()
            if val and val.lower() != "nan":
                answers.add(val.lower())
        return answers

    def get_image_path(self, item: dict | pd.Series) -> str | None:
        return None

    def format_timeout_message(self, item: dict | pd.Series, **kwargs) -> str:
        name = self.get_display_name(item)
        desc = self.get_description(item)
        return f"Time's up! The city was *{name}*.\n\n{desc}"

    def format_correct_message(self, item: dict | pd.Series, reward: int, total_credits: int | None = None, **kwargs) -> str:
        name = self.get_display_name(item)
        desc = self.get_description(item)
        credits_part = f"You receive *{reward}* credits!"
        if total_credits is not None:
            credits_part += f" [*{total_credits}* in total]"
        return f"Correct! The city was *{name}*.\n{credits_part}\n\n{desc}"

    def format_wrong_message(self, item: dict | pd.Series, **kwargs) -> str:
        name = self.get_display_name(item)
        desc = self.get_description(item)
        return f"Wrong! The correct answer was *{name}*.\n\n{desc}"
