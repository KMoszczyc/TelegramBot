"""Vector post-solve map for the city quiz: Natural Earth 10m geometry drawn crisply with Pillow."""

import math
from dataclasses import dataclass
from functools import lru_cache
from io import BytesIO

import numpy as np
import shapefile
import shapely
from PIL import Image, ImageDraw, ImageFilter, ImageFont

from src.config.paths import CITY_QUIZ_GEO_DIR_PATH, FLAGS_DIR_PATH
from src.core.font_utils import get_font
from src.models.quizzes.city_quiz import _haversine

_OUT_W = 1600
_OUT_H = 1000
_SS = 2
_W = _OUT_W * _SS
_H = _OUT_H * _SS

_EARTH_R_KM = 6_371.0
_MIN_SPAN_KM = 170.0
_FILL_X = 0.58
_FILL_Y = 0.50

_OCEAN = (176, 208, 232)
_LAND = (246, 243, 235)
_COAST = (112, 143, 172)
_BORDER = (176, 158, 190)
_DISTRICT_BORDER = (210, 202, 218)
_SLATE = (30, 41, 59)
_SLATE_SOFT = (100, 116, 139)
_ROSE = (225, 29, 72)
_BLUE = (37, 99, 235)
_WHITE = (255, 255, 255)
_SHADOW = (15, 23, 42)

_FONT_TITLE = 40
_FONT_CITY = 33
_FONT_DIST = 25
_CARD_PAD_X = 18
_CARD_PAD_Y = 12
_CARD_RADIUS = 14
_FLAG_H = 36
_CARD_MARGIN = 20

_GAPS = (18, 44, 80, 130, 200, 300)
_DIRECTIONS = [(1, 0), (-1, 0), (0, -1), (0, 1), (1, -1), (-1, -1), (1, 1), (-1, 1)]


Box = tuple[float, float, float, float]


@dataclass(frozen=True)
class _Shape:
    bbox: tuple[float, float, float, float]
    rings: tuple[np.ndarray, ...]
    is_hole: tuple[bool, ...]
    ring_bboxes: tuple[tuple[float, float, float, float], ...]


@dataclass(frozen=True)
class _Geometry:
    land: list[_Shape]
    lakes: list[_Shape]
    borders: list[_Shape]
    district_borders: list[_Shape] | None = None


@dataclass
class _Card:
    name: str
    dist_text: str | None
    flag: Image.Image | None
    pin: tuple[float, float]
    is_target: bool
    w: int = 0
    h: int = 0
    box: Box | None = None
    anchor: tuple[float, float] | None = None


def _font(size: int, bold: bool) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    return get_font(size * _SS, bold=bold)


@lru_cache(maxsize=64)
def _card_flag(iso2: str) -> Image.Image | None:
    path = FLAGS_DIR_PATH / f"{iso2.upper()}.jpg"
    if not path.exists():
        return None
    try:
        src = Image.open(path).convert("RGB")
    except OSError:
        return None
    h = _FLAG_H * _SS
    w = round(h * src.width / src.height)
    flag = src.resize((w, h), Image.Resampling.LANCZOS)
    border = max(1, _SS)
    framed = Image.new("RGB", (w + 2 * border, h + 2 * border), _SLATE_SOFT)
    framed.paste(flag, (border, border))
    return framed


def _read_layer(name: str, polygons: bool) -> list[_Shape] | None:
    shp_path = CITY_QUIZ_GEO_DIR_PATH / f"{name}.shp"
    shx_path = CITY_QUIZ_GEO_DIR_PATH / f"{name}.shx"
    if not shp_path.exists() or not shx_path.exists():
        return None
    shapes: list[_Shape] = []
    with open(shp_path, "rb") as shp, open(shx_path, "rb") as shx:
        reader = shapefile.Reader(shp=shp, shx=shx)
        for geom in reader.shapes():
            if not geom.points:
                continue
            pts = np.asarray(geom.points, dtype=float)
            bounds = [*geom.parts, len(pts)]
            rings, holes, ring_boxes = [], [], []
            for a, b in zip(bounds[:-1], bounds[1:], strict=True):
                ring = pts[a:b]
                if len(ring) < 2:
                    continue
                x, y = ring[:, 0], ring[:, 1]
                signed_area = 0.5 * float(np.sum(x[:-1] * y[1:] - x[1:] * y[:-1]))
                rings.append(ring)
                holes.append(polygons and signed_area > 0)
                ring_boxes.append((float(x.min()), float(y.min()), float(x.max()), float(y.max())))
            shapes.append(_Shape(tuple(float(v) for v in geom.bbox), tuple(rings), tuple(holes), tuple(ring_boxes)))
    return shapes


@lru_cache(maxsize=1)
def _load_geometry() -> _Geometry | None:
    """Parse and cache the shipped Natural Earth layers (None when the files are missing)."""
    land = _read_layer("ne_10m_land", True)
    lakes = _read_layer("ne_10m_lakes", True)
    borders = _read_layer("ne_10m_admin_0_boundary_lines_land", False)
    district_borders = _read_layer("ne_10m_admin_1_states_provinces_lines", False)
    if land is None:
        return None
    return _Geometry(
        land=land,
        lakes=lakes or [],
        borders=borders or [],
        district_borders=district_borders or [],
    )


def _merc_y(lat: np.ndarray | float) -> np.ndarray | float:
    lat = np.clip(lat, -85.0, 85.0)
    return np.log(np.tan(np.pi / 4 + np.radians(lat) / 2))


class _View:
    """Mercator viewport (conformal, so shapes are never stretched) fitted around the cities."""

    def __init__(self, lons: np.ndarray, lats: np.ndarray) -> None:
        xs = np.radians(lons)
        ys = _merc_y(lats)
        mid_lat = float(np.mean(lats))
        min_span = _MIN_SPAN_KM / _EARTH_R_KM / max(0.2, math.cos(math.radians(mid_lat)))
        span_x = max(float(xs.max() - xs.min()), min_span * _W / _H)
        span_y = max(float(ys.max() - ys.min()), min_span)
        self.cx = float(xs.max() + xs.min()) / 2
        self.cy = float(ys.max() + ys.min()) / 2
        self.scale = min(_W * _FILL_X / span_x, _H * _FILL_Y / span_y)

    @property
    def is_big_zoom(self) -> bool:
        """True when the viewport is zoomed into regional/local scale (scale >= 18,000, approx <= 1,000 km span)."""
        return self.scale >= 18_000

    def to_px(self, lons: np.ndarray | float, lats: np.ndarray | float):
        px = (np.radians(lons) - self.cx) * self.scale + _W / 2
        py = (self.cy - _merc_y(lats)) * self.scale + _H / 2
        return px, py

    def lonlat_bounds(self) -> tuple[float, float, float, float]:
        half_x, half_y = _W / 2 / self.scale, _H / 2 / self.scale

        def lat_of(y: float) -> float:
            return math.degrees(2 * math.atan(math.exp(y)) - math.pi / 2)

        return (
            math.degrees(self.cx - half_x) - 1,
            lat_of(self.cy - half_y) - 1,
            math.degrees(self.cx + half_x) + 1,
            lat_of(self.cy + half_y) + 1,
        )


def _thin(points: np.ndarray, min_step_px: float) -> np.ndarray:
    """Drop vertices that sit closer than *min_step_px* along the path to the last kept one."""
    if len(points) < 64:
        return points
    steps = np.hypot(*np.diff(points, axis=0).T)
    bucket = np.floor(np.concatenate(([0.0], np.cumsum(steps))) / min_step_px)
    keep = np.concatenate(([True], np.diff(bucket) > 0))
    keep[-1] = True
    return points[keep]


def _flat(view: _View, ring: np.ndarray) -> list[list[float]]:
    """
    Project a ring to pixel space, thinned to sub-pixel detail. Rings reaching far beyond the
    canvas are clipped exactly, since rasterising huge off-screen polygons is the slow path.
    """
    px, py = view.to_px(ring[:, 0], ring[:, 1])
    pts = _thin(np.column_stack((px, py)), 1.5)
    if len(pts) < 4:
        return []
    if pts.min() >= -_W and pts[:, 0].max() <= 2 * _W and pts[:, 1].max() <= 2 * _H:
        return [pts.ravel().tolist()]
    try:
        clipped = shapely.clip_by_rect(shapely.Polygon(pts), -30, -30, _W + 30, _H + 30)
    except shapely.errors.GEOSException:
        return [pts.ravel().tolist()]
    parts = [g for g in shapely.get_parts(clipped) if isinstance(g, shapely.Polygon) and not g.is_empty]
    return [np.asarray(g.exterior.coords).ravel().tolist() for g in parts]


def _visible(bbox: tuple[float, float, float, float], bounds: tuple[float, float, float, float]) -> bool:
    return not (bbox[2] < bounds[0] or bbox[0] > bounds[2] or bbox[3] < bounds[1] or bbox[1] > bounds[3])


def _draw_polygons(draw: ImageDraw.ImageDraw, view: _View, shapes: list[_Shape], fill, hole_fill, outline, width) -> None:
    bounds = view.lonlat_bounds()
    for shape in shapes:
        if not _visible(shape.bbox, bounds):
            continue
        flats = [
            (pts, hole)
            for ring, hole, box in zip(shape.rings, shape.is_hole, shape.ring_bboxes, strict=True)
            if _visible(box, bounds)
            for pts in _flat(view, ring)
        ]
        for pts, hole in flats:
            if not hole and len(pts) >= 6:
                draw.polygon(pts, fill=fill)
        for pts, hole in flats:
            if hole and len(pts) >= 6:
                draw.polygon(pts, fill=hole_fill)
        for pts, _ in flats:
            if len(pts) >= 4:
                draw.line(pts, fill=outline, width=width)


def _draw_borders(
    draw: ImageDraw.ImageDraw,
    view: _View,
    shapes: list[_Shape],
    fill: tuple[int, int, int] = _BORDER,
    width: int | None = None,
) -> None:
    stroke_w = width if width is not None else 3 * _SS
    bounds = view.lonlat_bounds()
    for shape in shapes:
        if not _visible(shape.bbox, bounds):
            continue
        for ring, box in zip(shape.rings, shape.ring_bboxes, strict=True):
            if not _visible(box, bounds):
                continue
            px, py = view.to_px(ring[:, 0], ring[:, 1])
            pts = _thin(np.column_stack((px, py)), 1.5).ravel().tolist()
            if len(pts) >= 4:
                draw.line(pts, fill=fill, width=stroke_w, joint="curve")


def _draw_base_map(draw: ImageDraw.ImageDraw, view: _View, geo: _Geometry, show_district_borders: bool = False) -> None:
    _draw_polygons(draw, view, geo.land, _LAND, _OCEAN, _COAST, 2 * _SS)
    _draw_polygons(draw, view, geo.lakes, _OCEAN, _LAND, _COAST, _SS)
    if show_district_borders and geo.district_borders:
        _draw_borders(draw, view, geo.district_borders, fill=_DISTRICT_BORDER, width=max(1, int(1.5 * _SS)))
    _draw_borders(draw, view, geo.borders)


def _boxes_overlap(a: Box, b: Box, margin: float) -> bool:
    return not (a[2] + margin < b[0] or a[0] - margin > b[2] or a[3] + margin < b[1] or a[1] - margin > b[3])


def _nearest_on_box(box: Box, p: tuple[float, float]) -> tuple[float, float]:
    return min(max(p[0], box[0]), box[2]), min(max(p[1], box[1]), box[3])


def _segment_hits_box(p: tuple[float, float], q: tuple[float, float], box: Box, pad: float) -> bool:
    for i in range(1, 24):
        t = i / 24
        x, y = p[0] + (q[0] - p[0]) * t, p[1] + (q[1] - p[1]) * t
        if box[0] - pad <= x <= box[2] + pad and box[1] - pad <= y <= box[3] + pad:
            return True
    return False


def _candidate_boxes(card: _Card, centroid: tuple[float, float]):
    """Yield candidate card boxes around the pin, outward-facing placements first."""
    px, py = card.pin
    out_angle = math.atan2(py - centroid[1], px - centroid[0])

    def outwardness(d: tuple[int, int]) -> float:
        diff = abs(math.atan2(d[1], d[0]) - out_angle) % (2 * math.pi)
        return min(diff, 2 * math.pi - diff)

    ordered = sorted(_DIRECTIONS, key=outwardness)
    w, h = card.w, card.h
    for gap in _GAPS:
        g = gap * _SS
        for dx, dy in ordered:
            x0 = px + g * 0.75 if dx > 0 else px - g * 0.75 - w if dx < 0 else px - w / 2
            y0 = py + g * 0.75 if dy > 0 else py - g * 0.75 - h if dy < 0 else py - h / 2
            if dx and not dy:
                x0 = px + g if dx > 0 else px - g - w
            if dy and not dx:
                y0 = py + g if dy > 0 else py - g - h
            yield (x0, y0, x0 + w, y0 + h)


def _place_cards(cards: list[_Card]) -> None:
    """Assign non-overlapping boxes: no card covers a pin, another card, or another leader line."""
    centroid = (sum(c.pin[0] for c in cards) / len(cards), sum(c.pin[1] for c in cards) / len(cards))
    margin = _CARD_MARGIN * _SS
    pin_pad = 16 * _SS
    gap_between = 12 * _SS
    placed: list[_Card] = []

    for card in cards:
        fallback: Box | None = None
        chosen: Box | None = None
        for raw in _candidate_boxes(card, centroid):
            x0 = min(max(raw[0], margin), _W - margin - card.w)
            y0 = min(max(raw[1], margin), _H - margin - card.h)
            box = (x0, y0, x0 + card.w, y0 + card.h)
            if any(_boxes_overlap(box, p.box, gap_between) for p in placed):
                continue
            fallback = fallback or box
            if any(box[0] - pin_pad <= c.pin[0] <= box[2] + pin_pad and box[1] - pin_pad <= c.pin[1] <= box[3] + pin_pad for c in cards):
                continue
            anchor = _nearest_on_box(box, card.pin)
            if any(_segment_hits_box(card.pin, anchor, p.box, 2 * _SS) for p in placed):
                continue
            if any(_segment_hits_box(p.pin, p.anchor, box, 2 * _SS) for p in placed):
                continue
            chosen = box
            break
        card.box = chosen or fallback or (margin, margin, margin + card.w, margin + card.h)
        card.anchor = _nearest_on_box(card.box, card.pin)
        placed.append(card)


def _measure_card(draw: ImageDraw.ImageDraw, card: _Card) -> None:
    name_font = _font(_FONT_TITLE if card.is_target else _FONT_CITY, True)
    width = 2 * _CARD_PAD_X * _SS + draw.textlength(card.name, font=name_font)
    height = 0.0
    if card.flag:
        width += card.flag.width + 12 * _SS
        height = card.flag.height
    if card.dist_text:
        width += 14 * _SS + draw.textlength(card.dist_text, font=_font(_FONT_DIST, False))
    cap = name_font.getbbox("Hg")
    height = max(height, cap[3] - cap[1])
    card.w = round(width)
    card.h = round(height + 2 * _CARD_PAD_Y * _SS)


def _draw_card(img: Image.Image, draw: ImageDraw.ImageDraw, card: _Card) -> None:
    x0, y0, x1, y1 = card.box
    border = _ROSE if card.is_target else _SLATE
    draw.rounded_rectangle(card.box, radius=_CARD_RADIUS * _SS, fill=_WHITE, outline=border, width=(5 if card.is_target else 4) * _SS)

    name_font = _font(_FONT_TITLE if card.is_target else _FONT_CITY, True)
    cy = (y0 + y1) / 2
    x = x0 + _CARD_PAD_X * _SS
    if card.flag:
        img.paste(card.flag, (round(x), round(cy - card.flag.height / 2)))
        x += card.flag.width + 12 * _SS
    draw.text((x, cy), card.name, font=name_font, fill=_ROSE if card.is_target else _SLATE, anchor="lm")
    if card.dist_text:
        x += draw.textlength(card.name, font=name_font) + 14 * _SS
        draw.text((x, cy + 2 * _SS), card.dist_text, font=_font(_FONT_DIST, False), fill=_SLATE_SOFT, anchor="lm")


def _draw_shadows(img: Image.Image, cards: list[_Card]) -> None:
    mask = Image.new("L", img.size, 0)
    mdraw = ImageDraw.Draw(mask)
    off = 6 * _SS
    for c in cards:
        x0, y0, x1, y1 = c.box
        mdraw.rounded_rectangle((x0 + 2 * _SS, y0 + off, x1 + 2 * _SS, y1 + off), radius=_CARD_RADIUS * _SS, fill=110)
    mask = mask.filter(ImageFilter.GaussianBlur(7 * _SS))
    img.paste(_SHADOW, mask=mask)


def _draw_pin(draw: ImageDraw.ImageDraw, pos: tuple[float, float], is_target: bool) -> None:
    x, y = pos
    r = (13 if is_target else 9) * _SS
    ring = r + (5 if is_target else 4) * _SS
    draw.ellipse((x - ring, y - ring, x + ring, y + ring), fill=_WHITE)
    draw.ellipse((x - r, y - r, x + r, y + r), fill=_ROSE if is_target else _BLUE)


def _city_name(row: dict) -> str:
    return str(row.get("city") or row.get("city_ascii") or "")


def _build_cards(view: _View, mystery_city: dict, hints: list[dict]) -> list[_Card]:
    def pin_of(row: dict) -> tuple[float, float]:
        px, py = view.to_px(float(row["lng"]), float(row["lat"]))
        return float(px), float(py)

    cards = [_Card(_city_name(mystery_city), None, _card_flag(str(mystery_city.get("iso2", ""))), pin_of(mystery_city), True)]
    m_lon, m_lat = float(mystery_city["lng"]), float(mystery_city["lat"])
    for h in hints:
        dist = h.get("_dist_km")
        dist = _haversine(m_lon, m_lat, float(h["lng"]), float(h["lat"])) if dist is None else float(dist)
        cards.append(_Card(_city_name(h), f"{round(dist):,} km", _card_flag(str(h.get("iso2", ""))), pin_of(h), False))
    return cards


def render_post_solve_map(mystery_city: dict, hints: list[dict], district_mode: bool = False) -> BytesIO | None:
    """
    Render the reveal map: crisp vector coastlines and borders (Mercator, so nothing is stretched),
    the mystery city highlighted and each hint city in a collision-free card with its distance.
    Returns JPEG bytes, or None when the geometry files are unavailable.
    """
    geo = _load_geometry()
    if geo is None:
        return None

    rows = [mystery_city, *hints]
    lons = np.array([float(r["lng"]) for r in rows])
    lats = np.array([float(r["lat"]) for r in rows])
    view = _View(lons, lats)

    img = Image.new("RGB", (_W, _H), _OCEAN)
    draw = ImageDraw.Draw(img)
    show_district_borders = district_mode or view.is_big_zoom
    _draw_base_map(draw, view, geo, show_district_borders=show_district_borders)

    cards = _build_cards(view, mystery_city, hints)
    for card in cards:
        _measure_card(draw, card)
    _place_cards(cards)

    for card in cards:
        draw.line([card.pin, card.anchor], fill=_ROSE if card.is_target else _SLATE, width=(4 if card.is_target else 3) * _SS)
    _draw_shadows(img, cards)
    draw = ImageDraw.Draw(img)
    for card in cards:
        _draw_card(img, draw, card)
    for card in reversed(cards):
        _draw_pin(draw, card.pin, card.is_target)

    final = img.resize((_OUT_W, _OUT_H), Image.Resampling.LANCZOS)
    buf = BytesIO()
    final.save(buf, "JPEG", quality=92, optimize=True)
    buf.seek(0)
    return buf
