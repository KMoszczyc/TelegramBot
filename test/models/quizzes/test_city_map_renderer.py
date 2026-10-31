import numpy as np
import pytest
from PIL import Image

from src.models.quizzes import city_map_renderer as cmr
from src.models.quizzes.city_map_renderer import render_post_solve_map


@pytest.fixture
def mystery():
    return {"city": "Warsaw", "lat": 52.2297, "lng": 21.0122, "iso2": "PL"}


@pytest.fixture
def hints():
    return [
        {"city": "Gdańsk", "lat": 54.352, "lng": 18.6466, "iso2": "PL", "_dist_km": 283},
        {"city": "Kraków", "lat": 50.0647, "lng": 19.945, "iso2": "PL", "_dist_km": 252},
        {"city": "Berlin", "lat": 52.52, "lng": 13.405, "iso2": "DE"},
    ]


@pytest.mark.parametrize("hint_count", [0, 1, 3])
def test_render_post_solve_map_returns_jpeg(mystery, hints, hint_count):
    buf = render_post_solve_map(mystery, hints[:hint_count])
    assert buf is not None
    img = Image.open(buf)
    assert img.format == "JPEG"
    assert img.size == (cmr._OUT_W, cmr._OUT_H)


def test_render_post_solve_map_without_geometry(mystery, monkeypatch):
    monkeypatch.setattr(cmr, "_load_geometry", lambda: None)
    assert render_post_solve_map(mystery, []) is None


@pytest.mark.parametrize("n_points, step", [(10, 2.0), (500, 1.5), (5000, 3.0)])
def test_thin_keeps_endpoints_and_respects_step(n_points, step):
    t = np.linspace(0, 100, n_points)
    pts = np.column_stack((t, np.zeros_like(t)))
    out = cmr._thin(pts, step)
    assert (out[0] == pts[0]).all() and (out[-1] == pts[-1]).all()
    if n_points >= 64:
        assert len(out) < n_points
        gaps = np.diff(out[:, 0])
        assert gaps[:-1].min() >= step * 0.5


def test_view_centres_cities_and_keeps_them_inside_the_canvas():
    lons, lats = np.array([21.0, 18.6, 13.4]), np.array([52.2, 54.4, 52.5])
    view = cmr._View(lons, lats)
    px, py = view.to_px(lons, lats)
    assert (px > 0).all() and (px < cmr._W).all() and (py > 0).all() and (py < cmr._H).all()
    assert abs((px.min() + px.max()) / 2 - cmr._W / 2) < 1
    lon0, lat0, lon1, lat1 = view.lonlat_bounds()
    assert lon0 < lons.min() and lon1 > lons.max() and lat0 < lats.min() and lat1 > lats.max()


def test_view_enforces_minimum_span():
    view = cmr._View(np.array([20.0, 20.001]), np.array([50.0, 50.001]))
    px, _ = view.to_px(np.array([20.0, 20.001]), np.array([50.0, 50.001]))
    assert abs(px[1] - px[0]) < cmr._W * 0.05


def test_place_cards_never_overlap_or_cover_pins():
    cards = [cmr._Card(f"City{i}", "100 km", None, (800 + i * 30, 500 + (i % 2) * 20), i == 0, w=520, h=80) for i in range(6)]
    cmr._place_cards(cards)
    for i, a in enumerate(cards):
        assert a.box[0] >= 0 and a.box[2] <= cmr._W and a.box[1] >= 0 and a.box[3] <= cmr._H
        for b in cards[i + 1 :]:
            assert not cmr._boxes_overlap(a.box, b.box, 0)
        for c in cards:
            assert not (a.box[0] <= c.pin[0] <= a.box[2] and a.box[1] <= c.pin[1] <= a.box[3])


@pytest.mark.parametrize(
    "box, point, expected",
    [((0, 0, 10, 10), (5, 5), (5, 5)), ((0, 0, 10, 10), (20, 5), (10, 5)), ((0, 0, 10, 10), (-3, -4), (0, 0))],
)
def test_nearest_on_box(box, point, expected):
    assert cmr._nearest_on_box(box, point) == expected


@pytest.mark.parametrize(
    "p, q, hits",
    [((0, 0), (100, 0), True), ((0, 50), (100, 50), False), ((0, 0), (20, 0), False)],
)
def test_segment_hits_box(p, q, hits):
    assert cmr._segment_hits_box(p, q, (40, -5, 60, 5), 0) is hits


def test_render_post_solve_map_district_mode(mystery, hints):
    buf = render_post_solve_map(mystery, hints, district_mode=True)
    assert buf is not None
    img = Image.open(buf)
    assert img.format == "JPEG"
    assert img.size == (cmr._OUT_W, cmr._OUT_H)


def test_view_is_big_zoom():
    tight_view = cmr._View(np.array([21.0, 21.2]), np.array([52.0, 52.2]))
    assert tight_view.is_big_zoom

    broad_view = cmr._View(np.array([-10.0, 40.0]), np.array([35.0, 65.0]))
    assert not broad_view.is_big_zoom


def test_load_geometry_has_district_borders():
    geo = cmr._load_geometry()
    assert geo is not None
    assert geo.district_borders is not None
    assert len(geo.district_borders) > 0
