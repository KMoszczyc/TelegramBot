import os
from unittest.mock import patch

import matplotlib

matplotlib.use("Agg")

import pytest

from src.models.map_quiz import _MIN_LAT_LON_RATIO, MapQuiz


@pytest.fixture()
def quiz():
    return MapQuiz(padding_deg=15.0)


# ── _compute_extent ──────────────────────────────────────────────────


@pytest.mark.parametrize(
    "locations, expected_center",
    [
        pytest.param([(-10.0, 30.0, "A", "red"), (10.0, 50.0, "B", "green")], (0.0, 40.0), id="europe_symmetric"),
        pytest.param([(-74.0, 40.7, "NYC", "red"), (139.65, 35.68, "TKY", "green")], (32.825, 38.19), id="nyc_tokyo"),
        pytest.param([(25.0, 60.0, "A", "red")], (25.0, 60.0), id="single_location"),
        pytest.param(
            [(18.07, 59.33, "STO", "red"), (24.94, 60.17, "HEL", "green"), (10.75, 59.91, "OSL", "blue")],
            (17.92, 59.803333),
            id="nordic_cluster",
        ),
    ],
)
def test_compute_extent_centers_on_mean(quiz, locations, expected_center):
    lon_c, lat_c, _, _ = quiz._compute_extent(locations)

    assert lon_c == pytest.approx(expected_center[0], abs=0.01)
    assert lat_c == pytest.approx(expected_center[1], abs=0.01)


def test_compute_extent_half_includes_padding(quiz):
    locations = [(-10.0, 30.0, "A", "red"), (10.0, 50.0, "B", "green")]

    _, _, lon_half, lat_half = quiz._compute_extent(locations)

    # Spread is 20 deg for both lon and lat.
    # Dynamic pad = max(10.0, min(15.0, 20 * 0.4)) = 10.0
    assert lon_half == pytest.approx(10.0 + 10.0)
    assert lat_half == pytest.approx(10.0 + 10.0)


@pytest.mark.parametrize(
    "locations",
    [
        pytest.param([(-74.0, 40.7, "NYC", "red"), (139.65, 35.68, "TKY", "green")], id="nyc_tokyo_213deg_spread"),
        pytest.param([(-43.17, -22.91, "RIO", "red"), (121.47, 31.23, "SHA", "green")], id="rio_shanghai_164deg_spread"),
    ],
)
def test_compute_extent_enforces_min_lat_ratio(quiz, locations):
    """When lon spread >> lat spread, lat_half is boosted to avoid ultra-wide maps."""
    _, _, lon_half, lat_half = quiz._compute_extent(locations)

    assert lat_half >= lon_half * _MIN_LAT_LON_RATIO


def test_compute_extent_single_location_uses_padding_only(quiz):
    locations = [(25.0, 60.0, "A", "red")]

    _, _, lon_half, lat_half = quiz._compute_extent(locations)

    # Spread is 0. Dynamic pad = max(10.0, min(15.0, 0)) = 10.0
    assert lon_half == pytest.approx(10.0)
    assert lat_half == pytest.approx(10.0)


# ── generate_image (fast, mocked IO) ────────────────────────────────


@pytest.mark.parametrize(
    "locations",
    [
        pytest.param([(-7.99, 31.62, "1963", "red"), (24.94, 60.17, "1917", "green")], id="europe"),
        pytest.param([(-74.0, 40.7, "NYC", "red"), (139.65, 35.68, "TKY", "green")], id="cross_continent"),
        pytest.param([(0.0, 0.0, "X", "blue")], id="single_equator"),
    ],
)
@patch("src.models.map_quiz.core_utils")
def test_generate_image_produces_valid_file(mock_utils, locations):
    mock_utils.get_random_id.return_value = "test-gen"
    quiz = MapQuiz()

    path = quiz.generate_image(locations)

    assert path.endswith(".jpg")
    assert os.path.isfile(path)
    assert os.path.getsize(path) > 0
    os.remove(path)


# ── visual inspection (run manually: pytest -m visual) ──────────────
# python -m pytest test/models/test_map_quiz.py -m visual -v

VISUAL_CASES = [
    pytest.param([(-7.99, 31.62, "1963", "red"), (24.94, 60.17, "1917", "green")], "europe", id="europe"),
    pytest.param([(-74.0, 40.7, "1934", "red"), (139.65, 35.68, "1917", "green")], "nyc_tokyo", id="nyc_tokyo"),
    pytest.param(
        [(18.07, 59.33, "1905", "red"), (24.94, 60.17, "1917", "green"), (10.75, 59.91, "1814", "blue")],
        "nordic_cluster",
        id="nordic_cluster",
    ),
    pytest.param([(-43.17, -22.91, "1822", "red"), (37.62, 55.76, "1917", "green")], "rio_moscow", id="southern_to_northern"),
    pytest.param(
        [(116.39, 39.90, "1949", "red"), (28.98, 41.01, "1923", "green"), (-99.13, 19.43, "1821", "blue")],
        "beijing_istanbul_mexico",
        id="beijing_istanbul_mexico",
    ),
    pytest.param([(-118.24, 34.05, "1850", "red"), (151.20, -33.86, "1901", "green")], "la_sydney", id="la_sydney"),
    pytest.param([(18.42, -33.92, "1910", "red"), (31.23, 30.04, "1922", "green")], "cape_town_cairo", id="cape_town_cairo"),
    pytest.param(
        [(-0.12, 51.50, "1066", "red"), (2.35, 48.85, "1789", "green"), (12.49, 41.89, "1861", "blue")],
        "london_paris_rome",
        id="london_paris_rome",
    ),
]


@pytest.mark.visual
@pytest.mark.parametrize("locations, filename", VISUAL_CASES)
def test_generate_image_visual(locations, filename):
    """Generates images for manual inspection. Run with: pytest -m visual"""
    quiz = MapQuiz()

    path = quiz.generate_image(locations)

    dest = os.path.join(os.path.dirname(path), f"{filename}.jpg")
    if os.path.exists(dest):
        os.remove(dest)
    os.replace(path, dest)

    print(f"\n  📍 Saved: {dest}")
    assert os.path.exists(dest)


# ── is_answer_correct ──────────────────────────────────────────────────


@pytest.mark.parametrize(
    "user_answer, valid_answers, expected",
    [
        # Exact and partial word subsets
        pytest.param("henryk viii", {"henryk viii tudor", "henryk", "viii", "tudor"}, True, id="exact_words_subset"),
        pytest.param("katarzyna wielka", {"katarzyna ii wielka", "katarzyna", "ii", "wielka"}, True, id="partial_multi_word"),
        pytest.param("tudor", {"henryk viii tudor", "henryk", "viii", "tudor"}, True, id="last_name_only"),
        # Fuzzy matching (typos and diacritics)
        pytest.param("katarzyna wielk", {"katarzyna ii wielka", "katarzyna", "ii", "wielka"}, True, id="fuzzy_match_typo"),
        pytest.param("jozef", {"józef piłsudski", "józef", "piłsudski"}, True, id="fuzzy_diacritic_jozef"),
        pytest.param("wladyslaw", {"władysław łokietek", "władysław", "łokietek"}, True, id="fuzzy_diacritic_wladyslaw"),
        # Roman numerals protection (should NOT fuzzy match)
        pytest.param("henryk vi", {"henryk viii tudor", "henryk", "viii", "tudor"}, False, id="roman_numeral_vi_viii_rejected"),
        pytest.param("vii", {"viii"}, False, id="roman_numeral_vii_viii_rejected"),
        pytest.param("viii", {"viii"}, True, id="roman_numeral_exact_match"),
        # Fail cases
        pytest.param("henryk smith", {"henryk viii tudor", "henryk", "viii", "tudor"}, False, id="one_correct_one_wrong_word"),
        pytest.param("og", {"óg"}, False, id="short_word_rejected_due_to_length"),
        pytest.param("random", {"henryk viii tudor", "henryk", "viii", "tudor"}, False, id="completely_wrong"),
    ],
)
def test_is_answer_correct(user_answer, valid_answers, expected):
    assert MapQuiz.is_answer_correct(user_answer, valid_answers) == expected
