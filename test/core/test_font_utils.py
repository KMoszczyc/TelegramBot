from unittest.mock import patch

import pytest

from src.core.font_utils import get_font


@pytest.mark.parametrize(
    "size,bold",
    [
        (16, False),
        (24, True),
        (36, False),
        (48, True),
        (72, False),
    ],
)
def test_get_font_returns_usable_font(size: int, bold: bool) -> None:
    font = get_font(size, bold=bold)
    assert font is not None
    bbox = font.getbbox("Sample Text 123")
    # Verify bbox is valid and scales with requested size
    width = bbox[2] - bbox[0]
    height = bbox[3] - bbox[1]
    assert width > 0
    assert height > 0
    # A 48px or 72px font should have height significantly larger than 10px
    if size >= 48:
        assert height > 20


@pytest.mark.parametrize("size", [20, 50])
def test_get_font_scales_with_size(size: int) -> None:
    f_small = get_font(12, bold=False)
    f_large = get_font(size, bold=False)

    bbox_small = f_small.getbbox("City ?")
    bbox_large = f_large.getbbox("City ?")

    height_small = bbox_small[3] - bbox_small[1]
    height_large = bbox_large[3] - bbox_large[1]

    assert height_large > height_small


def test_get_font_fallback_when_truetype_fails() -> None:
    get_font.cache_clear()
    with patch("src.core.font_utils._REGULAR_FONT_CANDIDATES", ("/invalid/path/nonexistent.ttf",)):
        fallback_font = get_font(40, bold=False)
        assert fallback_font is not None
        bbox = fallback_font.getbbox("City")
        assert bbox[2] - bbox[0] > 0
    get_font.cache_clear()
