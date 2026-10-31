import os
from functools import lru_cache
from pathlib import Path

from PIL import ImageFont

from src.config.paths import FONTS_DIR_PATH

_ROBOTO_PATH = FONTS_DIR_PATH / "roboto" / "Roboto-VariableFont_wdth,wght.ttf"

_BOLD_FONT_CANDIDATES: tuple[str | Path, ...] = (
    # Windows
    "C:/Windows/Fonts/segoeuib.ttf",
    "C:/Windows/Fonts/arialbd.ttf",
    "C:/Windows/Fonts/verdanab.ttf",
    # Linux system paths
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
    "/usr/share/fonts/truetype/freefont/FreeSansBold.ttf",
    # Bare names for fontconfig lookup
    "DejaVuSans-Bold.ttf",
    "LiberationSans-Bold.ttf",
    # Bundled Roboto fallback
    _ROBOTO_PATH,
    "/usr/share/fonts/truetype/roboto/Roboto-VariableFont_wdth,wght.ttf",
    # Regular fallbacks
    "C:/Windows/Fonts/segoeui.ttf",
    "C:/Windows/Fonts/arial.ttf",
    "DejaVuSans.ttf",
)

_REGULAR_FONT_CANDIDATES: tuple[str | Path, ...] = (
    # Windows
    "C:/Windows/Fonts/segoeui.ttf",
    "C:/Windows/Fonts/arial.ttf",
    "C:/Windows/Fonts/verdana.ttf",
    # Bundled Roboto fallback
    _ROBOTO_PATH,
    # Linux system paths
    "/usr/share/fonts/truetype/roboto/Roboto-VariableFont_wdth,wght.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
    "/usr/share/fonts/truetype/freefont/FreeSans.ttf",
    # Bare names for fontconfig lookup
    "DejaVuSans.ttf",
    "LiberationSans-Regular.ttf",
)


@lru_cache(maxsize=32)
def get_font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    """Load a TrueType font at the requested size with cross-platform fallback.

    Checks Windows system fonts, Linux system fonts, project-bundled fonts,
    and falls back to Pillow's scaled default font if no TTF file is found.

    Args:
        size: Desired font size in pixels/points.
        bold: Whether to prefer a bold typeface variant.

    Returns:
        Loaded FreeTypeFont, or fallback scaled ImageFont.
    """
    candidates = _BOLD_FONT_CANDIDATES if bold else _REGULAR_FONT_CANDIDATES
    for candidate in candidates:
        candidate_str = str(candidate)
        if ("/" in candidate_str or "\\" in candidate_str) and not os.path.exists(candidate_str):
            continue
        try:
            return ImageFont.truetype(candidate_str, size)
        except (OSError, Exception):
            continue

    try:
        return ImageFont.load_default(size=size)
    except Exception:
        return ImageFont.load_default()
