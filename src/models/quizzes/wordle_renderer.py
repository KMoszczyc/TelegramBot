import io

from PIL import Image, ImageDraw

from src.core.font_utils import get_font
from src.models.quizzes.wordle_game import LetterState, WordleGame

COLOR_BG = "#121213"
COLOR_EMPTY_BORDER = "#3A3A3C"
COLOR_ABSENT = "#3A3A3C"
COLOR_PRESENT = "#B59F3B"
COLOR_CORRECT = "#538D4E"
COLOR_KEY_DEFAULT = "#818384"
COLOR_TEXT = "#FFFFFF"

STATE_COLORS = {
    LetterState.CORRECT: COLOR_CORRECT,
    LetterState.PRESENT: COLOR_PRESENT,
    LetterState.ABSENT: COLOR_ABSENT,
}

KEYBOARD_ROWS: list[list[str]] = [
    ["Q", "W", "E", "R", "T", "Y", "U", "I", "O", "P"],
    ["A", "S", "D", "F", "G", "H", "J", "K", "L"],
    ["Z", "X", "C", "V", "B", "N", "M"],
    ["Ą", "Ć", "Ę", "Ł", "Ń", "Ó", "Ś", "Ź", "Ż"],
]


def render_wordle_board(game: WordleGame) -> io.BytesIO:
    """Render the Wordle game board and letter tracking keyboard as a sleek PNG image."""
    cols = game.word_length
    rows = game.max_tries
    tile_size = 64
    gap = 8
    pad_x = 24
    pad_y = 24
    radius = 5

    grid_width = cols * tile_size + (cols - 1) * gap
    grid_height = rows * tile_size + (rows - 1) * gap

    key_w = 34
    key_h = 42
    key_gap = 4
    key_row_gap = 6
    max_kb_row_len = max(len(r) for r in KEYBOARD_ROWS)
    kb_width = max_kb_row_len * key_w + (max_kb_row_len - 1) * key_gap
    kb_height = len(KEYBOARD_ROWS) * key_h + (len(KEYBOARD_ROWS) - 1) * key_row_gap
    section_gap = 24

    content_width = max(grid_width, kb_width)
    width = pad_x * 2 + content_width
    height = pad_y * 2 + grid_height + section_gap + kb_height

    img = Image.new("RGB", (width, height), COLOR_BG)
    draw = ImageDraw.Draw(img)
    tile_font = get_font(size=int(tile_size * 0.52), bold=True)
    key_font = get_font(size=18, bold=True)

    grid_start_x = pad_x + (content_width - grid_width) // 2
    grid_start_y = pad_y

    for r in range(rows):
        y1 = grid_start_y + r * (tile_size + gap)
        y2 = y1 + tile_size
        center_y = y1 + tile_size / 2

        is_guessed_row = r < len(game.guesses)
        guess_str = game.guesses[r] if is_guessed_row else ""
        eval_row = game.evaluations[r] if is_guessed_row else []

        for c in range(cols):
            x1 = grid_start_x + c * (tile_size + gap)
            x2 = x1 + tile_size
            center_x = x1 + tile_size / 2

            if is_guessed_row and c < len(guess_str):
                state = eval_row[c]
                color = STATE_COLORS.get(state, COLOR_ABSENT)
                draw.rounded_rectangle([x1, y1, x2, y2], radius=radius, fill=color, outline=color)
                draw.text(
                    (center_x, center_y),
                    guess_str[c].upper(),
                    fill=COLOR_TEXT,
                    font=tile_font,
                    anchor="mm",
                )
            else:
                draw.rounded_rectangle(
                    [x1, y1, x2, y2],
                    radius=radius,
                    fill=COLOR_BG,
                    outline=COLOR_EMPTY_BORDER,
                    width=2,
                )

    # Render keyboard at the bottom
    kb_start_y = grid_start_y + grid_height + section_gap
    kb_states = game.get_keyboard_states()

    for row_idx, row_keys in enumerate(KEYBOARD_ROWS):
        row_w = len(row_keys) * key_w + (len(row_keys) - 1) * key_gap
        row_start_x = pad_x + (content_width - row_w) // 2
        ky1 = kb_start_y + row_idx * (key_h + key_row_gap)
        ky2 = ky1 + key_h
        k_center_y = ky1 + key_h / 2

        for k_idx, key_char in enumerate(row_keys):
            kx1 = row_start_x + k_idx * (key_w + key_gap)
            kx2 = kx1 + key_w
            k_center_x = kx1 + key_w / 2

            st = kb_states.get(key_char)
            if st == LetterState.CORRECT:
                k_fill = COLOR_CORRECT
            elif st == LetterState.PRESENT:
                k_fill = COLOR_PRESENT
            elif st == LetterState.ABSENT:
                k_fill = COLOR_ABSENT
            else:
                k_fill = COLOR_KEY_DEFAULT

            draw.rounded_rectangle([kx1, ky1, kx2, ky2], radius=4, fill=k_fill)
            draw.text((k_center_x, k_center_y), key_char, fill=COLOR_TEXT, font=key_font, anchor="mm")

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return buf
