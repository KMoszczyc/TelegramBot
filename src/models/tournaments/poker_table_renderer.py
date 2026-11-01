from io import BytesIO

from PIL import Image, ImageDraw

from src.core.font_utils import get_font
from src.models.games.poker_hand import Card
from src.models.tournaments.poker_tournament import PokerPlayer, PokerTournament

CANVAS_W = 1400
CANVAS_H = 860
CENTER_X = CANVAS_W // 2
CENTER_Y = CANVAS_H // 2

POD_W = 320
POD_H = 140
LEFT_COL_MAX_W = 166

BG_COLOR = (14, 19, 26)
RAIL_OUTER_COLOR = (66, 37, 21)
RAIL_GOLD_TRIM = (220, 180, 58)
FELT_OUTER_COLOR = (15, 84, 52)
FELT_INNER_COLOR = (21, 114, 70)
BET_LINE_COLOR = (170, 205, 155)

CARD_WHITE = (252, 252, 254)
CARD_BORDER = (180, 186, 195)
CARD_SHADOW = (6, 10, 14)
CARD_BACK_FILL = (27, 56, 98)
CARD_BACK_INNER = (40, 78, 134)
CARD_BACK_GOLD = (220, 180, 58)

SUIT_RED = (216, 34, 34)
SUIT_BLACK = (22, 25, 32)

SEAT_BG_ACTIVE = (24, 35, 48)
SEAT_BG_TURN = (62, 48, 16)
SEAT_BG_WINNER = (16, 58, 42)
SEAT_BG_DIM = (20, 25, 32)

BORDER_TURN_GOLD = (255, 204, 21)
BORDER_TURN_HALO = (245, 158, 11)
BORDER_WINNER_GREEN = (52, 211, 153)
BORDER_WINNER_HALO = (16, 185, 129)
BORDER_ALL_IN_RED = (239, 68, 68)
BORDER_DEFAULT = (110, 128, 152)
BORDER_DIM = (58, 68, 84)

TEXT_WHITE = (248, 250, 252)
TEXT_MUTED = (148, 163, 184)
TEXT_GOLD = (253, 204, 21)
TEXT_GREEN = (74, 222, 128)
TEXT_RED = (248, 113, 113)


def _draw_fitted_text(
    draw: ImageDraw.ImageDraw,
    x: int,
    y: int,
    text: str,
    max_width: int,
    max_size: int,
    min_size: int,
    fill: tuple[int, int, int],
    bold: bool = True,
) -> None:
    """Draw text that automatically shrinks down to min_size and truncates so it never exceeds max_width."""
    chosen_font = get_font(min_size, bold=bold)
    for size in range(max_size, min_size - 1, -1):
        font = get_font(size, bold=bold)
        if draw.textlength(text, font=font) <= max_width:
            draw.text((x, y), text, fill=fill, font=font)
            return
        chosen_font = font

    clipped = text
    while len(clipped) > 1 and draw.textlength(clipped + "…", font=chosen_font) > max_width:
        clipped = clipped[:-1]
    draw.text((x, y), clipped + "…", fill=fill, font=chosen_font)


def _draw_suit_pip(draw: ImageDraw.ImageDraw, suit: str, cx: float, cy: float, radius: float, color: tuple[int, int, int]) -> None:
    """Draw a crisp vector suit icon (♠, ♥, ♦, ♣) centered at (cx, cy)."""
    r = radius
    if suit == "♦":
        pts = [(cx, cy - r), (cx + r * 0.80, cy), (cx, cy + r), (cx - r * 0.80, cy)]
        draw.polygon(pts, fill=color)
    elif suit == "♥":
        cr = r * 0.48
        lx, rx = cx - r * 0.38, cx + r * 0.38
        ty = cy - r * 0.25
        draw.ellipse((lx - cr, ty - cr, lx + cr, ty + cr), fill=color)
        draw.ellipse((rx - cr, ty - cr, rx + cr, ty + cr), fill=color)
        pts = [(cx - r * 0.83, cy - r * 0.08), (cx + r * 0.83, cy - r * 0.08), (cx, cy + r * 0.96)]
        draw.polygon(pts, fill=color)
    elif suit == "♠":
        cr = r * 0.44
        lx, rx = cx - r * 0.36, cx + r * 0.36
        by = cy + r * 0.12
        pts_top = [(cx, cy - r * 0.96), (cx - r * 0.83, cy + r * 0.05), (cx + r * 0.83, cy + r * 0.05)]
        draw.polygon(pts_top, fill=color)
        draw.ellipse((lx - cr, by - cr, lx + cr, by + cr), fill=color)
        draw.ellipse((rx - cr, by - cr, rx + cr, by + cr), fill=color)
        stem = [
            (cx - r * 0.13, cy + r * 0.2),
            (cx + r * 0.13, cy + r * 0.2),
            (cx + r * 0.30, cy + r * 0.96),
            (cx - r * 0.30, cy + r * 0.96),
        ]
        draw.polygon(stem, fill=color)
    else:
        cr = r * 0.43
        top_y = cy - r * 0.38
        bot_y = cy + r * 0.08
        lx, rx = cx - r * 0.40, cx + r * 0.40
        draw.ellipse((cx - cr, top_y - cr, cx + cr, top_y + cr), fill=color)
        draw.ellipse((lx - cr, bot_y - cr, lx + cr, bot_y + cr), fill=color)
        draw.ellipse((rx - cr, bot_y - cr, rx + cr, bot_y + cr), fill=color)
        draw.ellipse((cx - r * 0.25, cy - r * 0.2, cx + r * 0.25, cy + r * 0.3), fill=color)
        stem = [
            (cx - r * 0.13, cy + r * 0.2),
            (cx + r * 0.13, cy + r * 0.2),
            (cx + r * 0.30, cy + r * 0.96),
            (cx - r * 0.30, cy + r * 0.96),
        ]
        draw.polygon(stem, fill=color)


def _draw_face_up_card(draw: ImageDraw.ImageDraw, x: int, y: int, w: int, h: int, card: Card) -> None:
    """Draw a high-contrast face-up playing card with bold rank text and vector suit pips."""
    draw.rounded_rectangle((x + 3, y + 4, x + w + 3, y + h + 4), radius=10, fill=CARD_SHADOW)
    draw.rounded_rectangle((x, y, x + w, y + h), radius=10, fill=CARD_WHITE, outline=CARD_BORDER, width=2)

    color = SUIT_RED if card.is_red else SUIT_BLACK
    font_size = max(22, int(h * 0.29))
    rank_font = get_font(font_size, bold=True)
    draw.text((x + 6, y + 2), card.rank_str, fill=color, font=rank_font)

    small_pip_r = max(6.5, w * 0.12)
    _draw_suit_pip(draw, card.suit, x + 15, y + 6 + font_size + small_pip_r, small_pip_r, color)

    big_pip_r = max(12.0, w * 0.26)
    _draw_suit_pip(draw, card.suit, x + w * 0.60, y + h * 0.66, big_pip_r, color)


def _draw_face_down_card(draw: ImageDraw.ImageDraw, x: int, y: int, w: int, h: int, dimmed: bool = False) -> None:
    """Draw an ornate face-down card back."""
    fill_outer = (32, 40, 54) if dimmed else CARD_BACK_FILL
    fill_inner = (40, 50, 66) if dimmed else CARD_BACK_INNER
    border_col = (90, 100, 115) if dimmed else CARD_BACK_GOLD

    draw.rounded_rectangle((x + 2, y + 3, x + w + 2, y + h + 3), radius=9, fill=CARD_SHADOW)
    draw.rounded_rectangle((x, y, x + w, y + h), radius=9, fill=fill_outer, outline=CARD_WHITE, width=2)
    pad = max(5, w // 8)
    draw.rounded_rectangle((x + pad, y + pad, x + w - pad, y + h - pad), radius=5, fill=fill_inner, outline=border_col, width=2)


def _draw_table_felt(draw: ImageDraw.ImageDraw) -> None:
    """Draw the wide casino poker table rail, emerald felt, and inner betting line."""
    margin_x, margin_y = 60, 110
    draw.rounded_rectangle(
        (margin_x - 10, margin_y - 10, CANVAS_W - margin_x + 10, CANVAS_H - margin_y + 10),
        radius=260,
        fill=(6, 9, 13),
    )
    draw.rounded_rectangle(
        (margin_x, margin_y, CANVAS_W - margin_x, CANVAS_H - margin_y),
        radius=250,
        fill=RAIL_OUTER_COLOR,
        outline=RAIL_GOLD_TRIM,
        width=5,
    )
    felt_pad = 24
    draw.rounded_rectangle(
        (margin_x + felt_pad, margin_y + felt_pad, CANVAS_W - margin_x - felt_pad, CANVAS_H - margin_y - felt_pad),
        radius=230,
        fill=FELT_OUTER_COLOR,
        outline=RAIL_GOLD_TRIM,
        width=2,
    )
    inner_pad = 44
    draw.rounded_rectangle(
        (margin_x + inner_pad, margin_y + inner_pad, CANVAS_W - margin_x - inner_pad, CANVAS_H - margin_y - inner_pad),
        radius=210,
        fill=FELT_INNER_COLOR,
        outline=BET_LINE_COLOR,
        width=2,
    )


def _draw_center_board(draw: ImageDraw.ImageDraw, tournament: PokerTournament) -> None:
    """Draw the merged header pill above the 5 community cards and the POT pill below."""
    header_font = get_font(26, bold=True)
    pot_font = get_font(36, bold=True)

    round_num = max(1, tournament.round_number)
    sb, bb, ante = tournament.get_blinds_and_ante()
    ante_part = f"  •  ANTE {ante}" if ante > 0 else ""
    header_text = f"HAND {round_num}/{tournament.max_rounds}  •  {tournament.street.value.upper()}  •  BLINDS {sb}/{bb}{ante_part}"

    header_w = max(560, int(draw.textlength(header_text, font=header_font)) + 56)
    draw.rounded_rectangle(
        (CENTER_X - header_w // 2, 232, CENTER_X + header_w // 2, 286),
        radius=27,
        fill=(12, 52, 34),
        outline=RAIL_GOLD_TRIM,
        width=3,
    )
    draw.text((CENTER_X, 259), header_text, fill=TEXT_GOLD, font=header_font, anchor="mm")

    card_w, card_h = 114, 162
    gap = 18
    total_w = 5 * card_w + 4 * gap
    start_x = CENTER_X - total_w // 2
    cards_y = 308

    for i in range(5):
        cx = start_x + i * (card_w + gap)
        if i < len(tournament.community_cards):
            _draw_face_up_card(draw, cx, cards_y, card_w, card_h, tournament.community_cards[i])
        else:
            _draw_face_down_card(draw, cx, cards_y, card_w, card_h, dimmed=True)

    pot_text = f"POT: {tournament.pot}"
    pot_w = max(340, int(draw.textlength(pot_text, font=pot_font)) + 64)
    draw.rounded_rectangle(
        (CENTER_X - pot_w // 2, 492, CENTER_X + pot_w // 2, 554),
        radius=30,
        fill=(14, 42, 28),
        outline=RAIL_GOLD_TRIM,
        width=3,
    )
    draw.text((CENTER_X, 523), pot_text, fill=TEXT_WHITE, font=pot_font, anchor="mm")


def _get_row_x_centers(count: int) -> list[int]:
    """Return evenly spaced horizontal center X coordinates for 1 to 4 pods across CANVAS_W."""
    if count <= 0:
        return []
    if count == 1:
        return [CENTER_X]
    usable_w = CANVAS_W - 80
    step = usable_w // count
    start_x = 40 + step // 2
    return [start_x + i * step for i in range(count)]


def _get_seat_positions(num_seats: int) -> list[tuple[int, int]]:
    """Arrange 2 to 8 seats along the Top Rail and Bottom Rail (no middle-left/right seats).

    Top row flows left-to-right; bottom row flows right-to-left so clockwise poker turn order is preserved.
    """
    n = max(1, min(8, num_seats))
    top_count = (n + 1) // 2
    bot_count = n - top_count

    top_y = 38 + POD_H // 2
    bot_y = CANVAS_H - 28 - POD_H // 2

    top_xs = _get_row_x_centers(top_count)
    bot_xs = list(reversed(_get_row_x_centers(bot_count)))

    positions: list[tuple[int, int]] = [(x, top_y) for x in top_xs]
    positions.extend((x, bot_y) for x in bot_xs)
    return positions


def _draw_role_badge(draw: ImageDraw.ImageDraw, x: int, y: int, label: str, bg_color: tuple[int, int, int]) -> int:
    font = get_font(18, bold=True)
    w = 40 if len(label) > 1 else 32
    draw.rounded_rectangle((x, y, x + w, y + 28), radius=14, fill=bg_color, outline=(255, 255, 255), width=2)
    draw.text((x + w // 2, y + 14), label, fill=(15, 20, 28), font=font, anchor="mm")
    return w


def _draw_player_seat(
    draw: ImageDraw.ImageDraw,
    cx: int,
    cy: int,
    player: PokerPlayer,
    tournament: PokerTournament,
) -> None:
    """Draw a 320×140 side-by-side player pod with strict column separation and foreground hole cards."""
    box_w, box_h = POD_W, POD_H
    bx = max(12, min(CANVAS_W - box_w - 12, cx - box_w // 2))
    by = max(36, min(CANVAS_H - box_h - 12, cy - box_h // 2))

    is_turn = tournament.current_turn_user_id == player.user_id
    is_winner = player.user_id in tournament.last_hand_winners
    is_busted = player.tournament_credits == 0 and not player.in_current_hand
    is_dimmed = player.folded or is_busted

    if is_winner:
        border_col, border_w = BORDER_WINNER_GREEN, 4
        bg_col = SEAT_BG_WINNER
        draw.rounded_rectangle((bx - 6, by - 6, bx + box_w + 6, by + box_h + 6), radius=20, outline=BORDER_WINNER_HALO, width=5)
    elif is_turn:
        border_col, border_w = BORDER_TURN_GOLD, 5
        bg_col = SEAT_BG_TURN
        draw.rounded_rectangle((bx - 6, by - 6, bx + box_w + 6, by + box_h + 6), radius=20, outline=BORDER_TURN_HALO, width=6)
    elif player.all_in:
        border_col, border_w = BORDER_ALL_IN_RED, 3
        bg_col = SEAT_BG_ACTIVE
    elif is_dimmed:
        border_col, border_w = BORDER_DIM, 2
        bg_col = SEAT_BG_DIM
    else:
        border_col, border_w = BORDER_DEFAULT, 3
        bg_col = SEAT_BG_ACTIVE

    draw.rounded_rectangle((bx + 4, by + 5, bx + box_w + 4, by + box_h + 5), radius=16, fill=CARD_SHADOW)
    draw.rounded_rectangle((bx, by, bx + box_w, by + box_h), radius=16, fill=bg_col, outline=border_col, width=border_w)

    divider_x = bx + 184
    draw.line((divider_x, by + 10, divider_x, by + box_h - 10), fill=border_col, width=1)

    if is_turn or is_winner:
        banner_w, banner_h = 220, 32
        banner_x = bx + (box_w - banner_w) // 2
        banner_y = max(2, by - banner_h + 2)
        banner_fill = BORDER_TURN_GOLD if is_turn else BORDER_WINNER_GREEN
        banner_label = "▶ YOUR TURN ◀" if is_turn else "★ WINNER ★"
        banner_font = get_font(20, bold=True)
        draw.rounded_rectangle(
            (banner_x, banner_y, banner_x + banner_w, banner_y + banner_h),
            radius=16,
            fill=banner_fill,
            outline=(255, 255, 255),
            width=2,
        )
        draw.text((banner_x + banner_w // 2, banner_y + banner_h // 2), banner_label, fill=(14, 18, 24), font=banner_font, anchor="mm")

    name_col = TEXT_MUTED if is_dimmed else TEXT_WHITE
    _draw_fitted_text(draw, bx + 12, by + 14, player.username, LEFT_COL_MAX_W, max_size=26, min_size=16, fill=name_col)

    if is_busted:
        _draw_fitted_text(draw, bx + 12, by + 54, "BUSTED (0)", LEFT_COL_MAX_W, max_size=24, min_size=16, fill=TEXT_RED)
    else:
        stack_col = TEXT_GOLD if not is_dimmed else TEXT_MUTED
        _draw_fitted_text(
            draw,
            bx + 12,
            by + 54,
            f"Chips: {player.tournament_credits}",
            LEFT_COL_MAX_W,
            max_size=24,
            min_size=15,
            fill=stack_col,
        )

    status_text = ""
    status_col = TEXT_MUTED
    if is_winner and (player.best_hand_desc or player.last_action_label):
        status_text = player.best_hand_desc or player.last_action_label
        status_col = TEXT_GREEN
    elif player.revealed_cards and player.best_hand_desc:
        status_text = player.best_hand_desc
        status_col = TEXT_WHITE
    elif is_turn:
        status_text = f"Bet: {player.current_bet}" if player.current_bet > 0 else "▶ ACTING NOW"
        status_col = TEXT_GOLD
    elif player.all_in:
        status_text = player.last_action_label or "ALL-IN"
        status_col = TEXT_RED
    elif player.folded:
        status_text = player.last_action_label or "FOLDED"
        status_col = TEXT_MUTED
    elif player.last_action_label:
        status_text = player.last_action_label
        status_col = TEXT_GREEN
    elif player.is_afk:
        status_text = "AFK (Auto)"
        status_col = TEXT_RED

    if status_text:
        _draw_fitted_text(draw, bx + 12, by + 94, status_text, LEFT_COL_MAX_W, max_size=22, min_size=14, fill=status_col)

    badges: list[tuple[str, tuple[int, int, int]]] = []
    if tournament.dealer_user_id == player.user_id:
        badges.append(("D", (251, 191, 36)))
    if tournament.sb_user_id == player.user_id:
        badges.append(("SB", (56, 189, 248)))
    if tournament.bb_user_id == player.user_id:
        badges.append(("BB", (251, 146, 60)))

    badge_x = bx + box_w - 10
    for label, b_col in reversed(badges):
        bw = 40 if len(label) > 1 else 32
        badge_x -= bw
        _draw_role_badge(draw, badge_x, by + 8, label, b_col)
        badge_x -= 4

    if player.in_current_hand and player.hole_cards:
        hc_w, hc_h = 56, 84
        hc_x1 = bx + 192
        hc_x2 = hc_x1 + hc_w + 6
        hc_y = by + box_h - hc_h - 8

        if player.revealed_cards:
            _draw_face_up_card(draw, hc_x1, hc_y, hc_w, hc_h, player.hole_cards[0])
            _draw_face_up_card(draw, hc_x2, hc_y, hc_w, hc_h, player.hole_cards[1])
        else:
            _draw_face_down_card(draw, hc_x1, hc_y, hc_w, hc_h, dimmed=player.folded)
            _draw_face_down_card(draw, hc_x2, hc_y, hc_w, hc_h, dimmed=player.folded)


def render_poker_table(tournament: PokerTournament) -> bytes:
    """Render the current state of the PokerTournament table as PNG bytes."""
    img = Image.new("RGB", (CANVAS_W, CANVAS_H), color=BG_COLOR)
    draw = ImageDraw.Draw(img)

    _draw_table_felt(draw)
    _draw_center_board(draw, tournament)

    seat_uids = tournament.seat_order if tournament.seat_order else list(tournament.players.keys())
    positions = _get_seat_positions(len(seat_uids))
    for uid, (sx, sy) in zip(seat_uids, positions, strict=False):
        _draw_player_seat(draw, sx, sy, tournament.players[uid], tournament)

    buf = BytesIO()
    img.save(buf, format="PNG", optimize=True)
    return buf.getvalue()
