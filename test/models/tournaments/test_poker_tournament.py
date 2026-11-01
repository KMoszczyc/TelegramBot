import random
from unittest.mock import MagicMock

import pytest

from src.config.constants import MAX_POKER_PLAYERS
from src.config.enums import PokerAction, PokerStreet, TournamentState, TournamentType
from src.models.games.poker_hand import Card
from src.models.tournaments.poker_tournament import PokerTournament

HOST_ID = 101
USER_2 = 102
USER_3 = 103
USER_4 = 104
BUY_IN = 100


@pytest.fixture
def credits_mock():
    mock = MagicMock()
    mock.credits = {i: 10_000 for i in range(100, 120)}
    return mock


@pytest.fixture
def poker_2p(credits_mock):
    t = PokerTournament(
        chat_id=1,
        thread_id=10,
        host_user_id=HOST_ID,
        host_username="Alice",
        credits_obj=credits_mock,
        buy_in=BUY_IN,
        max_rounds=5,
        rng=random.Random(123),
    )
    t.add_player(USER_2, "Bob")
    return t


@pytest.fixture
def poker_3p(credits_mock):
    t = PokerTournament(
        chat_id=1,
        thread_id=10,
        host_user_id=HOST_ID,
        host_username="Alice",
        credits_obj=credits_mock,
        buy_in=BUY_IN,
        max_rounds=5,
        rng=random.Random(456),
    )
    t.add_player(USER_2, "Bob")
    t.add_player(USER_3, "Charlie")
    return t


def test_poker_tournament_type_and_max_players(credits_mock):
    t = PokerTournament(1, 10, HOST_ID, "Host", credits_mock, BUY_IN, 5)
    assert t.tournament_type == TournamentType.POKER
    for idx in range(2, MAX_POKER_PLAYERS + 1):
        msg, ok = t.add_player(100 + idx, f"P{idx}")
        assert ok is True
    msg, ok = t.add_player(115, "OverflowPlayer")
    assert ok is False
    assert "Table is full" in msg


@pytest.mark.parametrize(
    "round_num,expected_sb,expected_bb,expected_ante",
    [
        (1, 5, 10, 2),
        (2, 5, 10, 2),
        (3, 8, 16, 3),
        (4, 8, 16, 3),
        (5, 11, 22, 4),
    ],
)
def test_blinds_and_ante_escalation(poker_2p, round_num, expected_sb, expected_bb, expected_ante):
    sb, bb, ante = poker_2p.get_blinds_and_ante(round_num)
    assert (sb, bb, ante) == (expected_sb, expected_bb, expected_ante)


def test_heads_up_blind_posting_and_turn_order(poker_2p):
    poker_2p.start_betting_round()
    assert poker_2p.state == TournamentState.BETTING
    assert poker_2p.street == PokerStreet.PRE_FLOP
    assert poker_2p.dealer_user_id == HOST_ID
    assert poker_2p.sb_user_id == HOST_ID
    assert poker_2p.bb_user_id == USER_2
    assert poker_2p.current_turn_user_id == HOST_ID
    assert poker_2p.pot == 2 + 2 + 5 + 10
    assert len(poker_2p.players[HOST_ID].hole_cards) == 2
    assert len(poker_2p.players[USER_2].hole_cards) == 2


def test_three_player_blind_posting_and_utg_first(poker_3p):
    poker_3p.start_betting_round()
    assert poker_3p.dealer_user_id == HOST_ID
    assert poker_3p.sb_user_id == USER_2
    assert poker_3p.bb_user_id == USER_3
    assert poker_3p.current_turn_user_id == HOST_ID


def test_fold_win_and_flex_reveal(poker_2p):
    poker_2p.start_betting_round()
    res = poker_2p.process_action(HOST_ID, PokerAction.FOLD)
    assert res.valid is True
    assert res.hand_ended is True
    assert res.won_by_fold is True
    assert poker_2p.state == TournamentState.ROUND_RESULT
    assert poker_2p.won_by_fold_user_id == USER_2
    assert poker_2p.players[USER_2].tournament_credits == BUY_IN + 7

    wrong_msg, ok = poker_2p.reveal_winner_cards(HOST_ID)
    assert ok is False

    flex_msg, ok = poker_2p.reveal_winner_cards(USER_2)
    assert ok is True
    assert poker_2p.players[USER_2].revealed_cards is True
    assert "flexed their cards" in poker_2p.last_hand_summary


def test_full_hand_check_down_to_showdown(poker_2p):
    poker_2p.start_betting_round()
    res1 = poker_2p.process_action(HOST_ID, PokerAction.CALL)
    assert res1.valid is True
    assert res1.street_changed is False
    assert poker_2p.current_turn_user_id == USER_2

    res2 = poker_2p.process_action(USER_2, PokerAction.CHECK)
    assert res2.valid is True
    assert res2.street_changed is True
    assert poker_2p.street == PokerStreet.FLOP
    assert len(poker_2p.community_cards) == 3
    assert poker_2p.current_turn_user_id == USER_2

    poker_2p.process_action(USER_2, PokerAction.CHECK)
    res_turn = poker_2p.process_action(HOST_ID, PokerAction.CHECK)
    assert res_turn.street_changed is True
    assert poker_2p.street == PokerStreet.TURN
    assert len(poker_2p.community_cards) == 4

    poker_2p.process_action(USER_2, PokerAction.CHECK)
    res_river = poker_2p.process_action(HOST_ID, PokerAction.CHECK)
    assert res_river.street_changed is True
    assert poker_2p.street == PokerStreet.RIVER
    assert len(poker_2p.community_cards) == 5

    poker_2p.process_action(USER_2, PokerAction.CHECK)
    res_showdown = poker_2p.process_action(HOST_ID, PokerAction.CHECK)
    assert res_showdown.hand_ended is True
    assert poker_2p.state == TournamentState.ROUND_RESULT
    assert poker_2p.street == PokerStreet.SHOWDOWN
    assert poker_2p.players[HOST_ID].tournament_credits + poker_2p.players[USER_2].tournament_credits == 200


def test_raise_reopens_action_and_all_in_runout(poker_2p):
    poker_2p.start_betting_round()
    res_raise = poker_2p.process_action(HOST_ID, PokerAction.RAISE, raise_to_amount=30)
    assert res_raise.valid is True
    assert poker_2p.current_street_bet == 30
    assert poker_2p.current_turn_user_id == USER_2

    res_all_in = poker_2p.process_action(USER_2, PokerAction.ALL_IN)
    assert res_all_in.valid is True
    assert poker_2p.current_turn_user_id == HOST_ID

    res_call_all_in = poker_2p.process_action(HOST_ID, PokerAction.CALL)
    assert res_call_all_in.valid is True
    assert res_call_all_in.hand_ended is True
    assert poker_2p.street == PokerStreet.SHOWDOWN
    assert len(poker_2p.community_cards) == 5


def test_side_pots_short_stack_all_in(poker_3p):
    poker_3p.players[HOST_ID].tournament_credits = 30
    poker_3p.players[USER_2].tournament_credits = 100
    poker_3p.players[USER_3].tournament_credits = 100

    poker_3p.start_betting_round()
    poker_3p.community_cards = [
        Card(2, "♠"),
        Card(3, "♥"),
        Card(4, "♦"),
        Card(8, "♣"),
        Card(9, "♠"),
    ]
    poker_3p.players[HOST_ID].hole_cards = [Card(14, "♠"), Card(14, "♥")]
    poker_3p.players[USER_2].hole_cards = [Card(13, "♠"), Card(13, "♥")]
    poker_3p.players[USER_3].hole_cards = [Card(7, "♠"), Card(2, "♣")]

    poker_3p.process_action(HOST_ID, PokerAction.ALL_IN)
    poker_3p.process_action(USER_2, PokerAction.RAISE, raise_to_amount=80)
    poker_3p.process_action(USER_3, PokerAction.CALL)

    assert poker_3p.street == PokerStreet.FLOP
    poker_3p.community_cards = [Card(2, "♠"), Card(3, "♥"), Card(4, "♦")]
    poker_3p.deck = [Card(9, "♠"), Card(8, "♣")]
    poker_3p.process_action(USER_2, PokerAction.CHECK)
    poker_3p.process_action(USER_3, PokerAction.CHECK)
    poker_3p.process_action(USER_2, PokerAction.CHECK)
    poker_3p.process_action(USER_3, PokerAction.CHECK)
    poker_3p.process_action(USER_2, PokerAction.CHECK)
    res = poker_3p.process_action(USER_3, PokerAction.CHECK)

    assert res.hand_ended is True
    assert poker_3p.players[HOST_ID].tournament_credits == 90
    assert poker_3p.players[USER_2].tournament_credits == 122
    assert poker_3p.players[USER_3].tournament_credits == 18


def test_turn_timeout_and_afk_mode(poker_2p):
    poker_2p.start_betting_round()
    assert poker_2p.current_turn_user_id == HOST_ID

    res1 = poker_2p.handle_turn_timeout()
    assert res1.valid is True
    assert res1.won_by_fold is True
    assert poker_2p.players[HOST_ID].consecutive_timeouts == 1
    assert poker_2p.players[HOST_ID].is_afk is False

    poker_2p.start_betting_round()
    assert poker_2p.current_turn_user_id == USER_2
    poker_2p.process_action(USER_2, PokerAction.CALL)
    assert poker_2p.current_turn_user_id == HOST_ID

    res2 = poker_2p.handle_turn_timeout()
    assert res2.valid is True
    assert poker_2p.players[HOST_ID].consecutive_timeouts == 2
    assert poker_2p.players[HOST_ID].is_afk is True

    msg, ok = poker_2p.sit_back_in(HOST_ID)
    assert ok is True
    assert poker_2p.players[HOST_ID].is_afk is False
    assert poker_2p.players[HOST_ID].consecutive_timeouts == 0


@pytest.mark.parametrize(
    "rank,total_players,expected_mult",
    [
        (0, 2, 1.5),
        (1, 2, 1.0),
        (0, 4, 1.5),
        (1, 4, 1.2),
        (2, 4, 1.0),
        (3, 4, 1.0),
    ],
)
def test_casino_bonus_multipliers(poker_2p, rank, total_players, expected_mult):
    assert poker_2p._get_multiplier(rank, total_players) == expected_mult


def test_early_finish_when_one_player_has_all_chips(poker_2p):
    poker_2p.start_betting_round()
    poker_2p.players[HOST_ID].tournament_credits = 0
    poker_2p.players[USER_2].tournament_credits = 200
    assert poker_2p.is_last_round() is True

    final_text = poker_2p.get_final_results()
    assert poker_2p.state == TournamentState.FINISHED
    assert "Final Results" in final_text
    assert poker_2p.credits.credits[USER_2] == 10_000 - BUY_IN + 300


def test_player_cards_alert_and_keyboards(poker_2p):
    lobby_kb = poker_2p.build_lobby_keyboard()
    assert len(lobby_kb.inline_keyboard) == 2

    caption = poker_2p.start_betting_round()
    assert "_" not in caption
    alert = poker_2p.get_player_cards_alert(HOST_ID)
    assert "Your Hole Cards" in alert
    assert "Best Hand:" in alert

    table_kb = poker_2p.build_table_keyboard()
    assert len(table_kb.inline_keyboard) >= 2


def test_solo_vs_bot_gameplay_and_payouts(credits_mock):
    t = PokerTournament(1, 10, HOST_ID, "Alice", credits_mock, BUY_IN, 5, rng=random.Random(77))
    msg, ok = t.add_bot_player()
    assert ok is True
    assert "🤖 Ozjasz" in msg
    assert -1 in t.players
    assert t.players[-1].is_bot is True
    assert t.get_human_player_count() == 1

    t.start_betting_round()
    safety = 0
    while t.state == TournamentState.BETTING and safety < 10:
        safety += 1
        assert t.current_turn_user_id == HOST_ID
        to_call = t.get_to_call(HOST_ID)
        action = PokerAction.CALL if to_call > 0 else PokerAction.CHECK
        t.process_action(HOST_ID, action)

    assert t.state == TournamentState.ROUND_RESULT
    assert t._get_multiplier(0, 2) == 1.0

    t.players[HOST_ID].tournament_credits = 200
    t.players[-1].tournament_credits = 0
    assert t.is_last_round() is True
    t.finish()
    assert credits_mock.credits[HOST_ID] == 10_000 - BUY_IN + 200
    assert -1 not in credits_mock.credits


def test_is_last_round_when_all_humans_busted(credits_mock):
    t = PokerTournament(1, 10, HOST_ID, "Alice", credits_mock, BUY_IN, 5, rng=random.Random(11))
    t.add_bot_player()
    t.add_bot_player()
    t.start_betting_round()
    t.players[HOST_ID].tournament_credits = 0
    assert t.get_active_human_player_count() == 0
    assert t.is_last_round() is True


def test_step_all_in_runout_televised(credits_mock):
    t = PokerTournament(1, 10, HOST_ID, "Alice", credits_mock, BUY_IN, 5, rng=random.Random(33), auto_runout=False)
    t.add_player(USER_2, "Bob")
    t.start_betting_round()

    t.process_action(HOST_ID, PokerAction.ALL_IN)
    res = t.process_action(USER_2, PokerAction.CALL)

    assert res.valid is True
    assert t.is_all_in_runout is True
    assert t.street == PokerStreet.PRE_FLOP
    assert t.players[HOST_ID].revealed_cards is True
    assert t.players[USER_2].revealed_cards is True

    assert t.step_all_in_runout() is False
    assert t.street == PokerStreet.FLOP
    assert len(t.community_cards) == 3

    assert t.step_all_in_runout() is False
    assert t.street == PokerStreet.TURN
    assert len(t.community_cards) == 4

    assert t.step_all_in_runout() is False
    assert t.street == PokerStreet.RIVER
    assert len(t.community_cards) == 5

    assert t.step_all_in_runout() is True
    assert t.street == PokerStreet.SHOWDOWN
    assert t.state == TournamentState.ROUND_RESULT


def test_persistent_action_log_across_streets(poker_2p):
    poker_2p.start_betting_round()
    first = poker_2p.current_turn_user_id
    second = USER_2 if first == HOST_ID else HOST_ID

    poker_2p.process_action(first, PokerAction.CALL)
    poker_2p.process_action(second, PokerAction.CHECK)

    assert poker_2p.street == PokerStreet.FLOP
    assert any("calls" in line for line in poker_2p.street_action_log)
    assert any("checks" in line for line in poker_2p.street_action_log)
    assert any("🃏 *Flop:*" in line for line in poker_2p.street_action_log)


@pytest.mark.parametrize(
    "premove_action,expected_label_prefix",
    [
        (PokerAction.CALL, "Call"),
        (PokerAction.FOLD, "Fold"),
    ],
)
def test_premove_set_toggle_and_execution(poker_2p, premove_action, expected_label_prefix):
    poker_2p.start_betting_round()
    first = poker_2p.current_turn_user_id
    second = USER_2 if first == HOST_ID else HOST_ID

    # Setting and toggling off on second click
    ok, msg = poker_2p.set_premove(second, premove_action)
    assert ok is True
    assert "Premove set" in msg
    ok2, msg2 = poker_2p.set_premove(second, premove_action)
    assert ok2 is False
    assert "cancelled" in msg2

    # Set premove on BB (second player), where to_call == 0 initially unless first raises
    poker_2p.set_premove(second, premove_action)
    if premove_action == PokerAction.FOLD:
        # First raises so second faces a bet and folds via Check/Fold premove
        res = poker_2p.process_action(first, PokerAction.RAISE, raise_to_amount=20)
        assert res.hand_ended is True
        assert poker_2p.players[second].folded is True
        assert poker_2p.players[second].last_action_label.startswith(expected_label_prefix)
    else:
        # First calls, second has CALL premove (normalized to CHECK since to_call == 0 for BB) -> auto-checks to Flop!
        poker_2p.process_action(first, PokerAction.CALL)
        assert poker_2p.street == PokerStreet.FLOP
        assert any("Premove" in line for line in poker_2p.street_action_log)


def test_premove_cancelled_when_opponent_raises(poker_3p):
    poker_3p.start_betting_round()
    utg = poker_3p.current_turn_user_id
    bb = poker_3p.bb_user_id

    # BB has to_call == 0 right now and sets a CHECK premove
    ok, _ = poker_3p.set_premove(bb, PokerAction.CHECK)
    assert ok is True
    assert poker_3p.players[bb].premove == PokerAction.CHECK

    # UTG raises -> BB's CHECK premove must be automatically cancelled!
    poker_3p.process_action(utg, PokerAction.RAISE, raise_to_amount=30)
    assert poker_3p.players[bb].premove is None


def test_step_auto_turn_with_auto_bot_turns_disabled(credits_mock):
    t = PokerTournament(
        1,
        10,
        HOST_ID,
        "Human",
        credits_mock,
        BUY_IN,
        5,
        rng=random.Random(99),
        auto_runout=False,
        auto_bot_turns=False,
    )
    t.add_bot_player()
    t.start_betting_round()

    if t.current_turn_user_id == HOST_ID:
        t.process_action(HOST_ID, PokerAction.CALL)

    assert t.current_turn_user_id == -1
    assert t.has_pending_auto_turn() is True

    # During bot turn, keyboard shows standard clean buttons (no special Premove buttons)
    kb = t.build_table_keyboard()
    btn_texts = [btn.text for row in kb.inline_keyboard for btn in row]
    assert not any("Premove" in txt for txt in btn_texts)
    assert any("Fold" in txt for txt in btn_texts)
    assert any("Check" in txt or "Call" in txt for txt in btn_texts)

    res = t.step_auto_turn()
    assert res is not None
    assert res.valid is True


def test_premove_check_does_not_go_through_if_facing_bet(poker_2p):
    poker_2p.start_betting_round()
    first = poker_2p.current_turn_user_id
    second = USER_2 if first == HOST_ID else HOST_ID

    ok, _ = poker_2p.set_premove(second, PokerAction.CHECK)
    assert ok is True

    # First player raises
    poker_2p.process_action(first, PokerAction.RAISE, raise_to_amount=25)

    # When it becomes second player's turn, check does not go through; player acts normally
    assert poker_2p.current_turn_user_id == second
    assert poker_2p.has_pending_auto_turn() is False
    assert poker_2p.players[second].premove is None
    assert poker_2p.state == TournamentState.BETTING
