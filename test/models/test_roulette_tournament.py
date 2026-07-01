from unittest.mock import MagicMock

import pytest

from src.config.enums import RouletteBetType, TournamentState, TournamentType
from src.models.base_tournament import BaseTournament
from src.models.bot_state import BotState
from src.models.roulette_tournament import RouletteTournament

USER_1 = 100
USER_2 = 200
USER_3 = 300
USER_4 = 400
DEFAULT_BALANCE = 10000
BUY_IN = 1000
MAX_ROUNDS = 10


@pytest.fixture()
def credits_mock():
    mock = MagicMock()
    mock.credits = {
        USER_1: DEFAULT_BALANCE,
        USER_2: DEFAULT_BALANCE,
        USER_3: DEFAULT_BALANCE,
        USER_4: DEFAULT_BALANCE,
    }
    mock.update_credit_history = MagicMock()
    return mock


@pytest.fixture()
def tournament(credits_mock):
    return RouletteTournament(
        chat_id=1,
        thread_id=1,
        host_user_id=USER_1,
        host_username="Player1",
        credits_obj=credits_mock,
        buy_in=BUY_IN,
        max_rounds=MAX_ROUNDS,
    )


@pytest.fixture()
def tournament_with_players(credits_mock):
    t = RouletteTournament(
        chat_id=1,
        thread_id=1,
        host_user_id=USER_1,
        host_username="Player1",
        credits_obj=credits_mock,
        buy_in=BUY_IN,
        max_rounds=MAX_ROUNDS,
    )
    t.add_player(USER_2, "Player2")
    return t


@pytest.fixture()
def tournament_3_players(credits_mock):
    t = RouletteTournament(
        chat_id=1,
        thread_id=1,
        host_user_id=USER_1,
        host_username="Player1",
        credits_obj=credits_mock,
        buy_in=BUY_IN,
        max_rounds=MAX_ROUNDS,
    )
    t.add_player(USER_2, "Player2")
    t.add_player(USER_3, "Player3")
    return t


# ── Player Management ──────────────────────────────────────────────


def test_host_added_on_creation(tournament):
    assert USER_1 in tournament.players
    assert tournament.players[USER_1].is_host is True
    assert tournament.players[USER_1].tournament_credits == BUY_IN


def test_host_buy_in_deducted(tournament, credits_mock):
    assert credits_mock.credits[USER_1] == DEFAULT_BALANCE - BUY_IN


def test_add_player_during_joining(tournament):
    message, success = tournament.add_player(USER_2, "Player2")

    assert success is True
    assert USER_2 in tournament.players
    assert "Player2" in message


def test_add_player_deducts_buy_in(tournament, credits_mock):
    tournament.add_player(USER_2, "Player2")

    assert credits_mock.credits[USER_2] == DEFAULT_BALANCE - BUY_IN


def test_add_player_duplicate_rejected(tournament):
    tournament.add_player(USER_2, "Player2")

    message, success = tournament.add_player(USER_2, "Player2")

    assert success is False
    assert "already joined" in message.lower()


def test_add_player_wrong_state_rejected(tournament_with_players):
    tournament_with_players.start_betting_round()

    message, success = tournament_with_players.add_player(USER_3, "Player3")

    assert success is False
    assert "not accepting" in message.lower()


def test_add_player_insufficient_credits(tournament, credits_mock):
    credits_mock.credits[USER_2] = BUY_IN - 1

    message, success = tournament.add_player(USER_2, "Player2")

    assert success is False
    assert "Not enough credits" in message


def test_add_player_unknown_user(tournament, credits_mock):
    unknown = 999

    message, success = tournament.add_player(unknown, "Unknown")

    assert success is False
    assert "Not enough credits" in message


# ── State Management ───────────────────────────────────────────────


def test_has_enough_players_false_with_one(tournament):
    assert tournament.has_enough_players() is False


def test_has_enough_players_true_with_two(tournament_with_players):
    assert tournament_with_players.has_enough_players() is True


def test_cancel_and_refund(tournament_with_players, credits_mock):
    tournament_with_players.cancel_and_refund()

    assert credits_mock.credits[USER_1] == DEFAULT_BALANCE
    assert credits_mock.credits[USER_2] == DEFAULT_BALANCE
    assert tournament_with_players.state == TournamentState.FINISHED


def test_start_betting_round_increments_round(tournament_with_players):
    tournament_with_players.start_betting_round()

    assert tournament_with_players.round_number == 1
    assert tournament_with_players.state == TournamentState.BETTING


def test_is_active_true_during_game(tournament_with_players):
    assert tournament_with_players.is_active is True


def test_is_active_false_after_finish(tournament_with_players):
    tournament_with_players.state = TournamentState.FINISHED

    assert tournament_with_players.is_active is False


def test_is_last_round(tournament_with_players):
    tournament_with_players.round_number = MAX_ROUNDS

    assert tournament_with_players.is_last_round() is True


def test_is_not_last_round(tournament_with_players):
    tournament_with_players.round_number = MAX_ROUNDS - 1

    assert tournament_with_players.is_last_round() is False


# ── Betting ────────────────────────────────────────────────────────


def test_handle_bet_valid(tournament_with_players):
    tournament_with_players.start_betting_round()

    response = tournament_with_players.handle_game_message(USER_1, "bet red 500")

    assert response is not None
    assert "✅" in response
    assert USER_1 in tournament_with_players.bets
    assert tournament_with_players.bets[USER_1] == (RouletteBetType.RED, 500)


def test_handle_bet_insufficient_tournament_credits(tournament_with_players):
    tournament_with_players.start_betting_round()

    response = tournament_with_players.handle_game_message(USER_1, f"bet red {BUY_IN + 1}")

    assert "Not enough tournament credits" in response
    assert USER_1 not in tournament_with_players.bets


def test_handle_bet_invalid_type(tournament_with_players):
    tournament_with_players.start_betting_round()

    response = tournament_with_players.handle_game_message(USER_1, "bet purple 100")

    assert "Invalid bet type" in response
    assert USER_1 not in tournament_with_players.bets


def test_handle_bet_wrong_state(tournament_with_players):
    response = tournament_with_players.handle_game_message(USER_1, "bet red 500")

    assert response is None


def test_handle_bet_zero_credits_player(tournament_with_players):
    tournament_with_players.start_betting_round()
    tournament_with_players.players[USER_1].tournament_credits = 0

    response = tournament_with_players.handle_game_message(USER_1, "bet red 500")

    assert "no tournament credits" in response.lower()


def test_handle_bet_duplicate_rejected(tournament_with_players):
    tournament_with_players.start_betting_round()
    tournament_with_players.handle_game_message(USER_1, "bet red 500")

    response = tournament_with_players.handle_game_message(USER_1, "bet black 300")

    assert "already placed" in response.lower()


def test_handle_bet_non_player_ignored(tournament_with_players):
    tournament_with_players.start_betting_round()

    response = tournament_with_players.handle_game_message(USER_3, "bet red 500")

    assert response is None


def test_handle_bet_negative_amount(tournament_with_players):
    tournament_with_players.start_betting_round()

    response = tournament_with_players.handle_game_message(USER_1, "bet red -100")

    assert "positive" in response.lower()


def test_handle_bet_non_numeric_amount(tournament_with_players):
    tournament_with_players.start_betting_round()

    response = tournament_with_players.handle_game_message(USER_1, "bet red abc")

    assert "number" in response.lower()


def test_handle_non_bet_message_ignored(tournament_with_players):
    tournament_with_players.start_betting_round()

    response = tournament_with_players.handle_game_message(USER_1, "hello world")

    assert response is None


@pytest.mark.parametrize(
    "bet_str, expected_type",
    [
        pytest.param("bet red 100", RouletteBetType.RED, id="red"),
        pytest.param("bet black 100", RouletteBetType.BLACK, id="black"),
        pytest.param("bet green 100", RouletteBetType.GREEN, id="green"),
        pytest.param("bet odd 100", RouletteBetType.ODD, id="odd"),
        pytest.param("bet even 100", RouletteBetType.EVEN, id="even"),
        pytest.param("bet high 100", RouletteBetType.HIGH, id="high"),
        pytest.param("bet low 100", RouletteBetType.LOW, id="low"),
    ],
)
def test_handle_bet_all_valid_types(tournament_with_players, bet_str, expected_type):
    tournament_with_players.start_betting_round()

    response = tournament_with_players.handle_game_message(USER_1, bet_str)

    assert "✅" in response
    assert tournament_with_players.bets[USER_1][0] == expected_type


def test_all_bets_placed_true(tournament_with_players):
    tournament_with_players.start_betting_round()
    tournament_with_players.handle_game_message(USER_1, "bet red 100")
    tournament_with_players.handle_game_message(USER_2, "bet black 100")

    assert tournament_with_players.all_bets_placed() is True


def test_all_bets_placed_false(tournament_with_players):
    tournament_with_players.start_betting_round()
    tournament_with_players.handle_game_message(USER_1, "bet red 100")

    assert tournament_with_players.all_bets_placed() is False


def test_all_bets_placed_skips_zeroed_players(tournament_with_players):
    tournament_with_players.start_betting_round()
    tournament_with_players.players[USER_2].tournament_credits = 0
    tournament_with_players.handle_game_message(USER_1, "bet red 100")

    assert tournament_with_players.all_bets_placed() is True


def test_has_active_bets_true(tournament_with_players):
    tournament_with_players.start_betting_round()
    tournament_with_players.handle_game_message(USER_1, "bet red 100")

    assert tournament_with_players.has_active_bets() is True


def test_has_active_bets_false(tournament_with_players):
    tournament_with_players.start_betting_round()

    assert tournament_with_players.has_active_bets() is False


# ── Round Resolution ───────────────────────────────────────────────


def test_resolve_round_winner_gains_credits(mocker, tournament_with_players):
    mocker.patch("src.models.roulette_tournament.random.choice", return_value=1)
    tournament_with_players.start_betting_round()
    tournament_with_players.handle_game_message(USER_1, "bet red 500")

    tournament_with_players.resolve_round()

    assert tournament_with_players.players[USER_1].tournament_credits == BUY_IN + 500


def test_resolve_round_loser_loses_credits(mocker, tournament_with_players):
    mocker.patch("src.models.roulette_tournament.random.choice", return_value=2)
    tournament_with_players.start_betting_round()
    tournament_with_players.handle_game_message(USER_1, "bet red 500")

    tournament_with_players.resolve_round()

    assert tournament_with_players.players[USER_1].tournament_credits == BUY_IN - 500


def test_resolve_round_green_35x(mocker, tournament_with_players):
    mocker.patch("src.models.roulette_tournament.random.choice", return_value=0)
    tournament_with_players.start_betting_round()
    tournament_with_players.handle_game_message(USER_1, "bet green 100")

    tournament_with_players.resolve_round()

    assert tournament_with_players.players[USER_1].tournament_credits == BUY_IN + 100 * 35


def test_resolve_round_updates_correct_bets(mocker, tournament_with_players):
    mocker.patch("src.models.roulette_tournament.random.choice", return_value=1)
    tournament_with_players.start_betting_round()
    tournament_with_players.handle_game_message(USER_1, "bet red 100")

    tournament_with_players.resolve_round()

    assert tournament_with_players.players[USER_1].correct_bets == 1


def test_resolve_round_updates_total_bets(mocker, tournament_with_players):
    mocker.patch("src.models.roulette_tournament.random.choice", return_value=1)
    tournament_with_players.start_betting_round()
    tournament_with_players.handle_game_message(USER_1, "bet red 100")

    tournament_with_players.resolve_round()

    assert tournament_with_players.players[USER_1].total_bets == 1


def test_resolve_round_clears_bets(mocker, tournament_with_players):
    mocker.patch("src.models.roulette_tournament.random.choice", return_value=1)
    tournament_with_players.start_betting_round()
    tournament_with_players.handle_game_message(USER_1, "bet red 100")

    tournament_with_players.resolve_round()

    assert tournament_with_players.bets == {}


def test_resolve_round_credits_floor_at_zero(mocker, tournament_with_players):
    mocker.patch("src.models.roulette_tournament.random.choice", return_value=2)
    tournament_with_players.start_betting_round()
    tournament_with_players.handle_game_message(USER_1, f"bet red {BUY_IN}")

    tournament_with_players.resolve_round()

    assert tournament_with_players.players[USER_1].tournament_credits == 0


def test_resolve_round_state_becomes_round_result(mocker, tournament_with_players):
    mocker.patch("src.models.roulette_tournament.random.choice", return_value=1)
    tournament_with_players.start_betting_round()
    tournament_with_players.handle_game_message(USER_1, "bet red 100")

    tournament_with_players.resolve_round()

    assert tournament_with_players.state == TournamentState.ROUND_RESULT


def test_resolve_round_multiple_players(mocker, tournament_with_players):
    mocker.patch("src.models.roulette_tournament.random.choice", return_value=1)
    tournament_with_players.start_betting_round()
    tournament_with_players.handle_game_message(USER_1, "bet red 300")
    tournament_with_players.handle_game_message(USER_2, "bet black 200")

    result = tournament_with_players.resolve_round()

    assert tournament_with_players.players[USER_1].tournament_credits == BUY_IN + 300
    assert tournament_with_players.players[USER_2].tournament_credits == BUY_IN - 200
    assert "Winners" in result
    assert "Losers" in result


# ── Evaluate Bet ───────────────────────────────────────────────────


@pytest.mark.parametrize(
    "n, bet_type, expected_won",
    [
        pytest.param(1, RouletteBetType.RED, True, id="red_on_1_wins"),
        pytest.param(2, RouletteBetType.BLACK, True, id="black_on_2_wins"),
        pytest.param(1, RouletteBetType.BLACK, False, id="black_on_1_loses"),
        pytest.param(0, RouletteBetType.GREEN, True, id="green_on_0_wins"),
        pytest.param(5, RouletteBetType.GREEN, False, id="green_on_5_loses"),
        pytest.param(3, RouletteBetType.ODD, True, id="odd_on_3_wins"),
        pytest.param(4, RouletteBetType.EVEN, True, id="even_on_4_wins"),
        pytest.param(0, RouletteBetType.ODD, False, id="odd_on_0_loses"),
        pytest.param(0, RouletteBetType.EVEN, False, id="even_on_0_loses"),
        pytest.param(19, RouletteBetType.HIGH, True, id="high_on_19_wins"),
        pytest.param(18, RouletteBetType.LOW, True, id="low_on_18_wins"),
        pytest.param(0, RouletteBetType.HIGH, False, id="high_on_0_loses"),
        pytest.param(0, RouletteBetType.LOW, False, id="low_on_0_loses"),
    ],
)
def test_evaluate_bet(n, bet_type, expected_won):
    from src.config.constants import ROULETTE_COLORS

    color = RouletteBetType(ROULETTE_COLORS[n])
    is_even = n % 2 == 0 and n != 0
    is_high = n > 18

    won, _ = RouletteTournament._evaluate_bet(n, color, is_even, is_high, bet_type)

    assert won is expected_won


# ── Final Multipliers ─────────────────────────────────────────────


def test_multiplier_2_players():
    assert BaseTournament._get_multiplier(0, 2) == 2.0
    assert BaseTournament._get_multiplier(1, 2) == 0.5


def test_multiplier_3_players():
    assert BaseTournament._get_multiplier(0, 3) == 3.0
    assert BaseTournament._get_multiplier(1, 3) == 1.5
    assert BaseTournament._get_multiplier(2, 3) == 0.5


def test_multiplier_4_players():
    assert BaseTournament._get_multiplier(0, 4) == 3.0
    assert BaseTournament._get_multiplier(1, 4) == 1.5
    assert BaseTournament._get_multiplier(2, 4) == 1.0
    assert BaseTournament._get_multiplier(3, 4) == 0.5


def test_apply_multipliers_2_players(tournament_with_players):
    tournament_with_players.players[USER_1].tournament_credits = 2000
    tournament_with_players.players[USER_1].total_bets = 1
    tournament_with_players.players[USER_1].bet_history = [1]
    tournament_with_players.players[USER_2].tournament_credits = 500
    tournament_with_players.players[USER_2].total_bets = 1
    tournament_with_players.players[USER_2].bet_history = [2]

    payouts = tournament_with_players.apply_final_multipliers()

    assert payouts[USER_1] == 2000 * 2
    assert payouts[USER_2] == int(500 * 0.5)


def test_apply_multipliers_3_players(tournament_3_players):
    for i, uid in enumerate([USER_1, USER_2, USER_3]):
        tournament_3_players.players[uid].total_bets = 1
        tournament_3_players.players[uid].bet_history = [i]
    tournament_3_players.players[USER_1].tournament_credits = 3000
    tournament_3_players.players[USER_2].tournament_credits = 2000
    tournament_3_players.players[USER_3].tournament_credits = 1000

    payouts = tournament_3_players.apply_final_multipliers()

    assert payouts[USER_1] == 3000 * 3
    assert payouts[USER_2] == int(2000 * 1.5)
    assert payouts[USER_3] == int(1000 * 0.5)


def test_apply_multipliers_4_players(credits_mock):
    t = RouletteTournament(1, 1, USER_1, "P1", credits_mock, BUY_IN, MAX_ROUNDS)
    t.add_player(USER_2, "P2")
    t.add_player(USER_3, "P3")
    t.add_player(USER_4, "P4")

    for i, uid in enumerate([USER_1, USER_2, USER_3, USER_4]):
        t.players[uid].total_bets = 1
        t.players[uid].bet_history = [i]

    t.players[USER_1].tournament_credits = 4000
    t.players[USER_2].tournament_credits = 3000
    t.players[USER_3].tournament_credits = 2000
    t.players[USER_4].tournament_credits = 1000

    payouts = t.apply_final_multipliers()

    assert payouts[USER_1] == 4000 * 3
    assert payouts[USER_2] == int(3000 * 1.5)
    assert payouts[USER_3] == int(2000 * 1.0)
    assert payouts[USER_4] == int(1000 * 0.5)


def test_apply_multipliers_tied_players_share_higher(tournament_with_players):
    for i, uid in enumerate([USER_1, USER_2]):
        tournament_with_players.players[uid].total_bets = 1
        tournament_with_players.players[uid].bet_history = [i]
    tournament_with_players.players[USER_1].tournament_credits = 2000
    tournament_with_players.players[USER_2].tournament_credits = 2000

    payouts = tournament_with_players.apply_final_multipliers()

    assert payouts[USER_1] == 2000 * 2
    assert payouts[USER_2] == 2000 * 2


def test_apply_multipliers_tied_3_players_1st_2nd(tournament_3_players):
    for i, uid in enumerate([USER_1, USER_2, USER_3]):
        tournament_3_players.players[uid].total_bets = 1
        tournament_3_players.players[uid].bet_history = [i]
    tournament_3_players.players[USER_1].tournament_credits = 3000
    tournament_3_players.players[USER_2].tournament_credits = 3000
    tournament_3_players.players[USER_3].tournament_credits = 1000

    payouts = tournament_3_players.apply_final_multipliers()

    assert payouts[USER_1] == 3000 * 3
    assert payouts[USER_2] == 3000 * 3
    assert payouts[USER_3] == int(1000 * 0.5)


def test_get_zeroed_players(tournament_with_players):
    tournament_with_players.players[USER_1].tournament_credits = 0
    tournament_with_players.players[USER_2].tournament_credits = 500

    ranking = tournament_with_players.get_ranking()
    zeroed = [p.user_id for p in ranking if p.tournament_credits == 0]

    assert USER_1 in zeroed
    assert USER_2 not in zeroed


def test_distribute_payouts(tournament_with_players, credits_mock):
    initial_1 = credits_mock.credits[USER_1]
    initial_2 = credits_mock.credits[USER_2]

    tournament_with_players.distribute_payouts({USER_1: 5000, USER_2: 2000})

    assert credits_mock.credits[USER_1] == initial_1 + 5000
    assert credits_mock.credits[USER_2] == initial_2 + 2000


def test_finish_updates_state_and_real_credits(tournament_with_players, credits_mock):
    for i, uid in enumerate([USER_1, USER_2]):
        tournament_with_players.players[uid].total_bets = 1
        tournament_with_players.players[uid].bet_history = [i]
    tournament_with_players.players[USER_1].tournament_credits = 2000
    tournament_with_players.players[USER_2].tournament_credits = 500
    initial_1 = credits_mock.credits[USER_1]
    initial_2 = credits_mock.credits[USER_2]

    msg, zeroed = tournament_with_players.finish()

    assert tournament_with_players.state == TournamentState.FINISHED
    assert credits_mock.credits[USER_1] == initial_1 + 2000 * 2
    assert credits_mock.credits[USER_2] == initial_2 + int(500 * 0.5)


def test_finish_returns_zeroed_ids(tournament_with_players):
    tournament_with_players.players[USER_1].tournament_credits = 2000
    tournament_with_players.players[USER_2].tournament_credits = 0

    _, zeroed = tournament_with_players.finish()

    assert USER_2 in zeroed
    assert USER_1 not in zeroed


# ── Format / Display ──────────────────────────────────────────────


def test_format_header(tournament):
    assert "Roulette Tournament" in tournament.format_header()


def test_tournament_type(tournament):
    assert tournament.tournament_type == TournamentType.ROULETTE


def test_format_bets_summary(tournament_with_players):
    tournament_with_players.start_betting_round()
    tournament_with_players.handle_game_message(USER_1, "bet red 500")
    tournament_with_players.handle_game_message(USER_2, "bet black 300")

    summary = tournament_with_players.format_bets_summary()

    assert "Player1" in summary
    assert "Player2" in summary
    assert "red" in summary
    assert "black" in summary


def test_format_bets_summary_shows_skipped(tournament_with_players):
    tournament_with_players.start_betting_round()
    tournament_with_players.handle_game_message(USER_1, "bet red 500")

    summary = tournament_with_players.format_bets_summary()

    assert "Skipped" in summary
    assert "Player2" in summary


def test_get_standings(tournament_with_players):
    standings = tournament_with_players.get_standings()

    assert "Player1" in standings
    assert "Player2" in standings
    assert "Standings" in standings


def test_get_ranking_sorted(tournament_with_players):
    tournament_with_players.players[USER_1].tournament_credits = 500
    tournament_with_players.players[USER_2].tournament_credits = 2000

    ranking = tournament_with_players.get_ranking()

    assert ranking[0].user_id == USER_2
    assert ranking[1].user_id == USER_1


def test_get_final_results_contains_stats(mocker, tournament_with_players):
    mocker.patch("src.models.roulette_tournament.random.choice", return_value=1)
    tournament_with_players.start_betting_round()
    tournament_with_players.handle_game_message(USER_1, "bet red 100")
    tournament_with_players.resolve_round()

    msg, _ = tournament_with_players.get_final_results()

    assert "Final Results" in msg
    assert "Tournament Stats" in msg


# ── Tournament Stats ──────────────────────────────────────────────


def test_tournament_stats_counts(mocker, tournament_with_players):
    mocker.patch("src.models.roulette_tournament.random.choice", return_value=1)
    tournament_with_players.start_betting_round()
    tournament_with_players.handle_game_message(USER_1, "bet red 100")
    tournament_with_players.resolve_round()

    mocker.patch("src.models.roulette_tournament.random.choice", return_value=2)
    tournament_with_players.start_betting_round()
    tournament_with_players.handle_game_message(USER_1, "bet red 100")
    tournament_with_players.resolve_round()

    stats = tournament_with_players.get_tournament_stats()

    assert "Rounds played: 2" in stats
    assert "Red:" in stats
    assert "Black:" in stats


def test_tournament_stats_empty(tournament_with_players):
    stats = tournament_with_players.get_tournament_stats()

    assert stats == ""


# ── BotState Integration ──────────────────────────────────────────


def test_is_tournament_banned_false_initially():
    state = BotState.__new__(BotState)
    state.tournament_daily_bans = {}

    assert state.is_tournament_banned(USER_1, "roulette") is False


def test_ban_from_tournament():
    state = BotState.__new__(BotState)
    state.tournament_daily_bans = {}

    state.ban_from_tournament(USER_1, "roulette")

    assert state.is_tournament_banned(USER_1, "roulette") is True


def test_ban_does_not_affect_other_types():
    state = BotState.__new__(BotState)
    state.tournament_daily_bans = {}

    state.ban_from_tournament(USER_1, "roulette")

    assert state.is_tournament_banned(USER_1, "blackjack") is False


def test_ban_does_not_affect_other_users():
    state = BotState.__new__(BotState)
    state.tournament_daily_bans = {}

    state.ban_from_tournament(USER_1, "roulette")

    assert state.is_tournament_banned(USER_2, "roulette") is False


# ── Round History ─────────────────────────────────────────────────


def test_round_history_recorded(mocker, tournament_with_players):
    mocker.patch("src.models.roulette_tournament.random.choice", return_value=7)
    tournament_with_players.start_betting_round()
    tournament_with_players.handle_game_message(USER_1, "bet red 100")

    tournament_with_players.resolve_round()

    assert len(tournament_with_players.round_history) == 1
    assert tournament_with_players.round_history[0]["number"] == 7


# ── Integration / Full Flow ───────────────────────────────────────


def test_full_tournament_flow(mocker, credits_mock):
    t = RouletteTournament(1, 1, USER_1, "P1", credits_mock, BUY_IN, max_rounds=2)
    t.add_player(USER_2, "P2")

    mocker.patch("src.models.roulette_tournament.random.choice", return_value=1)
    t.start_betting_round()
    t.handle_game_message(USER_1, "bet red 500")
    t.handle_game_message(USER_2, "bet black 300")
    t.resolve_round()

    assert t.players[USER_1].tournament_credits == BUY_IN + 500
    assert t.players[USER_2].tournament_credits == BUY_IN - 300
    assert t.round_number == 1

    mocker.patch("src.models.roulette_tournament.random.choice", return_value=2)
    t.start_betting_round()
    t.handle_game_message(USER_1, "bet black 200")
    t.handle_game_message(USER_2, "bet red 100")
    t.resolve_round()

    assert t.players[USER_1].tournament_credits == BUY_IN + 500 + 200
    assert t.players[USER_2].tournament_credits == BUY_IN - 300 - 100
    assert t.round_number == 2
    assert t.is_last_round() is True

    msg, zeroed = t.get_final_results()

    assert t.state == TournamentState.FINISHED
    assert "Final Results" in msg
    assert len(zeroed) == 0


def test_full_tournament_player_zeroes_out(mocker, credits_mock):
    t = RouletteTournament(1, 1, USER_1, "P1", credits_mock, BUY_IN, max_rounds=2)
    t.add_player(USER_2, "P2")

    mocker.patch("src.models.roulette_tournament.random.choice", return_value=2)
    t.start_betting_round()
    t.handle_game_message(USER_1, f"bet red {BUY_IN}")
    t.handle_game_message(USER_2, "bet black 100")
    t.resolve_round()

    assert t.players[USER_1].tournament_credits == 0

    t.start_betting_round()
    response = t.handle_game_message(USER_1, "bet red 100")
    assert "no tournament credits" in response.lower()

    assert t.all_bets_placed() is False
    t.handle_game_message(USER_2, "bet black 200")
    assert t.all_bets_placed() is True


def test_credits_update_credit_history_defaults(mocker):
    from unittest.mock import MagicMock

    from src.config.enums import CreditActionType
    from src.models.credits import Credits

    c = Credits(MagicMock())
    save_mock = mocker.patch.object(c, "save_credits")
    c.update_credit_history(123, -100, CreditActionType.TOURNAMENT)
    save_mock.assert_called_once()
    df = save_mock.call_args[0][0]
    assert df.iloc[0]["bet_type"] is None
    assert bool(df.iloc[0]["success"]) is True


def test_format_round_start_no_standings_no_underscores(tournament_with_players):
    msg = tournament_with_players.start_betting_round()
    assert "Standings:" not in msg
    assert msg == "Place your bets!"
    assert "_2 player(s) can bet. Place your bets!_" not in msg
    assert "🎰 *Roulette Tournament* — Round 1/" in tournament_with_players.format_header()


def test_format_round_results_shows_didnt_bet_no_standings(mocker, tournament_with_players):
    mocker.patch("src.models.roulette_tournament.random.choice", return_value=1)
    tournament_with_players.start_betting_round()
    tournament_with_players.handle_game_message(USER_1, "bet red 100")
    # USER_2 does not bet

    result = tournament_with_players.resolve_round()
    assert "Standings:" not in result
    assert "*Didn't bet:* 😴" in result
    assert f"• Player2: *{BUY_IN}* credits" in result


def test_no_bets_penalty(tournament_with_players):
    tournament_with_players.players[USER_1].tournament_credits = 1000
    tournament_with_players.players[USER_1].total_bets = 0
    tournament_with_players.players[USER_2].tournament_credits = 1000
    tournament_with_players.players[USER_2].total_bets = 0

    payouts = tournament_with_players.apply_final_multipliers()
    # Both placed 0 bets, both get last place multiplier (0.5x) instead of 2.0x
    assert payouts[USER_1] == int(1000 * 0.5)
    assert payouts[USER_2] == int(1000 * 0.5)

    msg, _ = tournament_with_players.finish()
    assert "⚠️ NO BETS PENALTY" in msg
    assert "Players who placed no bets received last place multipliers" in msg


def test_mirror_bets_penalty(mocker, tournament_with_players):
    mocker.patch("src.models.roulette_tournament.random.choice", return_value=1)
    tournament_with_players.start_betting_round()
    tournament_with_players.handle_game_message(USER_1, "bet red 100")
    tournament_with_players.handle_game_message(USER_2, "bet red 100")
    tournament_with_players.resolve_round()

    payouts = tournament_with_players.apply_final_multipliers()
    # Both have total_bets > 0 and identical bet_history, so both receive mirror bets penalty (0.5x)
    assert payouts[USER_1] == int(tournament_with_players.players[USER_1].tournament_credits * 0.5)
    assert payouts[USER_2] == int(tournament_with_players.players[USER_2].tournament_credits * 0.5)

    msg, _ = tournament_with_players.finish()
    assert "⚠️ MIRROR BETS PENALTY" in msg
    assert "Players who mirrored identical bets in all rounds received last place multipliers" in msg


def test_is_last_round_when_all_zeroed(tournament_with_players):
    tournament_with_players.start_betting_round()
    assert not tournament_with_players.is_last_round()
    tournament_with_players.players[USER_1].tournament_credits = 0
    tournament_with_players.players[USER_2].tournament_credits = 0
    assert tournament_with_players.is_last_round()
