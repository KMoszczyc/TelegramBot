from unittest.mock import MagicMock

import pytest

from src.config.enums import CreditActionType, ErrorMessage, RouletteBetType
from src.models.roulette import Roulette

USER_ID = 111
DEFAULT_BALANCE = 1000


@pytest.fixture()
def credits_mock():
    mock = MagicMock()
    mock.credits = {USER_ID: DEFAULT_BALANCE}
    mock.update_credit_history = MagicMock()
    return mock


@pytest.fixture()
def roulette(credits_mock):
    return Roulette(credits_mock)


# ── parse_bet ────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "arg, expected",
    [
        pytest.param("red", RouletteBetType.RED, id="red"),
        pytest.param("black", RouletteBetType.BLACK, id="black"),
        pytest.param("green", RouletteBetType.GREEN, id="green"),
        pytest.param("odd", RouletteBetType.ODD, id="odd"),
        pytest.param("even", RouletteBetType.EVEN, id="even"),
        pytest.param("high", RouletteBetType.HIGH, id="high"),
        pytest.param("low", RouletteBetType.LOW, id="low"),
        pytest.param("single_number", RouletteBetType.SINGLE_NUMBER, id="single_number"),
    ],
)
def test_parse_bet_valid(roulette, arg, expected):
    result = roulette.parse_bet(arg)

    assert result == expected


@pytest.mark.parametrize(
    "arg",
    [
        pytest.param("", id="empty_string"),
        pytest.param("invalid", id="invalid_string"),
        pytest.param("RED", id="uppercase"),
        pytest.param("123", id="number_string"),
    ],
)
def test_parse_bet_invalid_returns_none(roulette, arg):
    result = roulette.parse_bet(arg)

    assert result == RouletteBetType.NONE


# ── play – early exits ──────────────────────────────────────────────


def test_play_insufficient_credits(roulette):
    roulette.credits.credits[USER_ID] = 5

    message, success = roulette.play(USER_ID, 10, "red")

    assert message == ErrorMessage.ROULETTE_NOT_ENOUGH_CREDITS
    assert success is False


def test_play_user_not_in_credits_dict(roulette):
    unknown_user = 999

    message, success = roulette.play(unknown_user, 10, "red")

    assert message == ErrorMessage.ROULETTE_NOT_ENOUGH_CREDITS
    assert success is False


def test_play_invalid_bet_type(roulette):
    message, success = roulette.play(USER_ID, 10, "invalid")

    assert message == ErrorMessage.ROULETTE_INVALID_BET
    assert success is False


def test_play_exact_balance_allowed(mocker, roulette):
    roulette.credits.credits[USER_ID] = 50
    mocker.patch("src.models.roulette.random.choice", return_value=1)

    message, success = roulette.play(USER_ID, 50, "red")

    assert message != ErrorMessage.ROULETTE_NOT_ENOUGH_CREDITS
    assert message != ErrorMessage.ROULETTE_INVALID_BET


# ── play – routing ──────────────────────────────────────────────────


@pytest.mark.parametrize(
    "bet_arg, delegated_method",
    [
        pytest.param("red", "play_red_black", id="red_routes_to_red_black"),
        pytest.param("black", "play_red_black", id="black_routes_to_red_black"),
        pytest.param("green", "play_red_black", id="green_routes_to_red_black"),
        pytest.param("odd", "play_odd_even", id="odd_routes_to_odd_even"),
        pytest.param("even", "play_odd_even", id="even_routes_to_odd_even"),
        pytest.param("high", "play_high_low", id="high_routes_to_high_low"),
        pytest.param("low", "play_high_low", id="low_routes_to_high_low"),
    ],
)
def test_play_routes_to_correct_method(mocker, roulette, bet_arg, delegated_method):
    spy = mocker.patch.object(roulette, delegated_method, return_value=("msg", True))

    roulette.play(USER_ID, 10, bet_arg)

    spy.assert_called_once()


# ── play_red_black ──────────────────────────────────────────────────


@pytest.mark.parametrize(
    "rolled_number, bet_type, should_win",
    [
        pytest.param(1, RouletteBetType.RED, True, id="red_bet_red_result_wins"),
        pytest.param(2, RouletteBetType.BLACK, True, id="black_bet_black_result_wins"),
        pytest.param(1, RouletteBetType.BLACK, False, id="black_bet_red_result_loses"),
        pytest.param(2, RouletteBetType.RED, False, id="red_bet_black_result_loses"),
        pytest.param(0, RouletteBetType.RED, False, id="red_bet_green_result_loses"),
        pytest.param(0, RouletteBetType.BLACK, False, id="black_bet_green_result_loses"),
    ],
)
def test_play_red_black_outcomes(mocker, roulette, rolled_number, bet_type, should_win):
    mocker.patch("src.models.roulette.random.choice", return_value=rolled_number)
    bet_size = 100

    message, success = roulette.play_red_black(USER_ID, bet_size, bet_type)

    assert success is should_win
    if should_win:
        assert "Congrats" in message
        assert roulette.credits.credits[USER_ID] == DEFAULT_BALANCE + bet_size
    else:
        assert "lose" in message.lower()
        assert roulette.credits.credits[USER_ID] == DEFAULT_BALANCE - bet_size


def test_play_red_black_green_bet_wins_35x(mocker, roulette):
    mocker.patch("src.models.roulette.random.choice", return_value=0)
    bet_size = 10

    message, success = roulette.play_red_black(USER_ID, bet_size, RouletteBetType.GREEN)

    assert success is True
    assert "Congrats" in message
    assert f"{bet_size * 35} credits" in message
    assert roulette.credits.credits[USER_ID] == DEFAULT_BALANCE + bet_size * 35


def test_play_red_black_green_bet_loses_on_non_zero(mocker, roulette):
    mocker.patch("src.models.roulette.random.choice", return_value=5)
    bet_size = 10

    message, success = roulette.play_red_black(USER_ID, bet_size, RouletteBetType.GREEN)

    assert success is False
    assert roulette.credits.credits[USER_ID] == DEFAULT_BALANCE - bet_size


# ── play_odd_even ───────────────────────────────────────────────────


@pytest.mark.parametrize(
    "rolled_number, bet_type, should_win",
    [
        pytest.param(3, RouletteBetType.ODD, True, id="odd_bet_odd_result_wins"),
        pytest.param(4, RouletteBetType.EVEN, True, id="even_bet_even_result_wins"),
        pytest.param(3, RouletteBetType.EVEN, False, id="even_bet_odd_result_loses"),
        pytest.param(4, RouletteBetType.ODD, False, id="odd_bet_even_result_loses"),
    ],
)
def test_play_odd_even_outcomes(mocker, roulette, rolled_number, bet_type, should_win):
    mocker.patch("src.models.roulette.random.choice", return_value=rolled_number)
    bet_size = 100

    message, success = roulette.play_odd_even(USER_ID, bet_size, bet_type)

    assert success is should_win
    if should_win:
        assert "Congrats" in message
        assert roulette.credits.credits[USER_ID] == DEFAULT_BALANCE + bet_size
    else:
        assert "lose" in message.lower()
        assert roulette.credits.credits[USER_ID] == DEFAULT_BALANCE - bet_size


@pytest.mark.parametrize(
    "bet_type",
    [
        pytest.param(RouletteBetType.ODD, id="odd_bet_on_zero"),
        pytest.param(RouletteBetType.EVEN, id="even_bet_on_zero"),
    ],
)
def test_play_odd_even_zero_always_loses(mocker, roulette, bet_type):
    mocker.patch("src.models.roulette.random.choice", return_value=0)
    bet_size = 50

    message, success = roulette.play_odd_even(USER_ID, bet_size, bet_type)

    assert success is False
    assert roulette.credits.credits[USER_ID] == DEFAULT_BALANCE - bet_size


# ── play_high_low ───────────────────────────────────────────────────


@pytest.mark.parametrize(
    "rolled_number, bet_type, should_win",
    [
        pytest.param(19, RouletteBetType.HIGH, True, id="high_bet_19_wins"),
        pytest.param(36, RouletteBetType.HIGH, True, id="high_bet_36_wins"),
        pytest.param(1, RouletteBetType.LOW, True, id="low_bet_1_wins"),
        pytest.param(18, RouletteBetType.LOW, True, id="low_bet_18_wins"),
        pytest.param(19, RouletteBetType.LOW, False, id="low_bet_19_loses"),
        pytest.param(1, RouletteBetType.HIGH, False, id="high_bet_1_loses"),
    ],
)
def test_play_high_low_outcomes(mocker, roulette, rolled_number, bet_type, should_win):
    mocker.patch("src.models.roulette.random.choice", return_value=rolled_number)
    bet_size = 100

    message, success = roulette.play_high_low(USER_ID, bet_size, bet_type)

    assert success is should_win
    if should_win:
        assert "Congrats" in message
        assert roulette.credits.credits[USER_ID] == DEFAULT_BALANCE + bet_size
    else:
        assert "lose" in message.lower()
        assert roulette.credits.credits[USER_ID] == DEFAULT_BALANCE - bet_size


@pytest.mark.parametrize(
    "bet_type",
    [
        pytest.param(RouletteBetType.HIGH, id="high_bet_on_zero"),
        pytest.param(RouletteBetType.LOW, id="low_bet_on_zero"),
    ],
)
def test_play_high_low_zero_always_loses(mocker, roulette, bet_type):
    mocker.patch("src.models.roulette.random.choice", return_value=0)
    bet_size = 50

    message, success = roulette.play_high_low(USER_ID, bet_size, bet_type)

    assert success is False
    assert roulette.credits.credits[USER_ID] == DEFAULT_BALANCE - bet_size


# ── apply_bet ───────────────────────────────────────────────────────


def test_apply_bet_winning_updates_balance_and_history(roulette):
    bet_size = 100

    message, success = roulette.apply_bet(
        n=7,
        result=RouletteBetType.RED,
        user_id=USER_ID,
        bet_size=bet_size,
        winning_rule=True,
        bet_type=RouletteBetType.RED,
    )

    assert roulette.credits.credits[USER_ID] == DEFAULT_BALANCE + bet_size
    assert success is True
    assert "Congrats" in message
    assert f"{bet_size} credits" in message
    roulette.credits.update_credit_history.assert_called_once_with(
        USER_ID,
        bet_size,
        CreditActionType.BET,
        RouletteBetType.RED,
        True,
    )


def test_apply_bet_losing_updates_balance_and_history(roulette):
    bet_size = 100

    message, success = roulette.apply_bet(
        n=7,
        result=RouletteBetType.RED,
        user_id=USER_ID,
        bet_size=bet_size,
        winning_rule=False,
        bet_type=RouletteBetType.BLACK,
    )

    assert roulette.credits.credits[USER_ID] == DEFAULT_BALANCE - bet_size
    assert success is False
    assert "lose" in message.lower()
    assert f"{bet_size} credits" in message
    roulette.credits.update_credit_history.assert_called_once_with(
        USER_ID,
        -bet_size,
        CreditActionType.BET,
        RouletteBetType.BLACK,
        False,
    )


def test_apply_bet_special_rule_35x_payout(roulette):
    bet_size = 10

    message, success = roulette.apply_bet(
        n=0,
        result=RouletteBetType.GREEN,
        user_id=USER_ID,
        bet_size=bet_size,
        winning_rule=True,
        bet_type=RouletteBetType.GREEN,
        special_rule=True,
        payout_multiplier=35,
    )

    assert roulette.credits.credits[USER_ID] == DEFAULT_BALANCE + bet_size * 35
    assert success is True
    assert f"{bet_size * 35} credits" in message
    roulette.credits.update_credit_history.assert_called_once_with(
        USER_ID,
        bet_size * 35,
        CreditActionType.BET,
        RouletteBetType.GREEN,
        True,
    )


def test_apply_bet_message_contains_rolled_number_and_result(roulette):
    message, _ = roulette.apply_bet(
        n=17,
        result=RouletteBetType.BLACK,
        user_id=USER_ID,
        bet_size=10,
        winning_rule=False,
        bet_type=RouletteBetType.RED,
    )

    assert "*17*" in message
    assert "*black*" in message


# ── integration through play() ──────────────────────────────────────


def test_play_full_flow_win(mocker, roulette):
    mocker.patch("src.models.roulette.random.choice", return_value=1)
    bet_size = 50

    message, success = roulette.play(USER_ID, bet_size, "red")

    assert success is True
    assert roulette.credits.credits[USER_ID] == DEFAULT_BALANCE + bet_size
    roulette.credits.update_credit_history.assert_called_once()


def test_play_full_flow_lose(mocker, roulette):
    mocker.patch("src.models.roulette.random.choice", return_value=2)
    bet_size = 50

    message, success = roulette.play(USER_ID, bet_size, "red")

    assert success is False
    assert roulette.credits.credits[USER_ID] == DEFAULT_BALANCE - bet_size
    roulette.credits.update_credit_history.assert_called_once()


def test_play_drains_all_credits(mocker, roulette):
    mocker.patch("src.models.roulette.random.choice", return_value=2)
    roulette.credits.credits[USER_ID] = 100

    message, success = roulette.play(USER_ID, 100, "red")

    assert success is False
    assert roulette.credits.credits[USER_ID] == 0
