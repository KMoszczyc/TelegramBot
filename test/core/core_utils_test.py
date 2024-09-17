import pytest

from definitions import PeriodFilterMode
from src.core.utils import parse_period
from src.models.command_args import CommandArgs


@pytest.mark.parametrize("period_str, expected_period_mode, expected_period_time", [
    pytest.param('4h', PeriodFilterMode.HOUR, 4, id="hour_correct"),
    pytest.param('2', PeriodFilterMode.ERROR, -1, id="hour_incorrect"),
    pytest.param('-10h', PeriodFilterMode.ERROR, -1, id="hour_incorrect"),
    pytest.param('aaaaaaa', PeriodFilterMode.ERROR, -1, id="incorrect"),

    pytest.param('today', PeriodFilterMode.TODAY, -1, id="today"),
    pytest.param('yesterday', PeriodFilterMode.YESTERDAY, -1, id="hour"),
    pytest.param('week', PeriodFilterMode.WEEK, -1, id="hour"),
    pytest.param('month', PeriodFilterMode.MONTH, -1, id="hour"),
    pytest.param('year', PeriodFilterMode.YEAR, -1, id="hour"),
    pytest.param('total', PeriodFilterMode.TOTAL, -1, id="hour"),

])
def test_parse_period(period_str, expected_period_mode, expected_period_time):
    command_args = CommandArgs()
    command_args = parse_period(command_args, period_str)

    assert command_args.period_mode == expected_period_mode
    assert command_args.period_time == expected_period_time