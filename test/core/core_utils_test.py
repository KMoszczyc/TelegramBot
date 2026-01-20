"""Tests for core utility functions."""
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from definitions import TIMEZONE, ArgType, DatetimeFormat, MessageType, PeriodFilterMode
from src.core.utils import (
    calculate_skewed_probability,
    datetime_to_ms,
    dt_to_pretty_str,
    generate_period_headline,
    generate_unique_number,
    get_error,
    get_username,
    has_numbers,
    is_aliased_named_arg,
    is_inside_square_brackets,
    is_named_arg,
    is_normal_named_arg,
    is_prime,
    is_word_in_list_of_multiple_words,
    match_substr_to_list_of_texts,
    max_str_length_in_col,
    max_str_length_in_list,
    merge_spaced_args,
    message_id_to_path,
    parse_date,
    parse_date_range,
    parse_int,
    parse_number,
    parse_period,
    parse_string,
    parse_user,
    regexify_multiword_filter,
    remove_punctuation,
    select_random_phrase,
    text_to_number,
    x_to_light_years_str,
)
from src.models.command_args import CommandArgs

# Test constants
TEST_DT_2024_01_15 = datetime(2024, 1, 15, tzinfo=ZoneInfo(TIMEZONE))
TEST_DT_2024_01_01 = datetime(2024, 1, 1, tzinfo=ZoneInfo(TIMEZONE))
TEST_DT_2024_01_31 = datetime(2024, 1, 31, tzinfo=ZoneInfo(TIMEZONE))
TEST_DT_WITH_TIME = datetime(2024, 1, 15, 14, 30, 45, tzinfo=ZoneInfo(TIMEZONE))

TEST_USERS_DF = pd.DataFrame({
    'first_name': ['John', 'Jane'],
    'last_name': ['Doe', 'Smith'],
    'username': ['johndoe', 'janesmith'],
    'final_username': ['JohnDoe', 'JaneSmith']
}, index=[123, 456])


@pytest.mark.parametrize("period_str, expected_period_mode, expected_period_time", [
    pytest.param('4h', PeriodFilterMode.HOUR, 4, id="hour_correct"),
    pytest.param('1h', PeriodFilterMode.HOUR, 1, id="one_hour"),
    pytest.param('24h', PeriodFilterMode.HOUR, 24, id="24_hours"),
    pytest.param('10m', PeriodFilterMode.MINUTE, 10, id="minute_correct"),
    pytest.param('30s', PeriodFilterMode.SECOND, 30, id="second_correct"),
    pytest.param('7d', PeriodFilterMode.DAY, 7, id="day_correct"),
    pytest.param('2w', PeriodFilterMode.WEEK, 2, id="week_correct"),
    pytest.param('0h', PeriodFilterMode.HOUR, 0, id="zero_hours"),
    pytest.param('2', PeriodFilterMode.ERROR, -1, id="no_unit"),
    pytest.param('-10h', PeriodFilterMode.ERROR, -1, id="negative_hour"),
    pytest.param('-5m', PeriodFilterMode.ERROR, -1, id="negative_minute"),
    pytest.param('aaaaaaa', PeriodFilterMode.ERROR, -1, id="invalid_text"),
    pytest.param('', PeriodFilterMode.TOTAL, -1, id="empty_string"),
    pytest.param('h', PeriodFilterMode.ERROR, -1, id="only_unit"),
    pytest.param('10x', PeriodFilterMode.ERROR, -1, id="invalid_unit"),
    pytest.param('today', PeriodFilterMode.TODAY, -1, id="today"),
    pytest.param('yesterday', PeriodFilterMode.YESTERDAY, -1, id="yesterday"),
    pytest.param('week', PeriodFilterMode.WEEK, -1, id="week"),
    pytest.param('month', PeriodFilterMode.MONTH, -1, id="month"),
    pytest.param('year', PeriodFilterMode.YEAR, -1, id="year"),
    pytest.param('total', PeriodFilterMode.TOTAL, -1, id="total"),
])
def test_parse_period(period_str, expected_period_mode, expected_period_time):
    """Test parsing period strings into PeriodFilterMode and time values."""
    command_args = CommandArgs()
    command_args, error = parse_period(command_args, period_str)

    assert command_args.period_mode == expected_period_mode
    assert command_args.period_time == expected_period_time


@pytest.mark.parametrize("num_str, positive_only, expected_num, expected_error", [
    pytest.param('42', False, 42, '', id="valid_positive"),
    pytest.param('-10', False, -10, '', id="valid_negative"),
    pytest.param('0', False, 0, '', id="zero"),
    pytest.param('0', True, 0, '', id="zero_positive_only"),
    pytest.param('1', True, 1, '', id="one_positive_only"),
    pytest.param('999999', False, 999999, '', id="large_number"),
    pytest.param('-999999', False, -999999, '', id="large_negative"),
    pytest.param('  42  ', False, 42, '', id="number_with_spaces"),
    pytest.param('-10', True, -1, 'Number cannot be negative!', id="negative_positive_only"),
    pytest.param('-1', True, -1, 'Number cannot be negative!', id="negative_one_positive"),
    pytest.param('abc', False, None, 'abc is not a number.', id="not_a_number"),
    pytest.param('12.5', False, None, '12.5 is not a number.', id="float_string"),
    pytest.param('', False, None, ' is not a number.', id="empty_string"),
])
def test_parse_int(num_str, positive_only, expected_num, expected_error):
    """Test parsing integer strings with validation."""
    num, error = parse_int(num_str, positive_only)
    assert num == expected_num
    assert expected_error in error


@pytest.mark.parametrize("text, expected", [
    pytest.param('[hello]', True, id="valid_brackets"),
    pytest.param('[]', True, id="empty_brackets"),
    pytest.param('[test', False, id="only_opening"),
    pytest.param('test]', False, id="only_closing"),
    pytest.param('test', False, id="no_brackets"),
    pytest.param('[a]b', False, id="text_after_closing"),
])
def test_is_inside_square_brackets(text, expected):
    """Test checking if text is inside square brackets."""
    assert is_inside_square_brackets(text) == expected


@pytest.mark.parametrize("phrases, error_message, expected_in_result", [
    pytest.param(['hello', 'world'], 'error', ['hello', 'world'], id="non_empty_list"),
    pytest.param([], 'No phrases found', ['No phrases found'], id="empty_list"),
])
def test_select_random_phrase(phrases, error_message, expected_in_result):
    """Test selecting random phrase or returning error message."""
    result = select_random_phrase(phrases, error_message)
    assert isinstance(result, str)
    assert result in expected_in_result


def test_generate_unique_number():
    """Test generating unique number based on user_id and date."""
    user_id = 12345
    result = generate_unique_number(user_id)
    assert isinstance(result, int)
    assert result > 0
    # Test consistency - same user_id on same day should give same result
    result2 = generate_unique_number(user_id)
    assert result == result2


@pytest.mark.parametrize("n, expected", [
    pytest.param(2, True, id="prime_2"),
    pytest.param(3, True, id="prime_3"),
    pytest.param(5, True, id="prime_5"),
    pytest.param(7, True, id="prime_7"),
    pytest.param(11, True, id="prime_11"),
    pytest.param(4, False, id="not_prime_4"),
    pytest.param(6, False, id="not_prime_6"),
    pytest.param(9, False, id="not_prime_9"),
    pytest.param(1, False, id="not_prime_1"),
    pytest.param(0, False, id="not_prime_0"),
    pytest.param(-5, False, id="negative"),
])
def test_is_prime(n, expected):
    """Test prime number detection."""
    assert is_prime(n) == expected


@pytest.mark.parametrize("num_str, expected", [
    pytest.param('abc123', True, id="has_numbers"),
    pytest.param('123', True, id="only_numbers"),
    pytest.param('test123test', True, id="numbers_in_middle"),
    pytest.param('abc', False, id="no_numbers"),
    pytest.param('', False, id="empty_string"),
])
def test_has_numbers(num_str, expected):
    """Test checking if string contains numbers."""
    assert has_numbers(num_str) == expected


@pytest.mark.parametrize("text, expected", [
    pytest.param('hello', 532, id="simple_text"),
    pytest.param('a', 97, id="single_char"),
    pytest.param('', 0, id="empty_string"),
])
def test_text_to_number(text, expected):
    """Test converting text to number by summing character codes."""
    assert text_to_number(text) == expected


@pytest.mark.parametrize("first_name, last_name, expected", [
    pytest.param('John', 'Doe', 'John Doe', id="both_names"),
    pytest.param('John', None, 'John', id="only_first_name"),
    pytest.param(None, 'Doe', 'Doe', id="only_last_name"),
    pytest.param('John', '', 'John', id="empty_last_name"),
    pytest.param(None, None, '', id="no_names"),
])
def test_get_username(first_name, last_name, expected):
    """Test generating username from first and last names."""
    assert get_username(first_name, last_name) == expected


@pytest.mark.parametrize("substr, texts, lower_case, expected", [
    pytest.param('test', ['testing', 'hello', 'test123'], True, 'testing', id="found_lower"),
    pytest.param('TEST', ['testing', 'hello', 'test123'], True, 'testing', id="case_insensitive"),
    pytest.param('Test', ['Testing', 'hello'], False, 'Testing', id="case_sensitive"),
    pytest.param('xyz', ['testing', 'hello', 'test123'], True, None, id="not_found"),
])
def test_match_substr_to_list_of_texts(substr, texts, lower_case, expected):
    """Test matching substring to list of texts."""
    assert match_substr_to_list_of_texts(substr, texts, lower_case) == expected


@pytest.mark.parametrize("x, expected", [
    pytest.param(100, '100', id="small_number"),
    pytest.param(9999999, '9999999', id="just_under_threshold"),
    pytest.param(9460730472580, '1.0 light years', id="one_light_year"),
    pytest.param(94607304725808, '10.0 light years', id="ten_light_years"),
])
def test_x_to_light_years_str(x, expected):
    """Test converting large numbers to light years string."""
    result = x_to_light_years_str(x)
    assert result == expected


def test_datetime_to_ms():
    """Test converting datetime to milliseconds."""
    dt = datetime(2024, 1, 1, 12, 0, 0, tzinfo=ZoneInfo(TIMEZONE))
    result = datetime_to_ms(dt)
    assert isinstance(result, int)
    assert result > 0


@pytest.mark.parametrize("s, expected", [
    pytest.param('hello, world!', 'hello world', id="with_punctuation"),
    pytest.param('test', 'test', id="no_punctuation"),
    pytest.param('!!!', '', id="only_punctuation"),
    pytest.param('', '', id="empty_string"),
])
def test_remove_punctuation(s, expected):
    """Test removing punctuation from strings."""
    assert remove_punctuation(s) == expected


@pytest.mark.parametrize("strings, expected", [
    pytest.param(['hello', 'world', 'test'], 5, id="normal_list"),
    pytest.param(['a', 'bb', 'ccc'], 3, id="increasing_length"),
    pytest.param([''], 0, id="empty_string_in_list"),
    pytest.param([], -1, id="empty_list"),
])
def test_max_str_length_in_list(strings, expected):
    """Test finding max string length in list."""
    assert max_str_length_in_list(strings) == expected


@pytest.mark.parametrize("series_data, expected", [
    pytest.param(['hello', 'world', 'test'], 5, id="normal_series"),
    pytest.param([], -1, id="empty_series"),
])
def test_max_str_length_in_col(series_data, expected):
    """Test finding max string length in pandas Series."""
    series = pd.Series(series_data)
    assert max_str_length_in_col(series) == expected


@pytest.mark.parametrize("value, max_value, expected", [
    pytest.param(0, 100, 0.5, id="zero_value"),
    pytest.param(100, 100, 0.0, id="max_value"),
    pytest.param(25, 100, 0.25, id="quarter_value"),
    pytest.param(0, 0, 0, id="both_zero"),
])
def test_calculate_skewed_probability(value, max_value, expected):
    """Test calculating skewed probability."""
    result = calculate_skewed_probability(value, max_value)
    assert abs(result - expected) < 0.01


@pytest.mark.parametrize("args, expected", [
    pytest.param(['arg1', '"spaced', 'arg"', 'arg2'], ['arg1', 'spaced arg', 'arg2'], id="quoted_args"),
    pytest.param(['arg1', 'arg2', 'arg3'], ['arg1', 'arg2', 'arg3'], id="no_quotes"),
    pytest.param(['arg1', '"single"'], ['arg1', 'single'], id="single_quoted"),
    pytest.param(['"first', 'quoted"', 'middle', '"second', 'quoted"'],
                 ['first quoted', 'middle', 'second quoted'], id="multiple_quoted"),
    pytest.param([], [], id="empty_args"),
])
def test_merge_spaced_args(args, expected):
    """Test merging spaced arguments with quotes."""
    command_args = CommandArgs(args=args)
    result = merge_spaced_args(command_args)
    assert result.args == expected


@pytest.mark.parametrize("word, list_of_multiple_words, expected", [
    pytest.param('test', ['testing hello', 'world'], True, id="found"),
    pytest.param('TEST', ['testing hello', 'world'], True, id="case_insensitive"),
    pytest.param('xyz', ['testing hello', 'world'], False, id="not_found"),
])
def test_is_word_in_list_of_multiple_words(word, list_of_multiple_words, expected):
    """Test checking if word is in list of multiple words."""
    assert is_word_in_list_of_multiple_words(word, list_of_multiple_words) == expected


@pytest.mark.parametrize("date_str, should_succeed, expected_format", [
    pytest.param('15-01-2024', True, DatetimeFormat.DATE, id="valid_date"),
    pytest.param('15-01-2024:14', True, DatetimeFormat.HOUR, id="valid_hour"),
    pytest.param('15-01-2024:14:30', True, DatetimeFormat.MINUTE, id="valid_minute"),
    pytest.param('15-01-2024:14:30:45', True, DatetimeFormat.SECOND, id="valid_second"),
    pytest.param('01-12-2023', True, DatetimeFormat.DATE, id="different_date"),
    pytest.param('31-12-2024:23:59:59', True, DatetimeFormat.SECOND, id="end_of_year"),
    pytest.param('invalid', False, None, id="invalid_date"),
    pytest.param('2024-01-15', False, None, id="wrong_format_iso"),
    pytest.param('15/01/2024', False, None, id="wrong_separator"),
    pytest.param('', False, None, id="empty_string"),
    pytest.param('32-01-2024', False, None, id="invalid_day"),
    pytest.param('15-13-2024', False, None, id="invalid_month"),
])
def test_parse_date(date_str, should_succeed, expected_format):
    """Test parsing date strings in various formats."""
    dt, dt_format, error = parse_date(date_str)
    if should_succeed:
        assert dt is not None
        assert dt_format == expected_format
        assert error == ''
    else:
        assert dt is None
        assert error != ''


@pytest.mark.parametrize("date_range_str, should_succeed, expected_format", [
    pytest.param('01-01-2024;31-01-2024', True, DatetimeFormat.DATE, id="valid_date_range"),
    pytest.param('01-01-2024:10;01-01-2024:15', True, DatetimeFormat.HOUR, id="valid_hour_range"),
    pytest.param('01-01-2024:10:30;01-01-2024:15:45', True, DatetimeFormat.MINUTE, id="valid_minute_range"),
    pytest.param('01-01-2024:10:30:00;01-01-2024:15:45:30', True, DatetimeFormat.SECOND, id="valid_second_range"),
    pytest.param('31-01-2024;01-01-2024', False, None, id="start_after_end"),
    pytest.param('invalid', False, None, id="no_semicolon"),
    pytest.param('2024-01-01;2024-01-31', False, None, id="wrong_format"),
    pytest.param('01-01-2024;15-01-2024;31-01-2024', False, None, id="too_many_semicolons"),
    pytest.param('', False, None, id="empty_string"),
])
def test_parse_date_range(date_range_str, should_succeed, expected_format):
    """Test parsing date range strings."""
    start_dt, end_dt, dt_format, error = parse_date_range(date_range_str)
    if should_succeed:
        assert start_dt is not None
        assert end_dt is not None
        assert start_dt < end_dt
        assert dt_format == expected_format
        assert error == ''
    else:
        assert error != ''


@pytest.mark.parametrize("text, min_len, max_len, should_succeed, has_separator", [
    pytest.param('hello', 3, 10, True, False, id="valid_string"),
    pytest.param('ab', 3, 10, False, False, id="too_short"),
    pytest.param('a' * 20, 3, 10, False, False, id="too_long"),
    pytest.param('hello&world', 3, 20, True, True, id="with_separator"),
])
def test_parse_string(text, min_len, max_len, should_succeed, has_separator):
    """Test parsing and validating strings."""
    command_args = CommandArgs(min_string_length=min_len, max_string_length=max_len, label='Test')
    result_text, result_args, error = parse_string(command_args, text)

    assert result_text == text
    if should_succeed:
        assert error == ''
        if has_separator:
            assert result_args.strings == text.split('&')
        else:
            assert result_args.string == text
    else:
        assert error != ''


def test_dt_to_pretty_str():
    """Test converting datetime to pretty string format."""
    result = dt_to_pretty_str(TEST_DT_WITH_TIME)
    assert '15-01-2024' in result
    assert '14:30:45' in result


def test_regexify_multiword_filter():
    """Test creating regex pattern for multiword filter."""
    words = ['hello', 'world']
    result = regexify_multiword_filter(words)
    assert isinstance(result, str)
    assert 'hello' in result
    assert 'world' in result


@pytest.mark.parametrize("period_mode, period_time, dt, start_dt, end_dt, dt_format, expected", [
    pytest.param(PeriodFilterMode.HOUR, 5, None, None, None, None, 'past 5 hours', id="hour"),
    pytest.param(PeriodFilterMode.SECOND, 30, None, None, None, None, 'past 30 seconds', id="second"),
    pytest.param(PeriodFilterMode.MINUTE, 15, None, None, None, None, 'past 15 minutes', id="minute"),
    pytest.param(PeriodFilterMode.DAY, 7, None, None, None, None, 'past 7 days', id="day"),
    pytest.param(PeriodFilterMode.WEEK, 2, None, None, None, None, 'past 2 weeks', id="week_with_time"),
    pytest.param(PeriodFilterMode.TODAY, -1, None, None, None, None, 'today', id="today"),
    pytest.param(PeriodFilterMode.YESTERDAY, -1, None, None, None, None, 'yesterday', id="yesterday"),
    pytest.param(PeriodFilterMode.WEEK, -1, None, None, None, None, 'week', id="week_no_time"),
    pytest.param(PeriodFilterMode.MONTH, -1, None, None, None, None, 'month', id="month"),
    pytest.param(PeriodFilterMode.YEAR, -1, None, None, None, None, 'year', id="year"),
    pytest.param(PeriodFilterMode.TOTAL, -1, None, None, None, None, 'total', id="total"),
])
def test_generate_period_headline(period_mode, period_time, dt, start_dt, end_dt, dt_format, expected):
    """Test generating period headline strings."""
    command_args = CommandArgs(
        period_mode=period_mode,
        period_time=period_time,
        dt=dt or TEST_DT_2024_01_15,
        start_dt=start_dt or TEST_DT_2024_01_01,
        end_dt=end_dt or TEST_DT_2024_01_31,
        dt_format=dt_format or DatetimeFormat.DATE
    )
    result = generate_period_headline(command_args)
    assert expected in result


def test_generate_period_headline_date():
    """Test generating period headline for DATE mode."""
    command_args = CommandArgs(
        period_mode=PeriodFilterMode.DATE,
        dt=TEST_DT_2024_01_15,
        dt_format=DatetimeFormat.DATE
    )
    result = generate_period_headline(command_args)
    assert '15-01-2024' in result


def test_generate_period_headline_date_range():
    """Test generating period headline for DATE_RANGE mode."""
    command_args = CommandArgs(
        period_mode=PeriodFilterMode.DATE_RANGE,
        start_dt=TEST_DT_2024_01_01,
        end_dt=TEST_DT_2024_01_31,
        dt_format=DatetimeFormat.DATE
    )
    result = generate_period_headline(command_args)
    assert '01-01-2024' in result
    assert '31-01-2024' in result


@pytest.mark.parametrize("user_str, should_succeed, expected_user, expected_user_id", [
    pytest.param('JohnDoe', True, 'JohnDoe', 123, id="exact_match"),
    pytest.param('Jane', True, 'JaneSmith', 456, id="partial_match"),
    pytest.param('@JohnDoe', True, 'JohnDoe', 123, id="with_at_symbol"),
    pytest.param('NonExistent', False, None, None, id="non_existent"),
])
def test_parse_user(user_str, should_succeed, expected_user, expected_user_id):
    """Test parsing user from string."""
    command_args = CommandArgs()
    result_args, error = parse_user(TEST_USERS_DF, command_args, user_str)

    if should_succeed:
        assert error == ''
        assert result_args.user == expected_user
        assert result_args.user_id == expected_user_id
    else:
        assert error != ''


@pytest.mark.parametrize("arg, expected", [
    pytest.param('--user', True, id="normal_user"),
    pytest.param('-u', True, id="alias_user"),
    pytest.param('--period', True, id="normal_period"),
    pytest.param('-p', True, id="alias_period"),
    pytest.param('notanarg', False, id="not_an_arg"),
])
def test_is_named_arg(arg, expected):
    """Test checking if argument is a named argument."""
    command_args = CommandArgs(
        available_named_args={'user': ArgType.USER, 'period': ArgType.PERIOD},
        available_named_args_aliases={'u': 'user', 'p': 'period'}
    )
    assert is_named_arg(arg, command_args) == expected


@pytest.mark.parametrize("arg, expected", [
    pytest.param('--user', True, id="valid_user"),
    pytest.param('--period', True, id="valid_period"),
    pytest.param('-u', False, id="alias_format"),
    pytest.param('user', False, id="no_dashes"),
])
def test_is_normal_named_arg(arg, expected):
    """Test checking if argument is a normal named argument."""
    available_named_args = {'user': ArgType.USER, 'period': ArgType.PERIOD}
    assert is_normal_named_arg(arg, available_named_args) == expected


@pytest.mark.parametrize("arg, expected", [
    pytest.param('-u', True, id="valid_u"),
    pytest.param('-p', True, id="valid_p"),
    pytest.param('--user', False, id="normal_format"),
    pytest.param('u', False, id="no_dash"),
])
def test_is_aliased_named_arg(arg, expected):
    """Test checking if argument is an aliased named argument."""
    aliases = {'u': 'user', 'p': 'period'}
    assert is_aliased_named_arg(arg, aliases) == expected


@pytest.mark.parametrize("num_str, min_num, max_num, positive_only, should_succeed, error_contains", [
    pytest.param('50', 1, 100, False, True, '', id="valid_middle"),
    pytest.param('1', 1, 100, False, True, '', id="boundary_min"),
    pytest.param('100', 1, 100, False, True, '', id="boundary_max"),
    pytest.param('200', 1, 100, False, False, 'too big', id="too_large"),
    pytest.param('-5', 1, 100, False, False, 'too small', id="too_small"),
    pytest.param('0', 1, 100, False, False, 'too small', id="below_min"),
    pytest.param('101', 1, 100, False, False, 'too big', id="above_max"),
    pytest.param('-5', 1, 100, True, False, 'negative', id="negative_positive_only"),
    pytest.param('', 1, 100, False, True, '', id="empty_string"),
    pytest.param('abc', 1, 100, False, False, 'not a number', id="invalid_number"),
])
def test_parse_number(num_str, min_num, max_num, positive_only, should_succeed, error_contains):
    """Test parsing and validating numbers."""
    command_args = CommandArgs(min_number=min_num, max_number=max_num)
    num, result_args, error = parse_number(command_args, num_str, positive_only)

    if should_succeed:
        if num_str:  # Not empty string
            assert num is not None
            assert error == ''
            assert result_args.number == num
        else:
            assert num is None
            assert error == ''
    else:
        assert error != ''
        assert error_contains.lower() in error.lower()


@pytest.mark.parametrize("errors, expected_result", [
    pytest.param(['Error 1', 'Error 2'], 'Error 1\nError 2', id="multiple_errors"),
    pytest.param(['Single error'], 'Single error', id="single_error"),
    pytest.param([], '', id="no_errors"),
])
def test_get_error(errors, expected_result):
    """Test getting error message from CommandArgs."""
    command_args = CommandArgs(errors=errors)
    result = get_error(command_args)
    assert result == expected_result


@pytest.mark.parametrize("message_id, message_type, expected_extension", [
    pytest.param(123, MessageType.IMAGE, '.jpg', id="image"),
    pytest.param(456, MessageType.GIF, '.mp4', id="gif"),
    pytest.param(789, MessageType.VIDEO, '.mp4', id="video"),
    pytest.param(101, MessageType.AUDIO, '.ogg', id="audio"),
])
def test_message_id_to_path(message_id, message_type, expected_extension):
    """Test converting message ID and type to file path."""
    result = message_id_to_path(message_id, message_type)
    assert result is not None
    assert result.endswith(expected_extension)
    assert str(message_id) in result
