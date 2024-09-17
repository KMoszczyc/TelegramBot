from unittest.mock import patch

import pytest
from src.stats.utils import *
from datetime import datetime

mock_users_df = pd.DataFrame({
    'final_username': ['user1', 'user2', 'user3']
})
users_v2_df = pd.DataFrame({'final_username': ['john_doe', 'jane_doe']})

mock_chat_df = pd.DataFrame({
    'message_id': [1, 2, 3],
    'timestamp': pd.to_datetime(['2023-06-17 16:08:32+00:00', '2023-06-18 16:08:32+00:00', '2023-06-19 16:08:32+00:00']),
    'final_username': ['user1', 'user2', 'user3'],
    'text': ['Hello', 'Hi', 'Hey'],
    'reaction_emojis': [['👍'], ['👎'], ['❤']],
    'photo': [False, False, False]
})

mock_reactions_df = pd.DataFrame({
    'message_id': [1, 2, 3],
    'timestamp': pd.to_datetime(['2023-06-17 16:08:32+00:00', '2023-06-18 16:08:32+00:00', '2023-06-19 16:08:32+00:00']),
    'reacted_to_username': ['user1', 'user2', 'user3'],
    'reacting_username': ['user3', 'user1', 'user2'],
    'emoji': ['👍', '👎', '❤']
})

# Test data
test_timestamp_df = pd.DataFrame([
    {"timestamp": datetime(2023, 10, 10, 1, 0, 0)},  # Included in HOUR, TODAY, MONTH, YEAR, TOTAL
    {"timestamp": datetime(2023, 10, 9, 23, 0, 0)},  # Included in TODAY, MONTH, YEAR, TOTAL
    {"timestamp": datetime(2023, 10, 9, 0, 0, 0)},  # Included in TODAY, MONTH, YEAR, TOTAL
    {"timestamp": datetime(2023, 10, 8, 2, 0, 0)},  # Included in WEEK, MONTH, YEAR, TOTAL
    {"timestamp": datetime(2023, 10, 2, 0, 0, 0)},  # Included in WEEK, MONTH, YEAR, TOTAL
    {"timestamp": datetime(2023, 9, 5, 0, 0, 0)},  # Included in MONTH, YEAR, TOTAL
    {"timestamp": datetime(2023, 8, 15, 0, 0, 0)},  # Included in MONTH, YEAR, TOTAL
    {"timestamp": datetime(2023, 6, 10, 0, 0, 0)},  # Included in YEAR, TOTAL
    {"timestamp": datetime(2022, 9, 10, 0, 0, 0)},  # Included in YEAR, TOTAL
    {"timestamp": datetime(2022, 1, 1, 0, 0, 0)},  # Included in YEAR, TOTAL
    {"timestamp": datetime(2021, 9, 10, 0, 0, 0)},  # Included in TOTAL
])


def mock_get_today_midnight_dt():
    return datetime(2023, 10, 10)


def mock_get_dt_now():
    return datetime(2023, 10, 10, 2, 0, 0)


@pytest.mark.parametrize("input_text, expected_output", [
    ("a.b-c(d){e}[f]_g:h", "a\\.b\\-c\\(d\\)\\{e\\}\\[f\\]\\_g\\:h")
])
def test_correctly_escapes_special_characters(input_text, expected_output):
    assert escape_special_characters(input_text) == expected_output


# @pytest.mark.parametrize("args, expected_period_mode, expected_mode_time, expected_user, expected_error", [
#     (['total'], PeriodFilterMode.TOTAL, -1, None, ''),
#     (['3h'], PeriodFilterMode.HOUR, 3, None, ''),
#     (['today'], PeriodFilterMode.TODAY, -1, None, ''),
#     (['yesterday'], PeriodFilterMode.YESTERDAY, -1, None, ''),
#     (['week'], PeriodFilterMode.WEEK, -1, None, ''),
#     (['month'], PeriodFilterMode.MONTH, -1, None, ''),
#     (['year'], PeriodFilterMode.YEAR, -1, None, ''),
#     (['invalid'], PeriodFilterMode.TOTAL, -1, None, 'There is no such time period as invalid.')
# ])
# def test_parse_args(args, expected_period_mode, expected_mode_time, expected_user, expected_error):
#     command_args = parse_args(mock_users_df, args)
#     assert period_mode == expected_period_mode
#     assert mode_time == expected_mode_time
#     assert user == expected_user
#     assert error == expected_error


@pytest.mark.parametrize("emoji_type, expected_label", [
    (EmojiType.ALL, 'Top'),
    (EmojiType.NEGATIVE, 'Top Sad')
])
def test_emoji_sentiment_to_label(emoji_type, expected_label):
    label = emoji_sentiment_to_label(emoji_type)
    assert label == expected_label


@pytest.mark.parametrize("user_str, expected_user", [
    pytest.param('john_doe', 'john_doe', id="exact match"),
    pytest.param('jane', 'jane_doe', id="partial match"),
    pytest.param('', None, id="empty username"),
    pytest.param('nonexistent_user', None, id="nonexistent user"),
    pytest.param('jo', None, id="partial match too short")
])
def test_parse_user(user_str, expected_user):
    command_args = CommandArgs()
    command_args = parse_user(users_v2_df, command_args, user_str)

    assert command_args.user == expected_user



def get_today_midnight_dt():
    return datetime(2023, 10, 10, 0, 0, 0)


def get_past_hr_dt(hours):
    return datetime(2023, 10, 10, 1, 15, 0) - timedelta(hours=hours)


def mock_get_dt_now():
    return datetime(2023, 10, 10, 2, 15, 0)


@pytest.mark.parametrize("period_mode, period_time, expected_count", [
    (PeriodFilterMode.HOUR, 1, 1),  # ID: hour_filter
    (PeriodFilterMode.TODAY, None, 1),  # ID: today_filter
    (PeriodFilterMode.YESTERDAY, None, 2),  # ID: yesterday_filter
    (PeriodFilterMode.WEEK, None, 4),  # ID: week_filter
    (PeriodFilterMode.MONTH, None, 5),  # ID: month_filter
    (PeriodFilterMode.YEAR, None, 8),  # ID: year_filter
    (PeriodFilterMode.TOTAL, None, 11),  # ID: total_filter
])
def test_filter_by_time_df(period_mode, period_time, expected_count, mocker):
    mocker.patch('src.stats.utils.get_today_midnight_dt', side_effect=get_today_midnight_dt)
    mocker.patch('src.stats.utils.get_past_hr_dt', side_effect=get_past_hr_dt)

    command_args = CommandArgs(period_mode=period_mode, period_time=period_time)
    result_df = filter_by_time_df(test_timestamp_df, command_args)

    assert len(result_df) == expected_count


@pytest.mark.parametrize("period_mode, period_time, expected_count", [
    (PeriodFilterMode.HOUR, 2, 1),  # One entry is within the last hour
    (PeriodFilterMode.TODAY, None, 1),  # Three entries from the shifted "today"
    (PeriodFilterMode.YESTERDAY, None, 1),  # One entry from the shifted "yesterday"
    (PeriodFilterMode.WEEK, None, 1),  # Two entries from the shifted "week"
    (PeriodFilterMode.MONTH, None, 2),  # Six entries from the shifted "month"
    (PeriodFilterMode.YEAR, None, 2),  # Nine entries from the shifted "year"
])
def test_filter_by_shifted_time_df(period_mode, period_time, expected_count, mocker):
    mocker.patch('src.stats.utils.get_today_midnight_dt', side_effect=mock_get_today_midnight_dt)
    mocker.patch('src.stats.utils.get_dt_now', side_effect=mock_get_dt_now)

    command_args = CommandArgs(period_mode=period_mode, period_time=period_time)
    result_df = filter_by_shifted_time_df(test_timestamp_df, command_args)

    assert len(result_df) == expected_count


mock_chat_data = {
    'message_id': [1, 2, 3, 4, 5],
    'user_id': [100, 101, 102, 103, 104]
}

# Mock BOT_ID
BOT_ID = 104


# Mock function to replace read_df
def mock_read_df(path):
    return pd.DataFrame(mock_chat_data)


@pytest.mark.parametrize(
    "message_ids, expected_result",
    [
        pytest.param([1, 2], False, id="non_bot_messages_present"),
        pytest.param([5], True, id="only_bot_message"),
        pytest.param([1, 5], False, id="mixed_messages"),
        pytest.param([], True, id="empty_message_ids"),
        pytest.param([6], False, id="message_id_not_in_chat"),
        pytest.param([1, 2, 3, 4, 5], False, id="all_non_bot_messages"),
        pytest.param([104], False, id="bot_id_as_message_id"),
    ]
)
@patch('src.stats.utils.read_df', side_effect=mock_read_df)
def test_check_bot_messages(mock_read_df, message_ids, expected_result):
    result = check_bot_messages(message_ids, BOT_ID)
    assert result == expected_result


