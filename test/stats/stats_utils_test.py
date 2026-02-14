"""Tests for stats utility functions."""
import os
from datetime import datetime
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from src.config.constants import TIMEZONE
from src.config.enums import DatetimeFormat, EmojiType, PeriodFilterMode
from src.models.command_args import CommandArgs
from src.stats.utils import (
    are_text_characters_allowed,
    check_new_username,
    contains_stopwords,
    dt_to_str,
    emoji_sentiment_to_label,
    enum_to_list,
    escape_special_characters,
    filter_by_shifted_time_df,
    filter_by_time_df,
    filter_df_in_range,
    filter_emoji_by_emoji_type,
    filter_emojis_by_emoji_type,
    generate_random_file_id,
    generate_random_filename,
    get_forbidden_usernames,
    get_last_message_id_of_a_user,
    get_random_media_path,
    get_users_map,
    is_alpha_numeric,
    is_chat_etl_locked,
    is_list_column,
    is_ngram_contaminated_by_stopwords,
    is_ngram_valid,
    is_string_column,
    lock_chat_etl,
    remove_chat_etl_lock,
    remove_diactric_accents,
    text_to_word_length_sum,
    username_to_user_id,
    validate_schema,
)

# Mock constants
MOCK_DT_2023_10_10 = datetime(2023, 10, 10, 1, 18, 0).replace(tzinfo=ZoneInfo(TIMEZONE))
MOCK_DT_2023_10_10_MIDNIGHT = datetime(2023, 10, 10, 0, 0, 0).replace(tzinfo=ZoneInfo(TIMEZONE))

# Mock DataFrames
mock_users_df = pd.DataFrame({"user_id": [100, 101, 102], "final_username": ["user1", "user2", "user3"]})

mock_chat_df = pd.DataFrame(
    {
        "message_id": [1, 2, 3, 4, 5],
        "user_id": [100, 101, 102, 103, 999],
        "timestamp": pd.to_datetime(
            [
                "2023-06-17 16:08:32+00:00",
                "2023-06-18 16:08:32+00:00",
                "2023-06-19 16:08:32+00:00",
                "2023-06-20 16:08:32+00:00",
                "2023-06-21 16:08:32+00:00",
            ]
        ),
        "final_username": ["user1", "user2", "user3", "user4", "bot"],
        "text": ["Hello", "Hi", "Hey", "Howdy", "Bot message"],
        "reaction_emojis": [["👍"], ["👎", "😢"], ["❤"], ["🔥"], ["👍"]],
        "photo": [False, False, True, False, False],
    }
)

mock_reactions_df = pd.DataFrame(
    {
        "message_id": [1, 2, 3, 4],
        "timestamp": pd.to_datetime(
            ["2023-06-17 16:08:32+00:00", "2023-06-18 16:08:32+00:00", "2023-06-19 16:08:32+00:00", "2023-06-20 16:08:32+00:00"]
        ),
        "reacted_to_username": ["user1", "user2", "user3", "user1"],
        "reacting_username": ["user3", "user1", "user2", "user2"],
        "emoji": ["👍", "👎", "❤", "😢"],
    }
)

test_timestamp_df = pd.DataFrame(
    [
        {"timestamp": datetime(2023, 10, 10, 1, 18, 0).replace(tzinfo=ZoneInfo(TIMEZONE))},
        {"timestamp": datetime(2023, 10, 9, 23, 1, 0).replace(tzinfo=ZoneInfo(TIMEZONE))},
        {"timestamp": datetime(2023, 10, 9, 0, 0, 0).replace(tzinfo=ZoneInfo(TIMEZONE))},
        {"timestamp": datetime(2023, 10, 8, 2, 0, 0).replace(tzinfo=ZoneInfo(TIMEZONE))},
        {"timestamp": datetime(2023, 10, 2, 0, 0, 0).replace(tzinfo=ZoneInfo(TIMEZONE))},
        {"timestamp": datetime(2023, 9, 5, 0, 0, 0).replace(tzinfo=ZoneInfo(TIMEZONE))},
        {"timestamp": datetime(2023, 8, 15, 0, 0, 0).replace(tzinfo=ZoneInfo(TIMEZONE))},
        {"timestamp": datetime(2023, 6, 10, 0, 0, 0).replace(tzinfo=ZoneInfo(TIMEZONE))},
        {"timestamp": datetime(2022, 9, 10, 0, 0, 0).replace(tzinfo=ZoneInfo(TIMEZONE))},
        {"timestamp": datetime(2022, 1, 1, 0, 0, 0).replace(tzinfo=ZoneInfo(TIMEZONE))},
        {"timestamp": datetime(2021, 9, 10, 0, 0, 0).replace(tzinfo=ZoneInfo(TIMEZONE))},
    ]
)


# Helper functions for mocking
def mock_get_today_midnight_dt():
    return MOCK_DT_2023_10_10_MIDNIGHT


def mock_get_dt_now():
    return MOCK_DT_2023_10_10


# Tests for escape_special_characters
@pytest.mark.parametrize(
    "input_text, expected_output",
    [
        pytest.param("hello", "hello", id="no_special_chars"),
        pytest.param("a.b-c(d){e}[f]_g:h", "a\\.b\\-c\\(d\\)\\{e\\}\\[f\\]\\_g\\:h", id="all_special_chars"),
        pytest.param("test+value", "test\\+value", id="plus_sign"),
        pytest.param("test!value", "test\\!value", id="exclamation"),
        pytest.param("test<>value", "test\\<\\>value", id="angle_brackets"),
        pytest.param("test#value", "test\\#value", id="hash"),
        pytest.param("test^value", "test\\^value", id="caret"),
        pytest.param("", "", id="empty_string"),
    ],
)
def test_escape_special_characters(input_text, expected_output):
    """Test escaping special characters for regex."""
    assert escape_special_characters(input_text) == expected_output


@pytest.mark.parametrize("emoji_type, expected_label", [(EmojiType.ALL, "Top"), (EmojiType.NEGATIVE, "Top Sad")])
def test_emoji_sentiment_to_label(emoji_type, expected_label):
    label = emoji_sentiment_to_label(emoji_type)
    assert label == expected_label


# Tests for contains_stopwords
@pytest.mark.parametrize(
    "text, stopwords, expected",
    [
        pytest.param("hello world", ["world"], True, id="contains_stopword"),
        pytest.param("hello there", ["world"], False, id="no_stopword"),
        pytest.param("Hello World", ["world"], True, id="case_insensitive"),
        pytest.param("the quick brown fox", ["the", "a"], True, id="multiple_stopwords"),
        pytest.param("", ["test"], False, id="empty_string"),
        pytest.param("test", [], False, id="empty_stopwords"),
    ],
)
def test_contains_stopwords(text, stopwords, expected):
    """Test checking if text contains stopwords."""
    assert contains_stopwords(text, stopwords) == expected


# Tests for is_ngram_contaminated_by_stopwords
@pytest.mark.parametrize(
    "words_str, ratio_threshold, stopwords, expected",
    [
        pytest.param("the quick brown", 0.5, ["the"], False, id="below_threshold"),
        pytest.param("the a an", 0.5, ["the", "a", "an"], True, id="above_threshold"),
        pytest.param("hello world", 0.5, ["world"], False, id="exactly_half"),
        pytest.param("test word", 0.5, [], False, id="no_stopwords"),
        pytest.param("the", 0.5, ["the"], True, id="single_word_stopword"),
    ],
)
def test_is_ngram_contaminated_by_stopwords(words_str, ratio_threshold, stopwords, expected):
    """Test filtering ngrams contaminated by stopwords."""
    assert is_ngram_contaminated_by_stopwords(words_str, ratio_threshold, stopwords) == expected


# Tests for is_ngram_valid
@pytest.mark.parametrize(
    "words_str, expected",
    [
        pytest.param("hello world", True, id="different_words"),
        pytest.param("test test", False, id="repeated_words"),
        pytest.param("single", True, id="single_word"),
        pytest.param("one two three", True, id="multiple_different"),
    ],
)
def test_is_ngram_valid(words_str, expected):
    """Test validating ngrams."""
    assert is_ngram_valid(words_str) == expected


# Tests for filter_df_in_range
@pytest.mark.parametrize(
    "start_dt, end_dt, expected_count",
    [
        pytest.param(
            datetime(2023, 10, 9, 0, 0, 0).replace(tzinfo=ZoneInfo(TIMEZONE)),
            datetime(2023, 10, 10, 0, 0, 0).replace(tzinfo=ZoneInfo(TIMEZONE)),
            2,
            id="one_day_range",
        ),
        pytest.param(
            datetime(2023, 1, 1, 0, 0, 0).replace(tzinfo=ZoneInfo(TIMEZONE)),
            datetime(2024, 1, 1, 0, 0, 0).replace(tzinfo=ZoneInfo(TIMEZONE)),
            8,
            id="full_year",
        ),
        pytest.param(
            datetime(2025, 1, 1, 0, 0, 0).replace(tzinfo=ZoneInfo(TIMEZONE)),
            datetime(2026, 1, 1, 0, 0, 0).replace(tzinfo=ZoneInfo(TIMEZONE)),
            0,
            id="future_range",
        ),
    ],
)
def test_filter_df_in_range(start_dt, end_dt, expected_count):
    """Test filtering dataframe by date range."""
    result_df = filter_df_in_range(test_timestamp_df, start_dt, end_dt)
    assert len(result_df) == expected_count


# Tests for filter_by_time_df
@pytest.mark.parametrize(
    "period_mode, period_time, expected_count",
    [
        pytest.param(PeriodFilterMode.HOUR, 1, 1, id="hour"),
        pytest.param(PeriodFilterMode.MINUTE, 30, 1, id="minute"),
        pytest.param(PeriodFilterMode.SECOND, 3600, 1, id="second"),
        pytest.param(PeriodFilterMode.DAY, 1, 2, id="day"),
        pytest.param(PeriodFilterMode.TODAY, -1, 1, id="today"),
        pytest.param(PeriodFilterMode.YESTERDAY, -1, 2, id="yesterday"),
        pytest.param(PeriodFilterMode.WEEK, -1, 4, id="week"),
        pytest.param(PeriodFilterMode.MONTH, -1, 5, id="month"),
        pytest.param(PeriodFilterMode.YEAR, -1, 8, id="year"),
        pytest.param(PeriodFilterMode.TOTAL, -1, 11, id="total"),
    ],
)
def test_filter_by_time_df(period_mode, period_time, expected_count, mocker):
    """Test filtering dataframe by time period."""
    mocker.patch("src.stats.utils.get_today_midnight_dt", side_effect=mock_get_today_midnight_dt)
    mocker.patch("src.stats.utils.get_dt_now", side_effect=mock_get_dt_now)

    command_args = CommandArgs()
    command_args.period_mode = period_mode
    command_args.period_time = period_time

    result_df = filter_by_time_df(test_timestamp_df, command_args)
    assert len(result_df) == expected_count


def test_filter_by_time_df_date_mode(mocker):
    """Test filtering by specific date."""
    mocker.patch("src.stats.utils.get_today_midnight_dt", side_effect=mock_get_today_midnight_dt)
    mocker.patch("src.stats.utils.get_dt_now", side_effect=mock_get_dt_now)

    command_args = CommandArgs()
    command_args.period_mode = PeriodFilterMode.DATE
    command_args.dt = datetime(2023, 10, 9, 12, 0, 0).replace(tzinfo=ZoneInfo(TIMEZONE))

    result_df = filter_by_time_df(test_timestamp_df, command_args)
    assert len(result_df) == 2


def test_filter_by_time_df_date_range_mode(mocker):
    """Test filtering by date range."""
    mocker.patch("src.stats.utils.get_today_midnight_dt", side_effect=mock_get_today_midnight_dt)
    mocker.patch("src.stats.utils.get_dt_now", side_effect=mock_get_dt_now)

    command_args = CommandArgs()
    command_args.period_mode = PeriodFilterMode.DATE_RANGE
    command_args.start_dt = datetime(2023, 10, 8, 0, 0, 0).replace(tzinfo=ZoneInfo(TIMEZONE))
    command_args.end_dt = datetime(2023, 10, 9, 0, 0, 0).replace(tzinfo=ZoneInfo(TIMEZONE))
    command_args.dt_format = DatetimeFormat.DATE

    result_df = filter_by_time_df(test_timestamp_df, command_args)
    assert len(result_df) == 3


# Tests for filter_by_shifted_time_df
@pytest.mark.parametrize(
    "period_mode, period_time, expected_count",
    [
        pytest.param(PeriodFilterMode.HOUR, 1, 0, id="hour"),
        pytest.param(PeriodFilterMode.MINUTE, 30, 0, id="minute"),
        pytest.param(PeriodFilterMode.SECOND, 3600, 0, id="second"),
        pytest.param(PeriodFilterMode.TODAY, -1, 1, id="today"),
        pytest.param(PeriodFilterMode.YESTERDAY, -1, 1, id="yesterday"),
        pytest.param(PeriodFilterMode.WEEK, -1, 1, id="week"),
        pytest.param(PeriodFilterMode.MONTH, -1, 2, id="month"),
        pytest.param(PeriodFilterMode.YEAR, -1, 2, id="year"),
        pytest.param(PeriodFilterMode.TOTAL, -1, 11, id="total"),
    ],
)
def test_filter_by_shifted_time_df(period_mode, period_time, expected_count, mocker):
    """Test filtering dataframe by shifted time period."""
    mocker.patch("src.stats.utils.get_today_midnight_dt", side_effect=mock_get_today_midnight_dt)
    mocker.patch("src.stats.utils.get_dt_now", side_effect=mock_get_dt_now)

    command_args = CommandArgs()
    command_args.period_mode = period_mode
    command_args.period_time = period_time

    result_df = filter_by_shifted_time_df(test_timestamp_df, command_args)
    assert len(result_df) == expected_count


mock_chat_data = {"message_id": [1, 2, 3, 4, 5], "user_id": [100, 101, 102, 103, 104]}

# Mock BOT_ID
BOT_ID = 104


# Mock function to replace read_df
def mock_read_df(path):
    return pd.DataFrame(mock_chat_data)


# @pytest.mark.parametrize(
#     "message_ids, expected_result",
#     [
#         pytest.param([1, 2], False, id="non_bot_messages_present"),
#         pytest.param([5], True, id="only_bot_message"),
#         pytest.param([1, 5], False, id="mixed_messages"),
#         pytest.param([], True, id="empty_message_ids"),
#         pytest.param([6], False, id="message_id_not_in_chat"),
#         pytest.param([1, 2, 3, 4, 5], False, id="all_non_bot_messages"),
#         pytest.param([104], False, id="bot_id_as_message_id"),
#     ]
# )
# @patch('src.stats.utils.read_df', side_effect=mock_read_df)
# def test_check_bot_messages(mock_read_df, message_ids, expected_result):
#     result = check_bot_messages(message_ids, BOT_ID)
#     assert result == expected_result


# Tests for filter_emojis_by_emoji_type
@pytest.mark.parametrize(
    "emoji_type, expected_non_empty",
    [
        pytest.param(EmojiType.ALL, 5, id="all_emojis"),
        pytest.param(EmojiType.NEGATIVE, 1, id="negative_only"),
    ],
)
def test_filter_emojis_by_emoji_type(emoji_type, expected_non_empty):
    """Test filtering emojis by type."""
    df = mock_chat_df.copy()
    result_df = filter_emojis_by_emoji_type(df, emoji_type)
    non_empty = result_df[result_df["reaction_emojis"].apply(len) > 0]
    assert len(non_empty) == expected_non_empty


# Tests for filter_emoji_by_emoji_type
@pytest.mark.parametrize(
    "emoji_type, expected_count",
    [
        pytest.param(EmojiType.ALL, 4, id="all_emojis"),
        pytest.param(EmojiType.NEGATIVE, 2, id="negative_only"),
    ],
)
def test_filter_emoji_by_emoji_type(emoji_type, expected_count):
    """Test filtering emoji column by type."""
    df = mock_reactions_df.copy()
    result_df = filter_emoji_by_emoji_type(df, emoji_type)
    assert len(result_df) == expected_count


# Tests for dt_to_str
@pytest.mark.parametrize(
    "dt, expected",
    [
        pytest.param(datetime(2023, 10, 10, 14, 30, 0).replace(tzinfo=ZoneInfo(TIMEZONE)), "10-10-2023 14:30", id="afternoon"),
        pytest.param(datetime(2023, 1, 5, 9, 15, 0).replace(tzinfo=ZoneInfo(TIMEZONE)), "05-01-2023 09:15", id="morning"),
    ],
)
def test_dt_to_str(dt, expected):
    """Test converting datetime to string."""
    assert dt_to_str(dt) == expected


# Tests for check_new_username
@pytest.mark.parametrize(
    "new_username, should_succeed",
    [
        pytest.param("newuser", True, id="valid_new_username"),
        pytest.param("user1", False, id="duplicate_username"),
        pytest.param("use", False, id="similar_prefix"),
        pytest.param("test@user", False, id="invalid_characters"),
        pytest.param("today", False, id="forbidden_username"),
        pytest.param("alice_123", True, id="valid_with_underscore_numbers"),
    ],
)
def test_check_new_username(new_username, should_succeed):
    """Test validating new usernames."""
    is_valid, error = check_new_username(mock_users_df, new_username)
    assert is_valid == should_succeed
    if not should_succeed:
        assert error != ""


# Tests for are_text_characters_allowed
@pytest.mark.parametrize(
    "text, characters_filter, expected",
    [
        pytest.param("abc123", "abc123", True, id="all_allowed"),
        pytest.param("abc", "abcdef", True, id="subset_allowed"),
        pytest.param("abc@", "abc", False, id="invalid_char"),
        pytest.param("", "abc", True, id="empty_string"),
    ],
)
def test_are_text_characters_allowed(text, characters_filter, expected):
    """Test checking if text characters are allowed."""
    assert are_text_characters_allowed(text, characters_filter) == expected


# Tests for is_alpha_numeric
@pytest.mark.parametrize(
    "text, expected",
    [
        pytest.param("abc123", False, id="alphanumeric"),
        pytest.param("abc-123", True, id="with_dash"),
        pytest.param("abc 123", True, id="with_space"),
        pytest.param("abc@123", True, id="with_special"),
    ],
)
def test_is_alpha_numeric(text, expected):
    """Test checking if text has non-alphanumeric characters."""
    assert is_alpha_numeric(text) == expected


# Tests for enum_to_list
def test_enum_to_list():
    """Test converting enum to list."""
    result = enum_to_list(PeriodFilterMode)
    assert isinstance(result, list)
    assert len(result) > 0
    assert "today" in result


# Tests for get_forbidden_usernames
def test_get_forbidden_usernames():
    """Test getting forbidden usernames."""
    forbidden = get_forbidden_usernames()
    assert isinstance(forbidden, list)
    assert "today" in forbidden
    assert "week" in forbidden


# Tests for generate_random_file_id
def test_generate_random_file_id():
    """Test generating random file ID."""
    file_id1 = generate_random_file_id()
    file_id2 = generate_random_file_id()

    assert isinstance(file_id1, str)
    assert len(file_id1) == 11
    assert file_id1.isdigit()
    assert file_id1 != file_id2  # Should be different


# Tests for generate_random_filename
@pytest.mark.parametrize(
    "extension, expected_pattern",
    [
        pytest.param("jpg", r"^\d{11}\.jpg$", id="jpg_extension"),
        pytest.param("png", r"^\d{11}\.png$", id="png_extension"),
        pytest.param("txt", r"^\d{11}\.txt$", id="txt_extension"),
    ],
)
def test_generate_random_filename(extension, expected_pattern):
    """Test generating random filename."""
    import re

    filename = generate_random_filename(extension)
    assert re.match(expected_pattern, filename)


# Tests for username_to_user_id
def test_username_to_user_id():
    """Test converting username to user ID."""
    user_id = username_to_user_id(mock_users_df, "user1")
    assert user_id == 100


# Tests for is_list_column
def test_is_list_column():
    """Test checking if column contains lists."""
    list_series = pd.Series([[1, 2], [3, 4], [5]])
    non_list_series = pd.Series([1, 2, 3])

    assert is_list_column(list_series)
    assert not is_list_column(non_list_series)


# Tests for is_string_column
def test_is_string_column():
    """Test checking if column contains strings."""
    string_series = pd.Series(["a", "b", "c"])
    non_string_series = pd.Series([1, 2, 3])

    assert is_string_column(string_series)
    assert not is_string_column(non_string_series)


# Tests for validate_schema
def test_validate_schema(mocker):
    """Test validating dataframe schema."""
    mock_schema = MagicMock()
    mock_schema.name = "test_schema"
    df = pd.DataFrame({"col1": [1, 2, 3]})

    validate_schema(df, mock_schema)
    mock_schema.assert_called_once_with(df)


def test_validate_schema_empty_df(mocker):
    """Test validating empty dataframe."""
    mock_schema = MagicMock()
    mock_schema.name = "test_schema"
    df = pd.DataFrame()

    validate_schema(df, mock_schema)
    mock_schema.assert_not_called()


# Tests for get_last_message_id_of_a_user
@pytest.mark.parametrize(
    "user_id, expected_message_id, should_succeed",
    [
        pytest.param(100, 1, True, id="user_with_messages"),
        pytest.param(101, 2, True, id="another_user"),
        pytest.param(888, None, False, id="user_no_messages"),
    ],
)
def test_get_last_message_id_of_a_user(user_id, expected_message_id, should_succeed):
    """Test getting last message ID of a user."""
    message_id, error = get_last_message_id_of_a_user(mock_chat_df, user_id)

    if should_succeed:
        assert message_id == expected_message_id
        assert error == ""
    else:
        assert message_id is None
        assert error != ""


# Tests for text_to_word_length_sum
@pytest.mark.parametrize(
    "text, expected",
    [
        pytest.param("hello world", 10, id="two_words"),
        pytest.param("test", 4, id="single_word"),
        pytest.param("a b c", 3, id="single_chars"),
        pytest.param("", 0, id="empty_string"),
        pytest.param("  spaces  ", 6, id="with_spaces"),
    ],
)
def test_text_to_word_length_sum(text, expected):
    """Test summing word lengths in text."""
    assert text_to_word_length_sum(text) == expected


# Tests for get_users_map
def test_get_users_map():
    """Test creating users map."""
    users_map = get_users_map(mock_users_df)

    assert isinstance(users_map, dict)
    assert 0 in users_map
    assert users_map[0] == "user1"
    assert users_map[1] == "user2"
    assert users_map[2] == "user3"


# Tests for get_random_media_path
def test_get_random_media_path(mocker):
    """Test getting random media path."""
    mock_listdir = mocker.patch("os.listdir", return_value=["file1.jpg", "file2.png", "file3.mp4"])
    mocker.patch("random.choice", return_value="file2.png")

    result = get_random_media_path("/test/directory")

    mock_listdir.assert_called_once_with("/test/directory")
    assert result == os.path.join("/test/directory", "file2.png")


# Tests for chat ETL lock functions
def test_is_chat_etl_locked(mocker):
    """Test checking if chat ETL is locked."""
    mocker.patch("os.path.exists", return_value=True)
    assert is_chat_etl_locked()

    mocker.patch("os.path.exists", return_value=False)
    assert not is_chat_etl_locked()


def test_lock_chat_etl(mocker):
    """Test locking chat ETL."""
    mock_open = mocker.patch("builtins.open", mocker.mock_open())
    lock_chat_etl()
    mock_open.assert_called_once()


def test_remove_chat_etl_lock(mocker):
    """Test removing chat ETL lock."""
    mocker.patch("os.path.exists", return_value=True)
    mock_remove = mocker.patch("os.remove")

    remove_chat_etl_lock()
    mock_remove.assert_called_once()


# Tests for remove_diactric_accents
@pytest.mark.parametrize(
    "text, expected",
    [
        pytest.param("café", "cafe", id="french_accent"),
        pytest.param("naïve", "naive", id="diaeresis"),
        pytest.param("Zürich", "Zurich", id="umlaut"),
        pytest.param("hello", "hello", id="no_accents"),
        pytest.param("łódź", "lodz", id="polish_chars"),
    ],
)
def test_remove_diactric_accents(text, expected):
    """Test removing diacritic accents from text."""
    assert remove_diactric_accents(text) == expected
