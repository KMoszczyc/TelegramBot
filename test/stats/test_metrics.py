import pandas as pd
import pytest

from src.stats.metrics import (
    calculate_fun_metric,
    calculate_fun_metric_periodized,
    calculate_monologue_index_metric_periodized,
    calculate_wholesome_metric,
)

TIMEZONE = "Europe/Warsaw"

CHAT_DATA = [
    (1, pd.Timestamp("2025-01-10 10:00", tz=TIMEZONE), 111, "user_a", "hello world", "text"),
    (2, pd.Timestamp("2025-01-10 11:00", tz=TIMEZONE), 222, "user_b", "foo bar baz", "text"),
    (3, pd.Timestamp("2025-01-10 12:00", tz=TIMEZONE), 333, "user_c", "test message", "text"),
    (4, pd.Timestamp("2025-01-10 13:00", tz=TIMEZONE), 111, "user_a", "extra text", "text"),
    (5, pd.Timestamp("2025-01-11 09:00", tz=TIMEZONE), 111, "user_a", "morning message long text", "text"),
]
CHAT_COLS = ["message_id", "timestamp", "user_id", "final_username", "text", "message_type"]

REACTIONS_DATA = [
    (1, pd.Timestamp("2025-01-10 10:05", tz=TIMEZONE), "user_a", "user_b", "👍"),
    (1, pd.Timestamp("2025-01-10 10:06", tz=TIMEZONE), "user_a", "user_c", "😂"),
    (2, pd.Timestamp("2025-01-10 11:05", tz=TIMEZONE), "user_b", "user_a", "👍"),
    (4, pd.Timestamp("2025-01-10 13:05", tz=TIMEZONE), "user_a", "user_b", "❤️"),
    (5, pd.Timestamp("2025-01-11 09:05", tz=TIMEZONE), "user_a", "user_b", "👎"),
]
REACTIONS_COLS = ["message_id", "timestamp", "reacted_to_username", "reacting_username", "emoji"]


@pytest.fixture
def chat_df():
    return pd.DataFrame(CHAT_DATA, columns=CHAT_COLS)


@pytest.fixture
def reactions_df():
    return pd.DataFrame(REACTIONS_DATA, columns=REACTIONS_COLS)


@pytest.mark.parametrize(
    "expected_user_order",
    [
        ["user_a", "user_b"],
    ],
)
def test_calculate_fun_metric(chat_df, reactions_df, expected_user_order):
    result = calculate_fun_metric(chat_df, reactions_df)
    assert not result.empty
    assert "ratio" in result.columns
    assert "final_username" in result.columns
    assert list(result["final_username"].values) == expected_user_order
    # user_a: 4 reactions / 3 msgs = 1.33
    # user_b: 1 reaction / 1 msg = 1.00
    ratios = dict(zip(result["final_username"], result["ratio"], strict=False))
    assert ratios["user_a"] == 1.33
    assert ratios["user_b"] == 1.0


@pytest.mark.parametrize(
    "min_ratio",
    [0.0],
)
def test_calculate_wholesome_metric(reactions_df, min_ratio):
    result = calculate_wholesome_metric(reactions_df)
    assert not result.empty
    assert "ratio" in result.columns
    assert "reacting_username" in result.columns
    assert (result["ratio"] >= min_ratio).all()
    # user_b gave 3 reactions, received 1 -> ratio 3.0
    ratios = dict(zip(result["reacting_username"], result["ratio"], strict=False))
    assert ratios["user_b"] == 3.0


@pytest.mark.parametrize("frequency", ["D", "W"])
def test_calculate_monologue_index_metric_periodized(chat_df, frequency):
    result = calculate_monologue_index_metric_periodized(chat_df, frequency=frequency)
    assert not result.empty
    assert "monologue_index_periodized" in result.columns
    assert "monologue_index_acc" in result.columns
    assert "word_count_acc" in result.columns


@pytest.mark.parametrize("frequency", ["D"])
def test_calculate_fun_metric_periodized(chat_df, reactions_df, frequency):
    result = calculate_fun_metric_periodized(chat_df, reactions_df, frequency=frequency)
    assert not result.empty
    assert "period" in result.columns
    assert "ratio" in result.columns
    assert "final_username" in result.columns
