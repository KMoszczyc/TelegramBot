from unittest.mock import MagicMock

import pandas as pd
import pytest

from src.commands.helpers.holy_text import handle_holy_text_named_params
from src.config.enums import ErrorMessage, HolyTextType


@pytest.fixture
def sample_holy_df():
    return pd.DataFrame(
        [
            {"book": "Rdz", "abbreviation": "Rdz", "chapter": 1, "verse": 1, "text": "Na początku było Słowo"},
            {"book": "Rdz", "abbreviation": "Rdz", "chapter": 1, "verse": 2, "text": "A ziemia była bezładem"},
            {"book": "Rdz", "abbreviation": "Rdz", "chapter": 1, "verse": 3, "text": "I rzekł Bóg niech stanie się światłość"},
        ]
    )


@pytest.fixture
def mock_bot_state():
    state = MagicMock()
    state.last_bible_verse_id = -1
    state.last_quran_verse_id = -1
    return state


def test_handle_holy_text_empty(sample_holy_df, mock_bot_state):
    ca = MagicMock(named_args={})
    resp, err = handle_holy_text_named_params(ca, sample_holy_df.iloc[0:0], sample_holy_df, mock_bot_state, "x", HolyTextType.BIBLE)
    assert resp == ErrorMessage.NO_SUCH_VERSE
    assert err == ""


@pytest.mark.parametrize(
    "named_args,expected_in_resp",
    [
        ({"num": 2}, "2 bible verses"),
        ({"all": True}, "3 bible verses"),
        ({"count": True}, "3 bible verses with"),
    ],
)
def test_handle_holy_text_named_args(sample_holy_df, mock_bot_state, named_args, expected_in_resp):
    ca = MagicMock(named_args=named_args)
    resp, err = handle_holy_text_named_params(ca, sample_holy_df, sample_holy_df, mock_bot_state, "test", HolyTextType.BIBLE)
    assert expected_in_resp in resp
    assert err == ""
