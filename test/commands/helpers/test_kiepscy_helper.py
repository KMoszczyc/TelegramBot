import pandas as pd
import pytest

from src.commands.helpers.kiepscy import format_kiepscy_messages, search_kiepscy_episodes


@pytest.fixture
def sample_kiepscy_df():
    return pd.DataFrame(
        [
            {"nr": "1", "title": "Umarł odbiornik", "description": "Ferdek ogląda telewizor"},
            {"nr": "2", "title": "Wiara czyni cuda", "description": "Boczek przynosi wiarę"},
            {"nr": "—", "title": "Specjalny odcinek", "description": "Odcinek bez numeru"},
        ]
    )


@pytest.mark.parametrize(
    "search_phrase,search_phrases,expected_count,expected_first_title",
    [
        ("telewizor", [], 1, "Umarł odbiornik"),
        ("Boczek", [], 1, "Wiara czyni cuda"),
        ("", ["Umarł", "odbiornik"], 1, "Umarł odbiornik"),
        ("nonexistent", [], 0, None),
    ],
)
def test_search_kiepscy_episodes(sample_kiepscy_df, search_phrase, search_phrases, expected_count, expected_first_title):
    res = search_kiepscy_episodes(sample_kiepscy_df, search_phrase, search_phrases)
    assert len(res) == expected_count
    if expected_first_title:
        assert res.iloc[0]["title"] == expected_first_title


def test_format_kiepscy_messages(sample_kiepscy_df):
    messages = format_kiepscy_messages(sample_kiepscy_df, "test")
    assert len(messages) >= 1
    assert "Kiepscy episodes that match" in messages[0]
    assert "Umarł odbiornik" in messages[0]
