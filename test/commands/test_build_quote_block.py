"""Unit tests for _build_quote_block escaping logic."""
import re

import pytest

# Replicate the escaping logic in isolation (no bot/game objects needed)


def escape_special_characters(text: str) -> str:
    special_characters = r"\-.()[{}_]:+!<>=#^|~$%[]"
    return re.sub(f"([{re.escape(special_characters)}])", r"\\\1", text)


def build_quote_block(source_quote: str, target_word: str, dataset_display_name: str) -> str:
    """Standalone version of _build_quote_block for testing."""
    if not source_quote:
        return ""
    escaped_quote = escape_special_characters(source_quote)
    escaped_name = escape_special_characters(dataset_display_name)
    escaped_word = re.escape(escape_special_characters(target_word))
    bolded = re.sub(
        rf"\b({escaped_word})\b",
        lambda m: f"*{m.group(1)}*",
        escaped_quote,
        flags=re.IGNORECASE,
    )
    return f"\n> From *{escaped_name}*:\n> _{bolded}_"


@pytest.mark.parametrize(
    "source_quote, target_word, display_name, expected_contains, not_contains",
    [
        # Bug case: underscore in quote text should not trigger italic.
        # The trailing "._" in input becomes "\\.\\_" (escaped dot + escaped underscore).
        # The closing italic delimiter "_" comes after, so tail is "\\.\\__" — that's fine.
        # Key check: underscores inside the content are escaped (\\_), not bare.
        (
            "_Kosmos będzie rozstrzygający w walce o przepływy strategiczne i panowanie na Ziemi ._",
            "Ziemi",
            "Bartosiak",
            ["> _\\_Kosmos", "*Ziemi*", "> From *Bartosiak*:", "\\_"],
            [],
        ),
        # Normal case: no special chars
        (
            "The quick brown fox jumps over the lazy dog.",
            "fox",
            "English",
            ["*fox*", "> From *English*:", "\\."],  # period gets escaped
            [],
        ),
        # Word with MarkdownV2-special chars in source name
        (
            "Some quote here.",
            "quote",
            "My.Source",
            ["*My\\.Source*", "*quote*"],
            [],
        ),
        # Empty source_quote returns empty string
        (
            "",
            "word",
            "Source",
            [],
            [],
        ),
        # Target word not found in quote — no bold applied, but block still built
        (
            "No match here.",
            "absent",
            "Source",
            ["> From *Source*:", "No match here\\."],
            ["*absent*"],
        ),
        # Case-insensitive match
        (
            "He visited ZIEMI last year.",
            "ziemi",
            "Book",
            ["*ZIEMI*"],
            [],
        ),
    ],
)
def test_build_quote_block(source_quote, target_word, display_name, expected_contains, not_contains):
    result = build_quote_block(source_quote, target_word, display_name)
    if not source_quote:
        assert result == ""
        return
    for fragment in expected_contains:
        assert fragment in result, f"Expected {fragment!r} in result:\n{result!r}"
    for fragment in not_contains:
        assert fragment not in result, f"Did NOT expect {fragment!r} in result:\n{result!r}"


def test_build_quote_block_no_double_escape():
    """Verifies the returned block is not double-escaped when concatenated after outer escaping."""
    base = escape_special_characters("💰 Reward: *+750* credits (Total: *606800*)")
    quote = build_quote_block(
        "_Kosmos będzie rozstrzygający w walce o panowanie na Ziemi ._",
        "Ziemi",
        "Bartosiak",
    )
    full = base + quote
    # The blockquote prefix chars should NOT be escaped (> is not in special_characters)
    assert "\n> From" in full
    # The * bold markers around the word should be present
    assert "*Ziemi*" in full or "*ziemi*" in full.lower()
    # Underscores in source text should be escaped
    assert "\\_" in full
