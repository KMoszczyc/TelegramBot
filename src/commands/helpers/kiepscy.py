import pandas as pd

import src.core.utils as core_utils
import src.stats.utils as stats_utils
from src.config.constants import LONG_MESSAGE_LIMIT


def search_kiepscy_episodes(kiepscy_df: pd.DataFrame, search_phrase: str, search_phrases: list[str]) -> pd.DataFrame:
    """Searches Kiepscy episodes by title and description matching query phrases."""
    if search_phrases:
        regex = core_utils.regexify_multiword_filter(search_phrases)
        matching_by_title_df = kiepscy_df[kiepscy_df["title"].str.contains(regex, case=False)]
        matching_by_description_df = kiepscy_df[kiepscy_df["description"].str.contains(regex, case=False)]
    else:
        matching_by_title_df = kiepscy_df[kiepscy_df["title"].str.contains(search_phrase, case=False)]
        matching_by_description_df = kiepscy_df[kiepscy_df["description"].str.contains(search_phrase, case=False)]

    merged_df = pd.concat([matching_by_title_df, matching_by_description_df], ignore_index=True)
    merged_df["nr"] = merged_df["nr"].replace("—", "999").astype(int)
    return merged_df.drop_duplicates("nr").sort_values("nr").reset_index(drop=True)


def format_kiepscy_messages(merged_df: pd.DataFrame, search_phrase: str) -> list[str]:
    """Paginates matched Kiepscy episodes into Telegram-compatible messages (<=4096 chars)."""
    text = f"Kiepscy episodes that match [{search_phrase}]:\n"
    last_text = text
    messages = []

    for _, row in merged_df.iterrows():
        text += f"- *{row['nr']}: {row['title']}* - {row['description']}\n"
        if len(messages) >= LONG_MESSAGE_LIMIT:
            return messages
        if len(text) > 4096:
            messages.append(stats_utils.escape_special_characters(last_text))
            text = ""
        last_text = text

    if text:
        messages.append(stats_utils.escape_special_characters(text))
    return messages
