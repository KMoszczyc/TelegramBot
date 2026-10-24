import logging

import pandas as pd

import src.core.utils as core_utils
import src.stats.metrics as stats_metrics
import src.stats.utils as stats_utils
from src.config.enums import EmojiType
from src.models.data.command_args import CommandArgs
from src.stats import charts

log = logging.getLogger(__name__)


def build_chat_user_stats(chat_df: pd.DataFrame, reactions_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Computes aggregated user statistics, fun metric, and wholesome metric."""
    df = chat_df.copy()
    df["word_count"] = df["text"].apply(lambda x: len(str(x).split()))
    df["word_length"] = df["text"].apply(lambda x: stats_utils.text_to_word_length_sum(x))

    user_stats = (
        df.groupby("final_username")
        .agg(word_count=("word_count", "sum"), word_length=("word_length", "sum"), message_count=("text", "size"))
        .reset_index()
    )
    numeric_cols = ["word_count", "word_length", "message_count"]
    user_stats[numeric_cols] = user_stats[numeric_cols].apply(pd.to_numeric).fillna(0).astype("int64")

    fun_metric = stats_metrics.calculate_fun_metric(chat_df, reactions_df)
    wholesome_metric = stats_metrics.calculate_wholesome_metric(reactions_df)

    user_stats["monologue_ratio"] = (user_stats["word_count"] / user_stats["message_count"]).round(2)
    user_stats["avg_word_length"] = (user_stats["word_length"] / user_stats["word_count"]).round(2)
    return user_stats, fun_metric, wholesome_metric


def build_summary_rows(
    chat_df: pd.DataFrame,
    reactions_df: pd.DataFrame,
    user_stats: pd.DataFrame,
    fun_metric: pd.DataFrame,
    wholesome_metric: pd.DataFrame,
    display_count: int,
) -> list[list[str]]:
    """Builds formatted HTML table rows for the summary chart."""
    sad_reactions_df = stats_utils.filter_emoji_by_emoji_type(reactions_df, EmojiType.NEGATIVE, "emoji")

    reactions_received_counts = (
        reactions_df.groupby("reacted_to_username").size().reset_index(name="count").sort_values("count", ascending=False)
    )
    reactions_given_counts = (
        reactions_df.groupby("reacting_username").size().reset_index(name="count").sort_values("count", ascending=False)
    )
    sad_reactions_received_counts = (
        sad_reactions_df.groupby("reacted_to_username").size().reset_index(name="count").sort_values("count", ascending=False)
    )
    sad_reactions_given_counts = (
        sad_reactions_df.groupby("reacting_username").size().reset_index(name="count").sort_values("count", ascending=False)
    )

    def fmt(df, name_col, val_col):
        return [f"{r[name_col]}: <b>{r[val_col]}</b>" for _, r in df.head(display_count).iterrows()]

    return [
        ["<b>Top spammer</b>", *fmt(user_stats.sort_values("message_count", ascending=False), "final_username", "message_count")],
        ["<b>Word count</b>", *fmt(user_stats.sort_values("word_count", ascending=False), "final_username", "word_count")],
        [
            "<b>Monologue index</b>",
            *fmt(user_stats.sort_values("monologue_ratio", ascending=False), "final_username", "monologue_ratio"),
        ],
        ["<b>Elaborateness</b>", *fmt(user_stats.sort_values("avg_word_length", ascending=False), "final_username", "avg_word_length")],
        ["<b>Fun</b>", *fmt(fun_metric, "final_username", "ratio")],
        ["<b>Wholesome</b>", *fmt(wholesome_metric, "reacting_username", "ratio")],
        ["<b>Unwholesome</b>", *fmt(wholesome_metric.sort_values("ratio", ascending=True), "reacting_username", "ratio")],
        ["<b>Most liked</b>", *fmt(reactions_received_counts, "reacted_to_username", "count")],
        ["<b>Most liking</b>", *fmt(reactions_given_counts, "reacting_username", "count")],
        ["<b>Most disliked</b>", *fmt(sad_reactions_received_counts, "reacted_to_username", "count")],
        ["<b>Most disliking</b>", *fmt(sad_reactions_given_counts, "reacting_username", "count")],
    ]


def build_summary_footnotes(
    chat_df: pd.DataFrame,
    reactions_df: pd.DataFrame,
    shifted_chat_df: pd.DataFrame,
    shifted_reactions_df: pd.DataFrame,
) -> str:
    """Formats summary footnotes including message/reaction volume changes and top message."""
    images_num = len(chat_df[chat_df["message_type"] == "image"])
    text_only_chat_df = chat_df[chat_df["text"] != ""]

    message_count_change = 0 if shifted_chat_df.empty else round((len(chat_df) - len(shifted_chat_df)) / len(shifted_chat_df) * 100, 1)
    reaction_count_change = (
        0 if shifted_reactions_df.empty else round((len(reactions_df) - len(shifted_reactions_df)) / len(shifted_reactions_df) * 100, 1)
    )
    message_count_change_text = f"+{message_count_change}%" if message_count_change > 0 else f"{message_count_change}%"
    reaction_count_change_text = f"+{reaction_count_change}%" if reaction_count_change > 0 else f"{reaction_count_change}%"

    top_msg = ", ".join(
        f"{r['final_username']} [{stats_utils.dt_to_str(r['timestamp'])}]: {r['text']} [{''.join(r['reaction_emojis'])}]"
        for _, r in text_only_chat_df.head(1).iterrows()
    )
    footnotes = [
        f"Total: {len(chat_df)} ({message_count_change_text}) messages, {len(reactions_df)} ({reaction_count_change_text}) reactions and {images_num} images",
        f"Top message: {top_msg}",
    ]
    return "\n".join(footnotes)


def generate_chat_summary(
    chat_df: pd.DataFrame,
    reactions_df: pd.DataFrame,
    shifted_chat_df: pd.DataFrame,
    shifted_reactions_df: pd.DataFrame,
    command_args: CommandArgs,
    display_count: int,
) -> tuple[str, str]:
    """Generates the chat summary footnote text and table image path."""
    user_stats, fun_metric, wholesome_metric = build_chat_user_stats(chat_df, reactions_df)
    log.info(user_stats.head(100))

    rows = build_summary_rows(chat_df, reactions_df, user_stats, fun_metric, wholesome_metric, display_count)
    send_msg = build_summary_footnotes(chat_df, reactions_df, shifted_chat_df, shifted_reactions_df)

    longest_row_count = max(len(row) for row in rows) - 1
    col_count = min(longest_row_count, display_count)
    columns = [f"<b>{core_utils.generate_period_headline(command_args)}</b>", *[f"<b>TOP{i + 1}</b>" for i in range(col_count)]]

    summary_df = pd.DataFrame(rows, columns=columns)
    path = charts.create_table_plotly(summary_df, command_args=command_args, columns=columns)
    return send_msg, path
