import pandas as pd

from src.config.constants import TIMEZONE


def calculate_fun_metric(chat_df: pd.DataFrame, reactions_df: pd.DataFrame) -> pd.DataFrame:
    """Calculates the ratio of received reactions to sent messages per user."""
    reactions_received_counts = reactions_df.groupby("reacted_to_username").size().reset_index(name="reaction_count")
    message_counts = chat_df.groupby("final_username").size().reset_index(name="message_count")
    message_counts = message_counts[message_counts["message_count"] > 0]

    merged_df = reactions_received_counts.merge(
        message_counts, left_on="reacted_to_username", right_on="final_username", how="inner"
    ).fillna(0)
    merged_df["ratio"] = (merged_df["reaction_count"] / merged_df["message_count"]).round(2)
    return merged_df[["final_username", "ratio"]].copy().sort_values("ratio", ascending=False)


def calculate_wholesome_metric(reactions_df: pd.DataFrame) -> pd.DataFrame:
    """Calculates the ratio of given reactions to received reactions per user."""
    reactions_received_counts = reactions_df.groupby("reacted_to_username").size().reset_index(name="reactions_received_count")
    reactions_given_counts = reactions_df.groupby("reacting_username").size().reset_index(name="reactions_given_count")
    reactions_received_counts = reactions_received_counts[reactions_received_counts["reactions_received_count"] > 0]

    merged_df = reactions_received_counts.merge(
        reactions_given_counts, left_on="reacted_to_username", right_on="reacting_username", how="inner"
    ).fillna(0)
    merged_df["ratio"] = (merged_df["reactions_given_count"] / merged_df["reactions_received_count"]).round(2)
    return merged_df[["reacting_username", "ratio"]].copy().sort_values("ratio", ascending=False)


def calculate_monologue_index_metric_periodized(chat_df: pd.DataFrame, frequency: str = "D") -> pd.DataFrame:
    """Calculates periodized and accumulated word-to-message monologue index per user."""
    df = chat_df.copy()
    df["period"] = df["timestamp"].dt.to_period(frequency)
    df = df.sort_values("timestamp")

    df["word_count"] = df["text"].apply(lambda x: len(str(x).split()))
    user_stats = (
        df.groupby(["period", "final_username"]).agg(word_count=("word_count", "sum"), message_count=("text", "size")).reset_index()
    )
    user_stats[["word_count", "message_count"]] = user_stats[["word_count", "message_count"]].astype("int64")

    user_stats["word_count_acc"] = user_stats.groupby("final_username")["word_count"].cumsum()
    user_stats["message_count_acc"] = user_stats.groupby("final_username")["message_count"].cumsum()

    user_stats["monologue_index_periodized"] = (user_stats["word_count"] / user_stats["message_count"]).round(2)
    user_stats["monologue_index_acc"] = (user_stats["word_count_acc"] / user_stats["message_count_acc"]).round(2)

    user_stats["period"] = user_stats["period"].dt.to_timestamp()
    user_stats["temp_period"] = user_stats["period"]
    user_stats.index = user_stats["temp_period"]
    user_stats["period"] = pd.to_datetime(user_stats["period"]).dt.tz_localize(TIMEZONE)
    return user_stats.reset_index(drop=True)


def calculate_fun_metric_periodized(chat_df: pd.DataFrame, reactions_df: pd.DataFrame, frequency: str = "D") -> pd.DataFrame:
    """Calculates periodized reaction-to-message ratio per user."""
    c_df = chat_df.copy()
    r_df = reactions_df.copy()
    c_df["period"] = c_df["timestamp"].dt.to_period(frequency)
    r_df["period"] = r_df["timestamp"].dt.to_period(frequency)

    message_counts = c_df.groupby(["period", "final_username"]).size().reset_index(name="message_count")
    reaction_counts = r_df.groupby(["period", "reacted_to_username"]).size().reset_index(name="reaction_count")

    merged_df = pd.merge(
        message_counts, reaction_counts, left_on=["period", "final_username"], right_on=["period", "reacted_to_username"], how="inner"
    ).fillna(0)

    merged_df["ratio"] = (merged_df["reaction_count"] / merged_df["message_count"]).round(2)
    return merged_df[["period", "final_username", "ratio"]].sort_values(["period", "ratio"], ascending=[True, False])
