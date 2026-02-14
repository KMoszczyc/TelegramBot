import pandas as pd

from src.config.paths import (
    ARGUMENTS_HELP_PATH,
    BARTOSIAK_PATH,
    BIBLE_PATH,
    BOCZEK_PATH,
    COMMANDS_PATH,
    EUROPEJSKAFIRMA_PATH,
    KIEPSCY_PATH,
    OZJASZ_PHRASES_PATH,
    POLISH_HOLIDAYS_PATH,
    POLISH_STOPWORDS_PATH,
    QUIZ_DATABASE_PATH,
    QURAN_PATH,
    SHOPPING_SUNDAYS_PATH,
    TVP_HEADLINES_PATH,
    TVP_LATEST_HEADLINES_PATH,
    WALESA_PATH,
)


def read_str_file(path):
    try:
        with open(path) as f:
            lines = f.read().splitlines()
        return lines
    except OSError:
        return []


tvp_headlines = read_str_file(str(TVP_HEADLINES_PATH))
tvp_latest_headlines = read_str_file(str(TVP_LATEST_HEADLINES_PATH))
ozjasz_phrases = read_str_file(str(OZJASZ_PHRASES_PATH))
bartosiak_phrases = read_str_file(str(BARTOSIAK_PATH))
commands = read_str_file(str(COMMANDS_PATH))
arguments_help = read_str_file(str(ARGUMENTS_HELP_PATH))
bible_df = pd.read_parquet(str(BIBLE_PATH))
quran_df = pd.read_parquet(str(QURAN_PATH))
shopping_sundays = read_str_file(str(SHOPPING_SUNDAYS_PATH))
europejskafirma_phrases = read_str_file(str(EUROPEJSKAFIRMA_PATH))
boczek_phrases = read_str_file(str(BOCZEK_PATH))
kiepscy_df = pd.read_parquet(str(KIEPSCY_PATH))
walesa_phrases = read_str_file(str(WALESA_PATH))
polish_stopwords = read_str_file(str(POLISH_STOPWORDS_PATH))
quiz_df = pd.read_parquet(str(QUIZ_DATABASE_PATH))
polish_holidays_df = pd.read_csv(str(POLISH_HOLIDAYS_PATH), sep=";")
