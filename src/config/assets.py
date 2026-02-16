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


class Assets:
    def __init__(self):
        self.tvp_headlines = self.read_str_file(str(TVP_HEADLINES_PATH))
        self.tvp_latest_headlines = self.read_str_file(str(TVP_LATEST_HEADLINES_PATH))
        self.ozjasz_phrases = self.read_str_file(str(OZJASZ_PHRASES_PATH))
        self.bartosiak_phrases = self.read_str_file(str(BARTOSIAK_PATH))
        self.commands = self.read_str_file(str(COMMANDS_PATH))
        self.arguments_help = self.read_str_file(str(ARGUMENTS_HELP_PATH))
        self.bible_df = pd.read_parquet(str(BIBLE_PATH))
        self.quran_df = pd.read_parquet(str(QURAN_PATH))
        self.shopping_sundays = self.read_str_file(str(SHOPPING_SUNDAYS_PATH))
        self.europejskafirma_phrases = self.read_str_file(str(EUROPEJSKAFIRMA_PATH))
        self.boczek_phrases = self.read_str_file(str(BOCZEK_PATH))
        self.kiepscy_df = pd.read_parquet(str(KIEPSCY_PATH))
        self.walesa_phrases = self.read_str_file(str(WALESA_PATH))
        self.polish_stopwords = self.read_str_file(str(POLISH_STOPWORDS_PATH))
        self.quiz_df = pd.read_parquet(str(QUIZ_DATABASE_PATH))
        self.polish_holidays_df = pd.read_csv(str(POLISH_HOLIDAYS_PATH), sep=";")

    def read_str_file(self, path):
        with open(path) as f:
            lines = f.read().splitlines()
        return lines
