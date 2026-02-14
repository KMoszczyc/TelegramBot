import os
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]

RUNTIME_ENV = os.getenv("RUNTIME_ENV")

DATA_DIR = Path("/data") if RUNTIME_ENV == "docker" else ROOT_DIR / "data"
TEMP_DIR = DATA_DIR / "temp"

DB_PATH = DATA_DIR / "bot.db"
DB_SCHEMA_SQL_PATH = ROOT_DIR / "src" / "models" / "db" / "schema.sql"

# Chat data
CHAT_ETL_LOCK_PATH = DATA_DIR / "chat" / "chat_etl.lock"
METADATA_PATH = DATA_DIR / "chat" / "metadata.pickle"
UPDATE_REQUIRED_PATH = DATA_DIR / "chat" / "update_required.lock"
CHAT_HISTORY_PATH = DATA_DIR / "chat" / "chat_history.parquet"
CHAT_IMAGES_DIR_PATH = DATA_DIR / "chat" / "images"
CHAT_GIFS_DIR_PATH = DATA_DIR / "chat" / "gifs"
CHAT_VIDEOS_DIR_PATH = DATA_DIR / "chat" / "videos"
CHAT_VIDEO_NOTES_DIR_PATH = DATA_DIR / "chat" / "video_notes"
CHAT_AUDIO_DIR_PATH = DATA_DIR / "chat" / "audio"
CHAT_WORD_STATS_DIR_PATH = DATA_DIR / "chat" / "word_stats"
WORD_STATS_UPDATE_LOCK_PATH = CHAT_WORD_STATS_DIR_PATH / "update.lock"

CLEANED_CHAT_HISTORY_PATH = DATA_DIR / "chat" / "cleaned_chat_history.parquet"
REACTIONS_PATH = DATA_DIR / "chat" / "reactions.parquet"
USERS_PATH = DATA_DIR / "chat" / "users.parquet"
COMMANDS_USAGE_PATH = DATA_DIR / "chat" / "commands_usage.parquet"
SCHEDULED_JOBS_PATH = DATA_DIR / "chat" / "scheduled_jobs.pkl"
CWEL_STATS_PATH = DATA_DIR / "chat" / "cwel_stats.parquet"
CREDITS_PATH = DATA_DIR / "chat" / "credits.pkl"
CREDIT_HISTORY_PATH = DATA_DIR / "chat" / "credit_history.parquet"

# Miscellaneous
TVP_HEADLINES_PATH = DATA_DIR / "misc" / "paski-tvp.txt"
TVP_LATEST_HEADLINES_PATH = DATA_DIR / "misc" / "tvp_latest_headlines.txt"
OZJASZ_PHRASES_PATH = DATA_DIR / "misc" / "ozjasz-wypowiedzi.txt"
POLISH_STOPWORDS_PATH = DATA_DIR / "misc" / "polish.stopwords.txt"
BARTOSIAK_PATH = DATA_DIR / "misc" / "bartosiak.txt"
COMMANDS_PATH = DATA_DIR / "misc" / "commands.txt"
ARGUMENTS_HELP_PATH = DATA_DIR / "misc" / "arguments_help.txt"
BIBLE_PATH = DATA_DIR / "misc" / "bible.parquet"
QURAN_PATH = DATA_DIR / "misc" / "quran.parquet"
SHOPPING_SUNDAYS_PATH = DATA_DIR / "misc" / "niedziele.txt"
EUROPEJSKAFIRMA_PATH = DATA_DIR / "misc" / "europejskafirma.txt"
BOCZEK_PATH = DATA_DIR / "misc" / "boczek.txt"
KIEPSCY_PATH = DATA_DIR / "misc" / "kiepscy.parquet"
WALESA_PATH = DATA_DIR / "misc" / "walesa.txt"
QUIZ_DATABASE_PATH = DATA_DIR / "misc" / "quiz_database.parquet"
POLISH_HOLIDAYS_PATH = DATA_DIR / "misc" / "polish_holidays.csv"
