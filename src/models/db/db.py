import json
import logging
import sqlite3

import pandas as pd

import src.core.utils as core_utils
from src.config.constants import TIMEZONE
from src.config.enums import DBSaveMode, Table
from src.config.paths import (
    CHAT_HISTORY_PATH,
    CLEANED_CHAT_HISTORY_PATH,
    COMMANDS_USAGE_PATH,
    CREDIT_HISTORY_PATH,
    CWEL_STATS_PATH,
    DB_PATH,
    DB_SCHEMA_SQL_PATH,
    REACTIONS_PATH,
    USERS_PATH,
)
from src.models.credits import Credits

log = logging.getLogger(__name__)


class DB:
    """SQLite database manager for the Telegram bot.

    This class handles database initialization, table creation, data migration from parquet files,
    and provides serialization/deserialization methods for complex data types.

    The class manages the transition from parquet-based storage to SQLite, providing
    methods to handle lists, datetimes, and boolean values that need special treatment
    when stored in a relational database.

    Attributes:
        conn (sqlite3.Connection): The SQLite database connection with autocommit disabled
    """

    def __init__(self) -> None:
        """Initialize the database connection and create tables.

        Establishes a SQLite connection with a 30-second timeout and no isolation level
        (autocommit mode), then creates all necessary tables from the schema file.
        """
        self.conn = self.init_db()
        self.create_tables()
        # self.migrate()

    def init_db(self) -> sqlite3.Connection:
        """Initialize and return a SQLite database connection.

        Returns:
            sqlite3.Connection: Database connection with 30s timeout and autocommit mode
        """
        return sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)

    def create_tables(self) -> None:
        """Create database tables from the SQL schema file.

        Reads the schema SQL file and executes it to create all necessary tables.
        Commits the changes after execution.
        """
        with open(DB_SCHEMA_SQL_PATH) as schema_file:
            schema_sql = schema_file.read()

        self.conn.executescript(schema_sql)
        self.conn.commit()

    def serialize_lists(self, df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
        """Convert list columns to JSON strings for database storage.

        Args:
            df: DataFrame containing list columns to serialize
            columns: List of column names containing list data

        Returns:
            DataFrame with list columns converted to JSON strings
        """
        for col in columns:
            df[col] = df[col].apply(core_utils.safe_json_dump)
        return df

    def serialize_datetimes(self, df: pd.DataFrame, datetime_columns: list[str]) -> pd.DataFrame:
        """Convert datetime columns to ISO format strings for database storage.

        Args:
            df: DataFrame containing datetime columns to serialize
            datetime_columns: List of column names containing datetime data

        Returns:
            DataFrame with datetime columns converted to ISO format strings
        """
        for col in datetime_columns:
            df[col] = df[col].apply(lambda x: x.isoformat() if pd.notna(x) else None)
        return df

    def bool_to_int(self, df: pd.DataFrame, column: str) -> pd.DataFrame:
        """Convert boolean column to integer for database storage.

        Fills NaN values with False, then converts boolean values to integers (0/1).

        Args:
            df: DataFrame containing boolean column to convert
            column: Name of the boolean column

        Returns:
            DataFrame with boolean column converted to integers
        """
        df[column] = df[column].fillna(False)
        df[column] = df[column].astype(int)
        return df

    def deserialize_lists(self, df: pd.DataFrame, list_columns: list[str]) -> pd.DataFrame:
        """Convert JSON string columns back to Python lists.

        Args:
            df: DataFrame containing JSON string columns to deserialize
            list_columns: List of column names containing JSON-encoded lists

        Returns:
            DataFrame with JSON columns converted back to Python lists
        """
        for col in list_columns:
            if col not in df.columns:
                continue
            df[col] = df[col].apply(json.loads)
        return df

    def deserialize_datetimes(self, df: pd.DataFrame, datetime_columns: list[str]) -> pd.DataFrame:
        """Convert ISO string columns back to datetime objects with timezone.

        Args:
            df: DataFrame containing datetime string columns to deserialize
            datetime_columns: List of column names containing ISO datetime strings

        Returns:
            DataFrame with string columns converted to timezone-aware datetime objects
        """
        for col in datetime_columns:
            if col not in df.columns:
                continue
            df[col] = pd.to_datetime(df[col], utc=True).dt.tz_convert(TIMEZONE).astype(f"datetime64[ns, {TIMEZONE}]")
        return df

    def deserialize_bools(self, df: pd.DataFrame, bool_columns: list[str]) -> pd.DataFrame:
        """Convert integer columns back to boolean values.

        Args:
            df: DataFrame containing integer columns to convert to boolean
            bool_columns: List of column names containing boolean values as integers

        Returns:
            DataFrame with integer columns converted to boolean values
        """
        for col in bool_columns:
            if col not in df.columns:
                continue
            df[col] = df[col].astype(bool)
        return df

    def migrate(self) -> None:
        """Migrate all parquet data to SQLite database.

        Reads data from all parquet files and the credits pickle file,
        then saves each DataFrame to its corresponding SQLite table using REPLACE mode.
        This method serves as the one-time migration from parquet-based to SQLite-based storage.
        """
        chat_df = core_utils.read_df(CHAT_HISTORY_PATH)
        cleaned_chat_df = core_utils.read_df(CLEANED_CHAT_HISTORY_PATH)
        reactions_df = core_utils.read_df(REACTIONS_PATH)
        users_df = core_utils.read_df(USERS_PATH)
        command_usage_df = core_utils.read_df(COMMANDS_USAGE_PATH)
        credit_history_df = core_utils.read_df(CREDIT_HISTORY_PATH)
        cwel_df = core_utils.read_df(CWEL_STATS_PATH)
        cwel_df = (
            cwel_df.drop_duplicates(
                subset=["timestamp", "receiver_username", "giver_username", "reply_message_id"], keep="last"
            ).reset_index(drop=True)
            if cwel_df is not None
            else None
        )
        credits_df = pd.DataFrame(Credits(self).credits.items(), columns=["user_id", "credits"])

        table_to_df_map = {
            Table.CHAT_HISTORY: chat_df,
            Table.CLEANED_CHAT_HISTORY: cleaned_chat_df,
            Table.REACTIONS: reactions_df,
            Table.USERS: users_df,
            Table.COMMANDS_USAGE: command_usage_df,
            Table.CREDIT_HISTORY: credit_history_df,
            Table.CWEL: cwel_df,
            Table.CREDITS: credits_df,
        }

        for table, df in table_to_df_map.items():
            self.save_dataframe(df, table, DBSaveMode.REPLACE)

    def save_dataframe(self, df: pd.DataFrame, table: Table, mode: DBSaveMode = DBSaveMode.APPEND) -> None:
        """Save a DataFrame to a SQLite table with appropriate serialization.

        Applies table-specific serialization logic for lists, datetimes, and booleans,
        then saves the data to the specified table using the given mode.

        Args:
            df: DataFrame to save to the database
            table: Target table enum value
            mode: Save mode (APPEND, REPLACE, or FAIL)
        """
        if df is None or df.empty:
            log.info(f"No data found for table {table}, skipping.")
            return
        df_copy = df.copy(deep=True)

        match table:
            case Table.CHAT_HISTORY | Table.CLEANED_CHAT_HISTORY:
                df_copy = self.serialize_lists(df_copy, ["reaction_emojis", "reaction_user_ids"])
                df_copy = self.serialize_datetimes(df_copy, ["timestamp"])
            case Table.USERS:
                df_copy = self.serialize_lists(df_copy, ["nicknames"])
            case Table.REACTIONS | Table.COMMANDS_USAGE | Table.CWEL | Table.CREDIT_HISTORY:
                df_copy = self.serialize_datetimes(df_copy, ["timestamp"])
                if table == Table.CREDIT_HISTORY:
                    df_copy = self.bool_to_int(df_copy, "success")
            case _:
                # default: no special handling
                pass
        write_index = table == Table.USERS

        # Write to SQLite
        before_count = self.count_rows(table)
        if mode == DBSaveMode.REPLACE:
            self.conn.execute(f"DELETE FROM {table.value}")
            df_copy.to_sql(table.value, self.conn, if_exists="append", index=write_index, chunksize=1000, method="multi")
            self.conn.commit()
        elif mode == DBSaveMode.APPEND:
            self.insert_ignore_duplicates(df_copy, table)
        after_count = self.count_rows(table)
        log.info(f"Added {after_count - before_count} rows to {table.value} table in {mode.value} mode. Currently at: {after_count} rows.")

    def count_rows(self, table: Table):
        return self.conn.execute(f"SELECT COUNT(*) FROM {table.value}").fetchone()[0]

    def insert_ignore_duplicates(self, df: pd.DataFrame, table: Table) -> None:
        """Insert records into the specified table, ignoring duplicates based on the primary key.

        Uses INSERT OR IGNORE to prevent duplicate key violations.

        Args:
            df: DataFrame containing records to insert
            table: Name of the target table
        """
        cols = ", ".join(df.columns)
        placeholders = ", ".join("?" for _ in df.columns)

        sql = f"""
            INSERT OR IGNORE INTO {table.value} ({cols})
            VALUES ({placeholders})
        """

        self.conn.executemany(sql, df.itertuples(index=False, name=None))
        self.conn.commit()

    def load_table(self, table: Table) -> pd.DataFrame:
        """Load all data from a table into a DataFrame with appropriate deserialization.

        Reads the entire table and applies deserialization for lists, datetimes,
        and booleans to restore the original data types.

        Args:
            table: Table enum value to load

        Returns:
            DataFrame with properly deserialized data types
        """
        df = pd.read_sql_query(f"SELECT * FROM {table.value}", self.conn)
        df = self.deserialize_lists(df, ["reaction_emojis", "reaction_user_ids", "nicknames"])
        df = self.deserialize_datetimes(df, ["timestamp"])
        df = self.deserialize_bools(df, ["success"])

        if table == Table.USERS:
            df = df.set_index("user_id")

        return df
