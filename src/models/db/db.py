import json
import sqlite3
from zoneinfo import ZoneInfo

import pandas as pd
from definitions import DB_PATH, CLEANED_CHAT_HISTORY_PATH, REACTIONS_PATH, USERS_PATH, CHAT_HISTORY_PATH, COMMANDS_USAGE_PATH, \
    CREDIT_HISTORY_PATH, DB_SCHEMA_SQL_PATH, CWEL_STATS_PATH, Table, TIMEZONE
import src.stats.utils as stats_utils
from src.models.credits import Credits
from src.stats.word_stats import WordStats
import src.core.utils as core_utils


class DB:
    def __init__(self):
        self.conn = self.init_db()
        self.create_tables()
        self.migrate()

    def init_db(self):
        return sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)

    def create_tables(self):
        with open(DB_SCHEMA_SQL_PATH, 'r') as schema_file:
            schema_sql = schema_file.read()
        self.conn.executescript(schema_sql)
        self.conn.commit()

    def serialize_lists(self, df, columns):
        for col in columns:
            # print(df[col].tolist())
            df[col] = df[col].apply(core_utils.safe_json_dump)
        return df

    def serialize_datetimes(self, df, datetime_columns):
        for col in datetime_columns:
            df[col] = df[col].apply(lambda x: x.isoformat() if pd.notna(x) else None)
        return df

    def bool_to_int(self, df, column):
        df[column] = df[column].fillna(False)
        df[column] = df[column].astype(int)
        return df

    def deserialize_lists(self, df, list_columns):
        for col in list_columns:
            if col not in df.columns:
                continue
            df[col] = df[col].apply(json.loads)
        return df

    def deserialize_datetimes(self, df, datetime_columns):
        for col in datetime_columns:
            if col not in df.columns:
                continue
            print('we deserializin datetimes baby')
            df[col] = pd.to_datetime(df[col])
        return df

    def deserialize_bools(self, df, bool_columns):
        for col in bool_columns:
            if col not in df.columns:
                continue
            df[col] = df[col].astype(bool)
        return df

    def migrate(self):
        """igrate DataFrames to SQLite tables using table-specific logic via match/case."""
        chat_df = stats_utils.read_df(CHAT_HISTORY_PATH)
        cleaned_chat_df = stats_utils.read_df(CLEANED_CHAT_HISTORY_PATH)
        reactions_df = stats_utils.read_df(REACTIONS_PATH)
        users_df = stats_utils.read_df(USERS_PATH)
        command_usage_df = stats_utils.read_df(COMMANDS_USAGE_PATH)
        credit_history_df = stats_utils.read_df(CREDIT_HISTORY_PATH)
        cwel_df = stats_utils.read_df(CWEL_STATS_PATH)

        credits_obj = Credits()
        credits_df = pd.DataFrame(credits_obj.credits.items(), columns=['user_id', 'credits'])

        table_to_df_map = {
            Table.CHAT_HISTORY: chat_df,
            Table.CHAT_HISTORY_CLEANED: cleaned_chat_df,
            Table.REACTIONS: reactions_df,
            Table.USERS: users_df,
            Table.COMMANDS_USAGE: command_usage_df,
            Table.CREDIT_HISTORY: credit_history_df,
            Table.CWEL: cwel_df,
            Table.CREDITS: credits_df,
        }

        for table, df in table_to_df_map.items():
            self.df_to_table(df, table)

    def df_to_table(self, df, table: Table):
        df_copy = df.copy(deep=True)

        match table:
            case Table.CHAT_HISTORY | Table.CHAT_HISTORY_CLEANED:
                print('are we here?')
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
        write_index = "user_id" in df_copy.columns and table == "users"
        # Write to SQLite
        print('now writing df:', table.value)
        print(df_copy.head(10))
        df_copy.to_sql(table.value, self.conn, if_exists="replace", index=write_index, chunksize=1000, method="multi")
        self.conn.commit()

    def table_to_df(self, table: Table):
        print(table.value)
        df = pd.read_sql_query(f"SELECT * FROM {table.value}", self.conn, parse_dates=["timestamp"])

        df = self.deserialize_lists(df, ["reaction_emojis", "reaction_user_ids"])
        # df = self.deserialize_datetimes(df, ["timestamp"])
        df = self.deserialize_bools(df, ["success"])
        return df


db = DB()

dftest = db.table_to_df(Table.CHAT_HISTORY)
print(dftest.head(10))
print(dftest.info())