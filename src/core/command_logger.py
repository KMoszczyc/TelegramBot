import time
from datetime import datetime
from functools import wraps
from zoneinfo import ZoneInfo

import pandas as pd
from telegram import Update
from telegram.ext import ContextTypes

from src.core.utils import read_df, save_df
from src.models.command_args import CommandArgs
from src.models.schemas import commands_usage_schema
from src.stats.utils import filter_by_time_df, validate_schema
from definitions import COMMANDS_USAGE_PATH, TIMEZONE


class CommandLogger:
    def __init__(self, bot_state):
        self.bot_state = bot_state
        self.command_usage_df = self.load_data()

    def count_command(self, command_name):
        """Decorator to log command executions and timestamps."""

        def decorator(func):
            @wraps(func)
            async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
                result = await func(update, context, *args, **kwargs)

                user_id = update.effective_user.id
                timestamp = datetime.now()
                new_entry = pd.DataFrame([{'timestamp': timestamp, 'user_id': user_id, 'command_name': command_name}])
                new_entry['timestamp'] = pd.to_datetime(new_entry['timestamp'], utc=True).dt.tz_convert(TIMEZONE)

                self.command_usage_df = pd.concat([self.command_usage_df, new_entry], ignore_index=True)

                validate_schema(self.command_usage_df, commands_usage_schema)
                save_df(self.command_usage_df, COMMANDS_USAGE_PATH)

                return result

            return wrapper

        return decorator

    def load_data(self):
        command_df = read_df(COMMANDS_USAGE_PATH)
        if command_df is None:
            command_df = pd.DataFrame(columns=['timestamp', 'user_id', 'command_name'])

        command_df['timestamp'] = pd.to_datetime(command_df['timestamp'], utc=True).dt.tz_convert(TIMEZONE)
        return command_df

    def preprocess_data(self, users_df, command_args: CommandArgs):
        filtered_df = filter_by_time_df(self.command_usage_df, command_args)
        filtered_df['username'] = filtered_df.merge(users_df[['final_username']], on='user_id', how='left')['final_username']
        filtered_df['timestamp'] = pd.to_datetime(filtered_df['timestamp'], utc=True).dt.tz_convert(TIMEZONE)

        if command_args.user is not None:
            filtered_df = filtered_df[filtered_df['username'] == command_args.user]
        return filtered_df


    def get_commands(self) -> list:
        return self.command_usage_df['command_name'].unique().tolist()