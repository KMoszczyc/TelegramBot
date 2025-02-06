import os
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
from definitions import COMMANDS_USAGE_PATH, TIMEZONE, whitelisted_commands_from_deletion, BOT_WHITELISTED_MESSAGES_PATH


class CommandLogger:
    def __init__(self, bot_state):
        self.bot_state = bot_state
        self.command_usage_df = self.load_command_usage_df()
        self.bot_whitelisted_messages_df = self.load_bot_whitelisted_messages()
        self.commands = []

    def log_command(self, command_name):
        """Decorator to log command executions and timestamps."""

        def decorator(func):
            @wraps(func)
            async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
                result = await func(update, context, *args, **kwargs)

                user_id = update.effective_user.id
                message_id = update.message.message_id

                self.update_command_counts(user_id, command_name)
                self.update_bot_whitelisted_messages(message_id, command_name)

                return result

            return wrapper

        return decorator

    def update_command_counts(self, user_id, command_name):
        timestamp = datetime.now()
        new_entry = pd.DataFrame([{'timestamp': timestamp, 'user_id': user_id, 'command_name': command_name}])
        new_entry['timestamp'] = pd.to_datetime(new_entry['timestamp'], utc=True).dt.tz_convert(TIMEZONE)

        self.command_usage_df = pd.concat([self.command_usage_df, new_entry], ignore_index=True)
        save_df(self.command_usage_df, COMMANDS_USAGE_PATH)

    def update_bot_whitelisted_messages(self, message_id, command_name):
        if command_name not in whitelisted_commands_from_deletion:
            return

        new_entry = pd.DataFrame([{'message_id': message_id}])
        self.bot_whitelisted_messages_df = pd.concat([self.bot_whitelisted_messages_df, new_entry], ignore_index=True)
        save_df(self.bot_whitelisted_messages_df, BOT_WHITELISTED_MESSAGES_PATH)

    def load_bot_whitelisted_messages(self):
        bot_whitelisted_messages_df = read_df(BOT_WHITELISTED_MESSAGES_PATH)
        if bot_whitelisted_messages_df is None:
            bot_whitelisted_messages_df = pd.DataFrame(columns=['message_id'])
        return bot_whitelisted_messages_df

    def load_command_usage_df(self):
        command_df = read_df(COMMANDS_USAGE_PATH)
        if command_df is None:
            command_df = pd.DataFrame(columns=['timestamp', 'user_id', 'command_name'])

        command_df['timestamp'] = pd.to_datetime(command_df['timestamp'], utc=True).dt.tz_convert(TIMEZONE)
        self.commands = command_df['command_name'].unique().tolist()
        return command_df

    def preprocess_data(self, users_df, command_args: CommandArgs):
        filtered_df = self.command_usage_df.copy()
        filtered_df['username'] = filtered_df.merge(users_df[['final_username']], on='user_id', how='left')['final_username']
        filtered_df = filter_by_time_df(filtered_df, command_args)
        filtered_df['timestamp'] = pd.to_datetime(filtered_df['timestamp'], utc=True).dt.tz_convert(TIMEZONE)

        if command_args.user is not None:
            filtered_df = filtered_df[filtered_df['username'] == command_args.user]

        if 'command' in command_args.named_args and command_args.named_args['command']:
            filtered_df = filtered_df[filtered_df['command_name'] == command_args.named_args['command']]
        return filtered_df

    def parse_command(self, command: str) -> tuple[bool, str]:
        if command is None:
            return False, ''

        if command in self.commands:
            return True, command
        return False, f'Command {command} does not exist.'

    def get_commands(self) -> list:
        return self.command_usage_df['command_name'].unique().tolist()
