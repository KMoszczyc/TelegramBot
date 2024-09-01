from datetime import datetime
from functools import wraps

import pandas as pd
from telegram import Update
from telegram.ext import ContextTypes


class CommandLogger:
    def __init__(self, bot_state):
        self.bot_state = bot_state
        self.command_data = pd.DataFrame(columns=['timestamp','command_name'])

    def count_command(self, command_name):
        """Decorator to count command executions and log timestamps."""
        def decorator(func):
            @wraps(func)
            async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
                # Log the command usage with timestamp
                timestamp = datetime.now()
                new_entry = {'timestamp': timestamp, 'command_name': command_name}
                self.command_data = pd.concat([self.command_data, pd.DataFrame([new_entry])], ignore_index=True)

                return await func(update, context, *args, **kwargs)
            return wrapper
        return decorator

    def get_command_usage(self):
        """Return the current command usage statistics as a DataFrame."""
        return self.command_data

    def get_command_summary(self):
        """Return a summary of command usage with counts and latest timestamp."""
        summary = self.command_data.groupby('command_name').agg({
            'timestamp': ['count', 'max']
        }).reset_index()
        summary.columns = ['command_name', 'count', 'last_used']
        return summary