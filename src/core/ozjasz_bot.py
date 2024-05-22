import os
from dotenv import load_dotenv
import logging
from telegram.ext import ApplicationBuilder, CommandHandler

import src.core.commands as commands
from src.core.client_api_handler import ClientAPIHandler
from src.stats.chat_stats import ChatStats
from src.stats.chat_commands import ChatCommands
import src.stats.utils as chat_utils

load_dotenv()
TOKEN = os.getenv('TOKEN')


class OzjaszBot:
    def __init__(self):
        self.api_handler = ClientAPIHandler()

        self.chat_stats = ChatStats(self.api_handler)
        self.chat_stats.update()
        self.chat_commands = ChatCommands()
        # chat_stats.generate_word_stats()
        # self.chat_stats.generate_reactions_df()

        self.application = ApplicationBuilder().token(TOKEN).build()
        self.add_commands()
        self.application.run_polling()

    def add_commands(self):
        command_handlers = [CommandHandler('ozjasz', commands.ozjasz),
                            CommandHandler('tvp', commands.tvp),
                            CommandHandler('tvp_latest', commands.tvp_latest),
                            CommandHandler('tusk', commands.tusk),
                            CommandHandler('help', commands.help),
                            CommandHandler('chatstats', self.chat_commands.summary)
                            ]
        self.application.add_handlers(command_handlers)
