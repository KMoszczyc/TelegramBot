import os
from dotenv import load_dotenv
import logging
from telegram.ext import ApplicationBuilder, CommandHandler

import src.core.commands as commands
from src.core.client_api_handler import ClientAPIHandler
from src.stats.chat_etl import ChatETL
from src.stats.chat_commands import ChatCommands
import src.stats.utils as chat_utils

load_dotenv()
TOKEN = os.getenv('TOKEN')


class OzjaszBot:
    def __init__(self):
        self.chat_commands = ChatCommands()

        self.application = ApplicationBuilder().token(TOKEN).build()
        self.add_commands()
        self.application.run_polling()

    def add_commands(self):
        command_handlers = [CommandHandler('ozjasz', commands.ozjasz),
                            CommandHandler('bartosiak', commands.bartosiak),
                            CommandHandler('tvp', commands.tvp),
                            CommandHandler('tvp_latest', commands.tvp_latest),
                            CommandHandler('tusk', commands.tusk),
                            CommandHandler('help', commands.help),
                            CommandHandler('chatstats', self.chat_commands.summary),
                            CommandHandler('topmessages', self.chat_commands.top_messages_by_reactions),
                            ]
        self.application.add_handlers(command_handlers)
