import os
from dotenv import load_dotenv
import logging
from telegram.ext import ApplicationBuilder, CommandHandler

import src.commands as commands

load_dotenv()
TOKEN = os.getenv('TOKEN')

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)


class OzjaszBot:
    def __init__(self):
        self.application = ApplicationBuilder().token(TOKEN).build()
        self.add_commands()
        self.application.run_polling()

    def add_commands(self):
        command_handlers = [CommandHandler('start', commands.start),
                            CommandHandler('ozjasz', commands.ozjasz),
                            CommandHandler('tvp', commands.tvp),
                            CommandHandler('tvp_latest', commands.tvp_latest),
                            CommandHandler('tusk', commands.tusk),
                            CommandHandler('help', commands.help)
                            ]
        self.application.add_handlers(command_handlers)
