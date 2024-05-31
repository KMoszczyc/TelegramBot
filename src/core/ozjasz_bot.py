import os
from dotenv import load_dotenv
import logging
from telegram.ext import ApplicationBuilder, CommandHandler

import src.core.commands as commands
from src.stats.chat_commands import ChatCommands
from definitions import EmojiType
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
                            # CommandHandler('topmessages', self.chat_commands.top_messages_by_reactions),
                            # CommandHandler('topmemes', self.chat_commands.top_memes_by_reactions),
                            CommandHandler('topmessages', lambda update, context: self.chat_commands.messages_by_reactions(update, context, EmojiType.ALL)),
                            CommandHandler('sadmessages', lambda update, context: self.chat_commands.messages_by_reactions(update, context, EmojiType.NEGATIVE)),
                            CommandHandler('topmemes', lambda update, context: self.chat_commands.memes_by_reactions(update, context, EmojiType.ALL)),
                            CommandHandler('sadmemes', lambda update, context: self.chat_commands.memes_by_reactions(update, context, EmojiType.NEGATIVE))
                            ]

        self.application.add_handlers(command_handlers)
