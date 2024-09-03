import os
from dotenv import load_dotenv
import logging
from telegram.ext import ApplicationBuilder, CommandHandler

import src.core.misc_commands as commands
from src.core.command_logger import CommandLogger
from src.models.bot_state import BotState
from src.stats.chat_commands import ChatCommands
from definitions import EmojiType, MessageType

load_dotenv()
TOKEN = os.getenv('TOKEN')
log = logging.getLogger(__name__)


class OzjaszBot:
    def __init__(self):
        log.info('Starting Ozjasz bot...')

        self.bot_state = BotState()
        self.command_logger = CommandLogger(self.bot_state)
        self.core_commands = commands.Commands(self.command_logger)
        self.chat_commands = ChatCommands(self.command_logger)

        self.application = ApplicationBuilder().token(TOKEN).build()
        self.add_commands()
        self.application.run_polling()

    def add_commands(self):
        # command_handlers = [CommandHandler('ozjasz', self.core_commands.cmd_ozjasz),
        #                     CommandHandler('bartosiak', self.core_commands.cmd_bartosiak),
        #                     CommandHandler('tvp', self.core_commands.cmd_tvp),
        #                     CommandHandler('tvp_latest', self.core_commands.cmd_tvp_latest),
        #                     CommandHandler('tusk', self.core_commands.cmd_tusk),
        #                     CommandHandler('starababa', self.core_commands.cmd_are_you_lucky_today),
        #                     CommandHandler('help', self.core_commands.cmd_help),
        #                     CommandHandler('bible', lambda update, context: self.core_commands.cmd_bible(update, context, self.bot_state)),
        #                     CommandHandler('handlowa', self.core_commands.cmd_show_shopping_sundays),
        #                     CommandHandler('commands', self.chat_commands.cmd_command_usage),
        #                     CommandHandler('summary', self.chat_commands.cmd_summary),
        #                     CommandHandler('topmessages', lambda update, context: self.chat_commands.cmd_messages_by_reactions(update, context, EmojiType.ALL)),
        #                     CommandHandler('sadmessages', lambda update, context: self.chat_commands.cmd_messages_by_reactions(update, context, EmojiType.NEGATIVE)),
        #                     CommandHandler('topmemes', lambda update, context: self.chat_commands.cmd_media_by_reactions(update, context, MessageType.IMAGE, EmojiType.ALL)),
        #                     CommandHandler('sadmemes', lambda update, context: self.chat_commands.cmd_media_by_reactions(update, context, MessageType.IMAGE, EmojiType.NEGATIVE)),
        #                     CommandHandler('topvideos', lambda update, context: self.chat_commands.cmd_media_by_reactions(update, context, MessageType.VIDEO, EmojiType.ALL)),
        #                     CommandHandler('topgifs', lambda update, context: self.chat_commands.cmd_media_by_reactions(update, context, MessageType.GIF, EmojiType.ALL)),
        #                     CommandHandler('topaudio', lambda update, context: self.chat_commands.cmd_media_by_reactions(update, context, MessageType.AUDIO, EmojiType.ALL)),
        #                     CommandHandler('lastmessages', lambda update, context: self.chat_commands.cmd_last_messages(update, context)),
        #                     CommandHandler('displayusers', lambda update, context: self.chat_commands.cmd_display_users(update, context)),
        #                     CommandHandler('setusername', lambda update, context: self.chat_commands.cmd_set_username(update, context)),
        #                     CommandHandler('addnickname', lambda update, context: self.chat_commands.cmd_add_nickname(update, context)),
        #                     CommandHandler('fun', lambda update, context: self.chat_commands.cmd_fun(update, context)),
        #                     CommandHandler('wholesome', lambda update, context: self.chat_commands.cmd_wholesome(update, context)),
        #                     CommandHandler('funchart', lambda update, context: self.chat_commands.cmd_funchart(update, context)),
        #                     CommandHandler('spamchart', lambda update, context: self.chat_commands.cmd_spamchart(update, context)),
        #                     CommandHandler('likechart', lambda update, context: self.chat_commands.cmd_likechart(update, context))
        #                     ]

        commands_map = self.get_commands_map()
        decorated_commands_map = {command_name: self.command_logger.count_command(command_name)(func) for command_name, func in commands_map.items()} # Apply the command counter decorator
        command_handlers = [CommandHandler(command_name, func) for command_name, func in decorated_commands_map.items()]
        self.application.add_handlers(command_handlers)


    def get_commands_map(self):
        return {
            'ozjasz': self.core_commands.cmd_ozjasz,
            'bartosiak': self.core_commands.cmd_bartosiak,
            'tvp': self.core_commands.cmd_tvp,
            'tvp_latest': self.core_commands.cmd_tvp_latest,
            'tusk': self.core_commands.cmd_tusk,
            'starababa': self.core_commands.cmd_are_you_lucky_today,
            'help': self.core_commands.cmd_help,
            'bible': lambda update, context: self.core_commands.cmd_bible(update, context, self.bot_state),
            'handlowa': self.core_commands.cmd_show_shopping_sundays,
            'commands': self.chat_commands.cmd_command_usage,
            'summary': self.chat_commands.cmd_summary,
            'topmessages': lambda update, context: self.chat_commands.cmd_messages_by_reactions(update, context, EmojiType.ALL),
            'sadmessages': lambda update, context: self.chat_commands.cmd_messages_by_reactions(update, context, EmojiType.NEGATIVE),
            'topmemes': lambda update, context: self.chat_commands.cmd_media_by_reactions(update, context, MessageType.IMAGE, EmojiType.ALL),
            'sadmemes': lambda update, context: self.chat_commands.cmd_media_by_reactions(update, context, MessageType.IMAGE, EmojiType.NEGATIVE),
            'topvideos': lambda update, context: self.chat_commands.cmd_media_by_reactions(update, context, MessageType.VIDEO, EmojiType.ALL),
            'topgifs': lambda update, context: self.chat_commands.cmd_media_by_reactions(update, context, MessageType.GIF, EmojiType.ALL),
            'topaudio': lambda update, context: self.chat_commands.cmd_media_by_reactions(update, context, MessageType.AUDIO, EmojiType.ALL),
            'lastmessages':  self.chat_commands.cmd_last_messages,
            'displayusers':self.chat_commands.cmd_display_users,
            'setusername': self.chat_commands.cmd_set_username,
            'addnickname': self.chat_commands.cmd_add_nickname,
            'fun': self.chat_commands.cmd_fun,
            'wholesome': self.chat_commands.cmd_wholesome,
            'funchart': self.chat_commands.cmd_funchart,
            'spamchart': self.chat_commands.cmd_spamchart,
            'likechart': self.chat_commands.cmd_likechart,
        }
