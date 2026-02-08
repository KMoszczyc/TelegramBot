import logging
import os
from functools import wraps

from dotenv import load_dotenv
from telegram import Update
from telegram.ext import ApplicationBuilder, CallbackQueryHandler, CommandHandler, ContextTypes

import src.commands.misc_commands as commands
import src.core.utils as core_utils
from definitions import EmojiType, MessageType
from src.commands.chat_commands import ChatCommands
from src.commands.credit_commands import CreditCommands
from src.core.command_logger import CommandLogger
from src.core.job_persistance import JobPersistance
from src.models.bot_state import BotState
from src.models.credits import Credits
from src.models.db.db import DB
from src.models.holidays import Holidays

load_dotenv()
TOKEN = os.getenv("TOKEN")
TEST_TOKEN = os.getenv("TEST_TOKEN")
CHAT_ID = int(os.getenv("CHAT_ID"))
TEST_CHAT_ID = int(os.getenv("TEST_CHAT_ID"))

log = logging.getLogger(__name__)


class OzjaszBot:
    def __init__(self, test=False):
        token = TEST_TOKEN if test else TOKEN
        init_message = "Starting Ozjasz in test mode" if test else "Starting Ozjasz in prod mode"
        log.info(init_message)
        self.application = ApplicationBuilder().token(token).read_timeout(30).write_timeout(30).concurrent_updates(True).build()
        self.db = DB()
        self.bot_state = BotState(self.application.job_queue)
        self.job_persistance = JobPersistance(self.application.job_queue)
        self.credits = Credits(self.db)
        self.holidays = Holidays(self.application.job_queue, self.credits)

        self.command_logger = CommandLogger(self.bot_state, self.db)
        self.core_commands = commands.Commands(self.command_logger, self.job_persistance, self.bot_state)
        self.chat_commands = ChatCommands(self.command_logger, self.job_persistance, self.bot_state, self.db)
        self.credit_commands = CreditCommands(self.command_logger, self.job_persistance, self.bot_state, self.credits)

        self.add_commands()
        self.application.run_polling()

    def add_commands(self):
        commands_map = self.get_commands_map()
        validated_commands_map = {
            command_name: self.validate_command()(func) for command_name, func in commands_map.items()
        }  # Validate chat_id via decorator
        counted_commands_map = {
            command_name: self.command_logger.count_command(command_name)(func) for command_name, func in validated_commands_map.items()
        }  # Apply the command counter decorator

        command_handlers = [CommandHandler(command_name, func) for command_name, func in counted_commands_map.items()]
        self.application.add_handlers(command_handlers)
        self.application.add_handler(CallbackQueryHandler(self.credit_commands.btn_quiz_callback))

    def get_commands_map(self):
        return {
            "all": self.core_commands.cmd_all,
            "ozjasz": self.core_commands.cmd_ozjasz,
            "europejskafirma": self.core_commands.cmd_europejskafirma,
            "boczek": self.core_commands.cmd_boczek,
            "bartosiak": self.core_commands.cmd_bartosiak,
            "tvp": self.core_commands.cmd_tvp,
            "tvp_latest": self.core_commands.cmd_tvp_latest,
            "tusk": self.core_commands.cmd_tusk,
            "walesa": self.core_commands.cmd_walesa,
            "starababa": self.core_commands.cmd_are_you_lucky_today,
            "help": self.core_commands.cmd_help,
            "bible": lambda update, context: self.core_commands.cmd_bible(update, context, self.bot_state),
            "koran": lambda update, context: self.core_commands.cmd_quran(update, context, self.bot_state),
            "handlowa": self.core_commands.cmd_show_shopping_sundays,
            "biblestats": self.core_commands.cmd_bible_stats,
            "remindme": self.core_commands.cmd_remind_me,
            "remind": self.chat_commands.cmd_remind,
            "commands": self.chat_commands.cmd_command_usage,
            "summary": self.chat_commands.cmd_summary,
            "kiepscy": self.core_commands.cmd_kiepscy,
            "kiepscyurl": self.core_commands.cmd_kiepscyurl,
            "getcredits": self.credit_commands.cmd_get_credits,
            "gift": self.credit_commands.cmd_gift_credits,
            "creditleaderboard": self.credit_commands.cmd_show_credit_leaderboard,
            "betleaderboard": self.credit_commands.cmd_show_top_bet_leaderboard,
            "stealleaderboard": self.credit_commands.cmd_show_steal_leaderboard,
            "bet": self.credit_commands.cmd_bet,
            "steal": self.credit_commands.cmd_steal_credits,
            "stealgraph": self.credit_commands.cmd_steal_graph,
            "quiz": self.credit_commands.cmd_quiz,
            "topmessages": lambda update, context: self.chat_commands.cmd_messages_by_reactions(update, context, EmojiType.ALL),
            "sadmessages": lambda update, context: self.chat_commands.cmd_messages_by_reactions(update, context, EmojiType.NEGATIVE),
            "topmemes": lambda update, context: self.chat_commands.cmd_media_by_reactions(
                update, context, MessageType.IMAGE, EmojiType.ALL
            ),
            "sadmemes": lambda update, context: self.chat_commands.cmd_media_by_reactions(
                update, context, MessageType.IMAGE, EmojiType.NEGATIVE
            ),
            "topvideos": lambda update, context: self.chat_commands.cmd_media_by_reactions(
                update, context, MessageType.VIDEO, EmojiType.ALL
            ),
            "topgifs": lambda update, context: self.chat_commands.cmd_media_by_reactions(update, context, MessageType.GIF, EmojiType.ALL),
            "topaudio": lambda update, context: self.chat_commands.cmd_media_by_reactions(
                update, context, MessageType.AUDIO, EmojiType.ALL
            ),
            "lastmessages": self.chat_commands.cmd_last_messages,
            "displayusers": self.chat_commands.cmd_display_users,
            "setusername": self.chat_commands.cmd_set_username,
            "addnickname": self.chat_commands.cmd_add_nickname,
            "fun": self.chat_commands.cmd_fun,
            "wholesome": self.chat_commands.cmd_wholesome,
            "funchart": self.chat_commands.cmd_funchart,
            "spamchart": self.chat_commands.cmd_spamchart,
            "likechart": self.chat_commands.cmd_likechart,
            "commandschart": self.chat_commands.cmd_command_usage_chart,
            "monologuechart": self.chat_commands.cmd_monologuechart,
            "relgraph": self.chat_commands.cmd_relationship_graph,
            "cwel": self.chat_commands.cmd_cwel,
            "topcwel": self.chat_commands.cmd_topcwel,
            "wordstats": self.chat_commands.cmd_wordstats,
            "play": self.chat_commands.cmd_play,
        }

    def validate_command(self):
        """Decorator to log command executions and timestamps."""

        def decorator(func):
            @wraps(func)
            async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
                src_chat_id = update.message.chat_id
                first_name = update.message.from_user.first_name
                last_name = update.message.from_user.last_name
                user_id = update.message.from_user.id
                user_name = core_utils.get_username(first_name, last_name)

                if src_chat_id not in [CHAT_ID, TEST_CHAT_ID]:
                    error = "You cannot use this bot in your channel, sorry! :("
                    log.info(f"Attempted bot usage from chat: [{src_chat_id}] by user: {user_name} ({user_id}). Access denied.")
                    await context.bot.send_message(chat_id=update.effective_chat.id, text=error)
                    return

                log.info(f"Attempted bot usage from chat: [{src_chat_id}] by user: {user_name} ({user_id}). Access approved.")

                return await func(update, context, *args, **kwargs)

            return wrapper

        return decorator
