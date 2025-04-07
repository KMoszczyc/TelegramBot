import logging
from collections import defaultdict
import schedule
from definitions import TIMEZONE, MAX_REMINDERS_DAILY_USAGE, HolyTextType

log = logging.getLogger(__name__)


class BotState:
    def __init__(self):
        self.last_bible_verse_id = -1
        self.last_quran_verse_id = -1
        self.remindme_usage_map = defaultdict(int)

    def update_remindme(self, user_id) -> bool:
        if user_id not in self.remindme_usage_map or self.remindme_usage_map[user_id] <= MAX_REMINDERS_DAILY_USAGE:
            self.remindme_usage_map[user_id] += 1
            return True
        else:
            return False

    def schedule_reset_remindme_usage(self):
        schedule.every().day.at("00:15", TIMEZONE).do(self.reset_remindme_usage)

    def reset_remindme_usage(self):
        self.remindme_usage_map = defaultdict(int)
        log.info('Remindme usage reset.')

    def set_holy_text_last_verse_id(self, last_verse_id, holy_text_type: HolyTextType):
        if holy_text_type == HolyTextType.BIBLE:
            self.last_bible_verse_id = last_verse_id
        elif holy_text_type == HolyTextType.QURAN:
            self.last_quran_verse_id = last_verse_id
