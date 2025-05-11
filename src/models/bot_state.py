import logging
from collections import defaultdict
import schedule
from definitions import TIMEZONE, MAX_REMINDERS_DAILY_USAGE, HolyTextType, MAX_CWEL_USAGE_DAILY

log = logging.getLogger(__name__)


class BotState:
    def __init__(self):
        self.last_bible_verse_id = -1
        self.last_quran_verse_id = -1
        self.remindme_usage_map = defaultdict(int)
        self.cwel_usage_daily_count_map = defaultdict(int)

        self.run_schedules()


    def update_cwel_usage_map(self, cwel_giver_id) -> [bool, str]:
        if cwel_giver_id in self.cwel_usage_daily_count_map and self.cwel_usage_daily_count_map[cwel_giver_id] >= MAX_CWEL_USAGE_DAILY:
            return False, 'You have reached your daily cwel limit.'

        self.cwel_usage_daily_count_map[cwel_giver_id] += 1
        return True, ''

    def update_remindme(self, user_id) -> bool:
        if user_id not in self.remindme_usage_map or self.remindme_usage_map[user_id] <= MAX_REMINDERS_DAILY_USAGE:
            self.remindme_usage_map[user_id] += 1
            return True
        else:
            return False

    def run_schedules(self):
        schedule.every().day.at("00:15", TIMEZONE).do(self.reset_command_limits)

    def reset_command_limits(self):
        self.remindme_usage_map = defaultdict(int)
        self.cwel_usage_daily_count = defaultdict(int)

        log.info('Remindme and cwel usage limits have been reset.')

    def set_holy_text_last_verse_id(self, last_verse_id, holy_text_type: HolyTextType):
        if holy_text_type == HolyTextType.BIBLE:
            self.last_bible_verse_id = last_verse_id
        elif holy_text_type == HolyTextType.QURAN:
            self.last_quran_verse_id = last_verse_id
