import logging
from collections import defaultdict
import datetime
from zoneinfo import ZoneInfo

from definitions import TIMEZONE, MAX_REMINDERS_DAILY_USAGE, HolyTextType, MAX_CWEL_USAGE_DAILY, MAX_GET_CREDITS_DAILY, MAX_STEAL_CREDITS_DAILY

log = logging.getLogger(__name__)


class BotState:
    def __init__(self, job_queue):
        self.last_bible_verse_id = -1
        self.last_quran_verse_id = -1
        self.remindme_usage_map = defaultdict(int)
        self.cwel_usage_daily_count_map = defaultdict(int)
        self.get_credits_daily_count_map = defaultdict(int)
        self.steal_credits_daily_count_map = defaultdict(int)
        self.quiz_cache = {}

        self.run_schedules(job_queue)

    def update_cwel_usage_map(self, cwel_giver_id, cwel_value) -> [bool, str]:
        if cwel_giver_id in self.cwel_usage_daily_count_map and self.cwel_usage_daily_count_map[cwel_giver_id] + cwel_value > MAX_CWEL_USAGE_DAILY:
            return False, 'You have reached your daily cwel limit.'

        self.cwel_usage_daily_count_map[cwel_giver_id] += cwel_value
        return True, ''

    def update_remindme(self, user_id) -> bool:
        if user_id not in self.remindme_usage_map or self.remindme_usage_map[user_id] < MAX_REMINDERS_DAILY_USAGE:
            self.remindme_usage_map[user_id] += 1
            return True
        else:
            return False

    def update_get_credits_limits(self, user_id) -> bool:
        if user_id not in self.get_credits_daily_count_map or self.get_credits_daily_count_map[user_id] < MAX_GET_CREDITS_DAILY:
            self.get_credits_daily_count_map[user_id] += 1
            return True
        else:
            return False

    def update_steal_credits_limits(self, user_id) -> bool:
        if user_id not in self.steal_credits_daily_count_map or self.steal_credits_daily_count_map[user_id] < MAX_STEAL_CREDITS_DAILY:
            self.steal_credits_daily_count_map[user_id] += 1
            return True
        else:
            return False

    def run_schedules(self, job_queue):
        time = datetime.time(0, 0, tzinfo=ZoneInfo(TIMEZONE))
        job_queue.run_daily(callback=lambda context: self.reset_command_limits(context), time=time, name='Reset command limits (/remindme, /cwel, /get_credits and /steal_credits)')

    async def reset_command_limits(self, context):
        self.remindme_usage_map = defaultdict(int)
        self.cwel_usage_daily_count_map = defaultdict(int)
        self.get_credits_daily_count_map = defaultdict(int)
        self.steal_credits_daily_count_map = defaultdict(int)
        self.quiz_cache = {}

        log.info('Remindme, cwel, get_credits and steal_credits usage limits have been reset.')

    def set_holy_text_last_verse_id(self, last_verse_id, holy_text_type: HolyTextType):
        if holy_text_type == HolyTextType.BIBLE:
            self.last_bible_verse_id = last_verse_id
        elif holy_text_type == HolyTextType.QURAN:
            self.last_quran_verse_id = last_verse_id

    def get_cwels_left(self, user_id):
        return MAX_CWEL_USAGE_DAILY - self.cwel_usage_daily_count_map[user_id]
