import logging
from collections import defaultdict
import schedule
from definitions import TIMEZONE, MAX_REMINDERS_DAILY_USAGE

log = logging.getLogger(__name__)

class BotState:
    def __init__(self):
        self.last_bible_verse_id = -1
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