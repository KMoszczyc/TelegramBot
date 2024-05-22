from definitions import CHAT_HISTORY_PATH, USERS_PATH, METADATA_PATH, CLEANED_CHAT_HISTORY_PATH, POLISH_STOPWORDS_PATH, REACTIONS_PATH
import src.stats.utils as stats_utils

class ViewGenerator:
    """Responsible for generating grouped pandas dataframes (views), ready to be converted to a string and sent to telegram."""
    def __init__(self):
        self.chat_df = stats_utils.read_df(CLEANED_CHAT_HISTORY_PATH)
        self.reactions_df = stats_utils.read_df(REACTIONS_PATH)
        self.users_df = stats_utils.read_df(USERS_PATH)

