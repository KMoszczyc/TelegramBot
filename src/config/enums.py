from enum import Enum

from src.models.random_event import RandomFailureEvent, RandomSuccessEvent


class PeriodFilterMode(Enum):
    TODAY = "today"
    SECOND = "second"
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"
    YESTERDAY = "yesterday"
    WEEK = "week"
    WEEKS = "weeks"
    MONTH = "month"
    YEAR = "year"
    TOTAL = "total"
    DATE = "date"
    DATE_RANGE = "date_range"
    ERROR = "error"

    @classmethod
    def _missing_(cls, value):
        return PeriodFilterMode.ERROR


class DatetimeFormat(Enum):
    DATE = "%d-%m-%Y"
    HOUR = "%d-%m-%Y:%H"
    MINUTE = "%d-%m-%Y:%H:%M"
    SECOND = "%d-%m-%Y:%H:%M:%S"


class EmojiType(Enum):
    ALL = "all"
    POSITIVE = "positive"
    NEGATIVE = "negative"


class ArgType(Enum):
    USER = "user"
    PERIOD = "period"
    TEXT = "text"
    TEXT_MULTISPACED = "text_multispaced"
    REGEX = "regex"
    NUMBER = "number"
    STRING = "string"
    POSITIVE_INT = "positive_int"
    NONE = "none"


class MessageType(Enum):
    TEXT = "text"
    GIF = "gif"
    VIDEO = "video"
    VIDEO_NOTE = "video_note"
    IMAGE = "image"
    AUDIO = "audio"
    VOICE = "voice"
    NONE = "none"


class ErrorMessage(str, Enum):
    NO_SUCH_PHRASE = "Nie ma takiej wypowiedzi :("
    NO_SUCH_HEADLINE = "Nie ma takiego nagłówka :("
    NO_SUCH_ITEM = "Nie ma takiego przedmiotu :("
    NO_SUCH_VERSE = "Nie ma takiego wersetu. Beduinom pustynnym weszło post-nut clarity po wyruchaniu kozy. :("
    NO_SUCH_EPISODE = "Nie ma takiego epizodu :("
    NO_DATA_FOR_PERIOD = "No data from that period, sorry :("
    TOO_MUCH_TEXT = "Too much text to display. Lower the number of messages."
    CWEL_NO_REPLY = "You have to reply to a message to cwel someone."
    CWEL_BOT = "You cannot cwel Ozjasz. Only Ozjasz can cwel you."
    CWEL_SELF = "You cannot cwel yourself."


class NamedArgType(Enum):
    SHORT = "short"
    NORMAL = "normal"
    NONE = "none"


class ChartType(Enum):
    LINE = "line"
    BAR = "bar"
    MIXED = "mixed"


class HolyTextType(Enum):
    BIBLE = "bible"
    QURAN = "quran"


class SiglumType(Enum):
    FULL = "full"
    SHORT = "short"


class LuckyScoreType(Enum):
    VERY_UNLUCKY = "very unlucky"
    UNLUCKY = "unlucky"
    NEUTRAL = "neutral"
    LUCKY = "lucky"
    VERY_LUCKY = "very lucky"


class RouletteBetType(Enum):
    RED = "red"
    BLACK = "black"
    GREEN = "green"
    ODD = "odd"
    EVEN = "even"
    NONE = "none"
    HIGH = "high"
    LOW = "low"
    SINGLE_NUMBER = "single_number"


class CreditActionType(Enum):
    GET = "get"
    BET = "bet"
    STEAL = "steal"
    QUIZ = "quiz"
    GIFT = "gift"


class Table(Enum):
    USERS = "users"
    CHAT_HISTORY = "chat_history"
    CLEANED_CHAT_HISTORY = "cleaned_chat_history"
    CWEL = "cwel"
    CREDIT_HISTORY = "credit_history"
    COMMANDS_USAGE = "commands_usage"
    REACTIONS = "reactions"
    CREDITS = "credits"
    UPDATED_MESSAGE_IDS = "updated_message_ids"


class DBSaveMode(Enum):
    APPEND = "append"
    REPLACE = "replace"
    FAIL = "fail"


# Events
STEAL_EVENTS = [
    RandomFailureEvent("A swarm of angry bees stole your credits mid-escape — half gone!", lambda amount: -amount // 2),
    RandomFailureEvent("The target turned into a dragon and breathed fire — you fled empty-handed!", lambda amount: -amount),
    RandomFailureEvent("Your shadow betrayed you and ate the credits — 75% vanished!", lambda amount: int(-amount * 0.75)),
    RandomFailureEvent("Gravity reversed, credits floated away — quarter lost!", lambda amount: -amount // 4),
    RandomFailureEvent("Time looped, you stole from yourself — total paradox loss!", lambda amount: -amount),
    RandomSuccessEvent("You accidentally robbed a bank vault — bonus jackpot!", lambda amount: amount // 2),
    RandomSuccessEvent("The target's gold statue came to life and thanked you — extra gold!", lambda amount: amount),
    RandomSuccessEvent("You found a portal to a credit dimension — double reality!", lambda amount: amount * 2),
    RandomSuccessEvent("Aliens beamed down and upgraded your haul — triple cosmic bonus!", lambda amount: amount * 3),
    RandomSuccessEvent("Your evil twin helped — lucky twin credits!", lambda amount: amount // 3),
]

BET_EVENTS = [
    RandomFailureEvent("The roulette wheel turned into a black hole — sucked double losses!", lambda amount: -amount * 2),
    RandomFailureEvent("Your bet offended the casino ghost — table haunted you away!", lambda amount: -amount),
    RandomFailureEvent("The dealer was a vampire — drained your credits twice!", lambda amount: -amount * 2),
    RandomFailureEvent("Bad luck gremlin possessed you — lost all plus gremlin fee!", lambda amount: -amount - 50),
    RandomFailureEvent("The ball became sentient and rebelled — total anarchy wipeout!", lambda amount: -amount),
    RandomSuccessEvent("Lady Luck was actually a wizard — enchanted double winnings!", lambda amount: amount),
    RandomSuccessEvent("You summoned the jackpot demon — triple infernal payout!", lambda amount: amount * 3),
    RandomSuccessEvent("Stars formed a winning constellation — triple celestial bonus!", lambda amount: amount * 3),
    RandomSuccessEvent("Epic win? Nah, legendary streak — 5x mega bonus!", lambda amount: amount * 5),
    RandomSuccessEvent("Casino turned into a fairy tale — unbelievable 10x magic!", lambda amount: amount * 10),
]

QUIZ_EVENTS = [
    RandomFailureEvent("Distracted by a dancing squirrel — penalty for cuteness overload!", lambda: -10),
    RandomFailureEvent("Your brain turned into jelly — extra wobbly penalty!", lambda: -20),
    RandomFailureEvent("Wrong answer summoned a quiz troll — big oops penalty!", lambda: -30),
    RandomFailureEvent("Memory erased by time travelers — lost in history!", lambda: -15),
    RandomFailureEvent("Quiz apocalypse — severe cosmic fail penalty!", lambda: -50),
    RandomSuccessEvent("Your answer echoed through dimensions — bonus echo credits!", lambda: 15),
    RandomSuccessEvent("Quiz gods blessed you — divine knowledge payout!", lambda: 25),
    RandomSuccessEvent("Time froze for your brilliance — frozen time bonus!", lambda: 50),
    RandomSuccessEvent("You broke the quiz matrix — system crash payout!", lambda: 30),
    RandomSuccessEvent("Genius level: Expert — ultimate expert bonus!", lambda: 100),
]
