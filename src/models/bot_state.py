from dataclasses import dataclass, field


@dataclass
class BotState:
    last_bible_verse_id: int = -1
