import random
from typing import Optional

from src.models.random_event import RandomEvent


class EventManager:
    def __init__(self):
        self.events: dict[str, dict[str, list[RandomEvent]]] = {}

    def add_event(self, command_name: str, event):
        if command_name not in self.events:
            self.events[command_name] = {'success': [], 'failure': []}
        self.events[command_name][event.event_type].append(event)

    def get_random_event(self, command_name: str, event_type: str) -> Optional['RandomEvent']:
        if command_name not in self.events or not self.events[command_name][event_type]:
            return None
        return random.choice(self.events[command_name][event_type])
