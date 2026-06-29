from collections.abc import Callable
from dataclasses import dataclass


@dataclass
class EffectResult:
    credit_change: int
    message: str


@dataclass
class RandomEvent:
    description: str
    effect_func: Callable
    event_type: str

    def apply_effect(self, **kwargs) -> EffectResult:
        credit_change = self.effect_func(**kwargs)
        return EffectResult(credit_change, self.description)


class RandomFailureEvent(RandomEvent):
    def __init__(self, description: str, effect_func: Callable):
        super().__init__(description, effect_func, "failure")


class RandomSuccessEvent(RandomEvent):
    def __init__(self, description: str, effect_func: Callable):
        super().__init__(description, effect_func, "success")
