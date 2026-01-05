from typing import Callable


class EffectResult:
    def __init__(self, credit_change: int, message: str):
        self.credit_change = credit_change
        self.message = message


class RandomEvent:
    def __init__(self, description: str, effect_func: Callable, event_type: str):
        self.description = description
        self.effect_func = effect_func
        self.event_type = event_type

    def apply_effect(self, **kwargs) -> EffectResult:
        credit_change = self.effect_func(**kwargs)
        return EffectResult(credit_change, self.description)


class RandomFailureEvent(RandomEvent):
    def __init__(self, description: str, effect_func: Callable):
        super().__init__(description, effect_func, 'failure')


class RandomSuccessEvent(RandomEvent):
    def __init__(self, description: str, effect_func: Callable):
        super().__init__(description, effect_func, 'success')
