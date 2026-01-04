import random


class RollResult:
    def __init__(self, roll: int, is_critical_success: bool, is_critical_failure: bool):
        self.roll = roll
        self.is_critical_success = is_critical_success
        self.is_critical_failure = is_critical_failure


class CriticalRoll:
    CRITICAL_SUCCESS_RANGE = range(1, 6)
    CRITICAL_FAILURE_RANGE = range(96, 101)

    def roll(self) -> RollResult:
        roll = random.randint(1, 100) 
        return RollResult(
            roll=roll,
            is_critical_success=roll in self.CRITICAL_SUCCESS_RANGE,
            is_critical_failure=roll in self.CRITICAL_FAILURE_RANGE
        )