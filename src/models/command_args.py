from dataclasses import dataclass, field

from definitions import PeriodFilterMode, ArgType


@dataclass
class CommandArgs:
    args: list[str] = field(default_factory=lambda: [])
    joined_args: str = ''
    joined_args_lower: str = ''
    expected_args: list[ArgType] = field(default_factory=lambda: [])
    arg_type: ArgType = None
    phrases: list[str] = field(default_factory=lambda: [])
    period_mode: PeriodFilterMode = PeriodFilterMode.TOTAL
    period_time: int = -1
    user: str = None
    number: int = 5
    number_limit: int = 100
    period_error: str = ''
    user_error: str = ''
    parse_error: str = ''
    error: str = ''
