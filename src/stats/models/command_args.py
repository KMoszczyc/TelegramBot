from dataclasses import dataclass, field

from definitions import PeriodFilterMode, ArgType


@dataclass
class CommandArgs:
    args: list[str] = field(default_factory=lambda: [])
    expected_args: list[ArgType] = field(default_factory=lambda: [])
    period_mode: PeriodFilterMode = PeriodFilterMode.TOTAL
    period_time: int = -1
    user: str = None
    period_error: str = ''
    user_error: str = ''
    parse_error: str = ''
    error: str = ''

