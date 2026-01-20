from dataclasses import dataclass, field
from datetime import datetime

from definitions import ArgType, DatetimeFormat, PeriodFilterMode


@dataclass
class CommandArgs:
    args: list[str] = field(default_factory=lambda: [])
    joined_args: str = ''
    joined_args_lower: str = ''
    expected_args: list[ArgType] = field(default_factory=lambda: [])
    handled_expected_args: list[ArgType] = field(default_factory=lambda: [])
    optional: list[bool] = field(default_factory=lambda: [])
    available_named_args: dict[str, ArgType] = field(default_factory=lambda: {})
    available_named_args_aliases: dict[str, str] = field(default_factory=lambda: {})
    named_args: dict = field(default_factory=lambda: {})
    errors: list[str] = field(default_factory=lambda: [])
    optional_errors: list[str] = field(default_factory=lambda: [])
    arg_type: ArgType = None
    phrases: list[str] = field(default_factory=lambda: [])
    period_mode: PeriodFilterMode = PeriodFilterMode.TOTAL
    period_time: int = -1
    user: str = None
    user_id: str = None
    start_dt: datetime = None
    end_dt: datetime = None
    dt: datetime = None
    dt_format: DatetimeFormat = None
    number: int = None
    string: str = ''
    strings: list[str] = field(default_factory=lambda: [])
    min_string_length: int = 0
    max_string_length: int = 20
    label: str = ''
    max_number: int = 100
    min_number: int = 0
    is_text_arg: bool = False
    period_error: str = ''
    user_error: str = ''
    parse_error: str = ''
    error: str = ''

    def __post_init__(self):
        if not self.optional:
            self.optional = [False] * len(self.expected_args)
