import copy
import logging
import re
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd

from src.config.constants import MAX_INT, TIMEZONE
from src.config.enums import ArgType, DatetimeFormat, PeriodFilterMode
from src.models.command_args import CommandArgs

log = logging.getLogger(__name__)


class ArgParser:
    """Encapsulates all command argument parsing logic for the bot."""

    @staticmethod
    def preprocess_input(users_df: pd.DataFrame, command_args: CommandArgs):
        command_args = ArgParser.parse_args(users_df, command_args)
        filtered_phrases, command_args = ArgParser.filter_phrases(command_args)
        return filtered_phrases, command_args

    @staticmethod
    def parse_args(users_df: pd.DataFrame, command_args: CommandArgs) -> CommandArgs:
        """
        Parses arguments and returns updated CommandArgs with period mode, period time, user, and error.
        """
        command_args = ArgParser.merge_spaced_args(command_args)
        command_args = ArgParser.parse_named_args(users_df, command_args)
        command_args.joined_args = " ".join(command_args.args)
        command_args.joined_args_lower = " ".join(command_args.args).lower()
        if command_args.is_text_arg:
            command_args.args = [command_args.joined_args]
            command_args.arg_type = ArgType.REGEX if ArgParser.is_inside_square_brackets(command_args.joined_args) else ArgType.TEXT
            return command_args

        args_num = len(command_args.args)
        expected_args_num = len(command_args.expected_args)
        if args_num > expected_args_num:
            command_args.error = f"Invalid number of arguments. Expected {command_args.expected_args}, got {command_args.args}"
            return command_args

        # Handle args
        command_args = ArgParser.handle_args(users_df, command_args)
        command_args.error = ArgParser.get_error(command_args)
        return command_args

    @staticmethod
    def handle_args(users_df: pd.DataFrame, command_args_ref: CommandArgs):
        """Handle optional arguments like Period or User."""
        if len(command_args_ref.args) == 0:
            return command_args_ref

        command_args = copy.deepcopy(command_args_ref)
        successes = []
        expected_args = command_args.expected_args.copy()
        for i, arg_type in enumerate(expected_args):
            if not command_args.optional[i]:
                if i >= len(command_args.args):
                    command_args.errors.append(f"Missing required argument: {arg_type.value}")
                    continue
                arg = " ".join(command_args.args[i:]) if arg_type == ArgType.TEXT_MULTISPACED else command_args.args[i]
                _, command_args = ArgParser.parse_arg(users_df, command_args, arg, arg_type, is_optional=False)
                continue

            if sum(successes) == len(command_args.args):
                continue

            # handle optional arg
            for arg in command_args.args:
                _, command_args = ArgParser.parse_arg(users_df, command_args, arg, arg_type, is_optional=True)
                if command_args.optional_errors[-1] != "":
                    successes.append(False)
                else:
                    successes.append(True)

        if not any(successes):
            log.info("None optional args were parsed successfully, despite there being an argument send by user.")
            command_args.errors.extend(command_args.optional_errors)
            return command_args

        log.info("All args were parsed successfully.")
        return command_args

    @staticmethod
    def merge_spaced_args(command_args: CommandArgs):
        new_args = []
        quotation_opened = False
        current_spaced_args = []
        for arg in command_args.args:
            if '"' in arg and not quotation_opened:
                quotation_opened = True
                current_spaced_args.append(arg.replace('"', ""))
            elif '"' in arg and quotation_opened:
                current_spaced_args.append(arg.replace('"', ""))
                new_args.append(" ".join(current_spaced_args))
                quotation_opened = False
                current_spaced_args = []
            elif quotation_opened:
                current_spaced_args.append(arg)
            else:
                new_args.append(arg)
        if len(current_spaced_args) == 1:
            new_args.append(current_spaced_args[0])
        command_args.args = new_args

        return command_args

    @staticmethod
    def filter_phrases(command_args: CommandArgs):
        log.info(f"Command received: {command_args.arg_type} - {command_args.joined_args}")
        match command_args.arg_type:
            case ArgType.TEXT:
                return ArgParser.text_filter(command_args)
            case ArgType.REGEX:
                return ArgParser.regex_filter(command_args)

    @staticmethod
    def text_filter(command_args):
        return [phrase for phrase in command_args.phrases if command_args.joined_args_lower in phrase.lower()], command_args

    @staticmethod
    def regex_filter(command_args):
        pattern = command_args.joined_args[1:-1]  # removes brackets
        try:
            return [phrase for phrase in command_args.phrases if re.search(pattern, phrase, flags=re.IGNORECASE)], command_args
        except re.error as e:
            command_args.error = f"{pattern} - is and invalid regex pattern."
            log.info(f"{command_args.error} - {e}")

            return [], command_args

    @staticmethod
    def is_inside_square_brackets(text: str):
        return text.startswith("[") and text.endswith("]")

    @staticmethod
    def parse_arg(users_df, command_args_ref, arg_str, arg_type: ArgType, is_optional=False) -> tuple[str | int, CommandArgs]:
        command_args = copy.deepcopy(command_args_ref)
        value = None
        error = ""
        match arg_type:
            case ArgType.USER:
                command_args, error = ArgParser.parse_user(users_df, command_args, arg_str)
            case ArgType.PERIOD:
                command_args, error = ArgParser.parse_period(command_args, arg_str)
            case ArgType.POSITIVE_INT:
                value, command_args, error = ArgParser.parse_number(command_args, arg_str, positive_only=True)
            case ArgType.STRING | ArgType.TEXT | ArgType.TEXT_MULTISPACED:
                value, command_args, error = ArgParser.parse_string(command_args, arg_str)
            case _:
                command_args = command_args

        if is_optional:
            command_args.optional_errors.append(error)
        else:
            command_args.errors.append(error)

        return value, command_args

    @staticmethod
    def parse_named_args(users_df, command_args_ref: CommandArgs):
        command_args = copy.deepcopy(command_args_ref)
        command_args.args = [arg.replace("—", "--") for arg in command_args.args]
        if not command_args.available_named_args_aliases:
            command_args.available_named_args_aliases = {arg[0]: arg for arg in command_args.available_named_args}
        args = copy.deepcopy(command_args.args)
        for i, arg in enumerate(args):
            named_arg = ArgParser.parse_named_arg(arg, command_args)
            if named_arg is None:
                continue
            if command_args.available_named_args[named_arg] == ArgType.NONE:
                command_args.named_args[named_arg] = None
            elif i + 1 < len(args) and not ArgParser.is_named_arg(args[i + 1], command_args):  # this arg has a value
                arg_type = command_args.available_named_args[named_arg]
                value, command_args = ArgParser.parse_arg(users_df, command_args, args[i + 1], arg_type)
                command_args.args.remove(args[i + 1])
                if ArgParser.get_error(command_args) == "":
                    command_args.named_args[named_arg] = value
            else:
                command_args.errors.append(f"Argument {named_arg} requires a value")
            command_args.args.remove(arg)

        command_args.error = ArgParser.get_error(command_args)
        return command_args

    @staticmethod
    def parse_named_arg(arg, command_args):
        if ArgParser.is_normal_named_arg(arg, command_args.available_named_args):
            return arg.replace("-", "")
        elif ArgParser.is_aliased_named_arg(arg, command_args.available_named_args_aliases):
            alias = arg.replace("-", "")
            return command_args.available_named_args_aliases[alias]
        return None

    @staticmethod
    def is_aliased_named_arg(arg, shortened_available_named_args):
        return arg.startswith("-") and arg[1:] in shortened_available_named_args

    @staticmethod
    def is_normal_named_arg(arg, available_named_args):
        return arg.startswith("--") and arg[2:] in available_named_args

    @staticmethod
    def is_named_arg(arg, commands_args):
        return ArgParser.is_aliased_named_arg(arg, commands_args.available_named_args_aliases) or ArgParser.is_normal_named_arg(
            arg, commands_args.available_named_args
        )

    @staticmethod
    def parse_period(command_args, arg_str) -> tuple[CommandArgs, str]:
        error = ""
        if arg_str == "":
            error = "Period cannot be empty."
            log.error(error)
            return command_args, error

        period_mode_str = arg_str
        try:
            if "s" in arg_str and ArgParser.has_numbers(arg_str):
                command_args.period_time, error = ArgParser.parse_int(arg_str.replace("s", ""), positive_only=True)
                period_mode_str = "second"
            elif ("min" in arg_str or (arg_str.endswith("m") and "mo" not in arg_str)) and ArgParser.has_numbers(arg_str):
                command_args.period_time, error = ArgParser.parse_int(arg_str.replace("min", "").replace("m", ""), positive_only=True)
                period_mode_str = "minute"
            elif "h" in arg_str and ArgParser.has_numbers(arg_str):
                command_args.period_time, error = ArgParser.parse_int(arg_str.replace("h", ""), positive_only=True)
                period_mode_str = "hour"
            elif "d" in arg_str and ArgParser.has_numbers(arg_str):
                command_args.period_time, error = ArgParser.parse_int(arg_str.replace("d", ""), positive_only=True)
                period_mode_str = "day"
            elif "w" in arg_str and ArgParser.has_numbers(arg_str):
                command_args.period_time, error = ArgParser.parse_int(arg_str.replace("w", ""), positive_only=True)
                period_mode_str = "week"
            elif ("mo" in arg_str or "mth" in arg_str) and ArgParser.has_numbers(arg_str):
                command_args.period_time, error = ArgParser.parse_int(arg_str.replace("mth", "").replace("mo", ""), positive_only=True)
                period_mode_str = "month"
            elif "y" in arg_str and ArgParser.has_numbers(arg_str):
                command_args.period_time, error = ArgParser.parse_int(arg_str.replace("y", ""), positive_only=True)
                period_mode_str = "year"

            if error == "":
                command_args.period_mode = PeriodFilterMode(period_mode_str)

            if command_args.period_mode == PeriodFilterMode.ERROR and ";" in arg_str:
                command_args.start_dt, command_args.end_dt, command_args.dt_format, error = ArgParser.parse_date_range(arg_str)
                command_args.period_mode = PeriodFilterMode.DATE_RANGE
            elif command_args.period_mode == PeriodFilterMode.ERROR:
                command_args.dt, command_args.dt_format, error = ArgParser.parse_date(arg_str)
                command_args.period_mode = PeriodFilterMode.DATE

            command_args.parse_error = error
        except ValueError:
            error = f"There is no such time period as {arg_str}."
            log.error(error)

        if error != "":
            command_args.period_mode = PeriodFilterMode.ERROR

        return command_args, error

    @staticmethod
    def parse_date(date_str: str) -> tuple[datetime, DatetimeFormat, str] | tuple[None, None, str]:
        dt_formats = [
            DatetimeFormat.DATE,
            DatetimeFormat.HOUR,
            DatetimeFormat.MINUTE,
            DatetimeFormat.SECOND,
        ]

        for dt_format in dt_formats:
            try:
                return datetime.strptime(date_str, dt_format.value).replace(tzinfo=ZoneInfo(TIMEZONE)), dt_format, ""
            except ValueError:
                pass
        return None, None, f"Could not parse date: {date_str}"

    @staticmethod
    def parse_date_range(date_range_str: str) -> tuple[datetime, datetime, DatetimeFormat, str] | tuple[None, None, None, str]:
        date_range_split = date_range_str.split(";")
        if len(date_range_split) != 2:
            error = f"Could not parse date range: {date_range_str}"
            return None, None, None, error
        start_date, dt_format, start_date_error = ArgParser.parse_date(date_range_split[0])
        end_date, dt_format, end_date_error = ArgParser.parse_date(date_range_split[1])
        error = start_date_error + end_date_error

        if error == "" and start_date > end_date:
            error = "The start date cannot be after the end date of the range u dummy!"

        return start_date, end_date, dt_format, error

    @staticmethod
    def parse_user(users_df, command_args, arg_str) -> tuple[CommandArgs, str]:
        if arg_str == "":
            error = "User cannot be empty."
            log.error(error)
            return command_args, error

        user_str = arg_str.replace("@", "")

        exact_matching_users = users_df[users_df["final_username"].str.lower() == user_str.lower()]
        partially_matching_users = users_df[users_df["final_username"].str.contains(user_str, case=False)]

        if not exact_matching_users.empty:
            command_args.user = exact_matching_users.iloc[0]["final_username"]
            command_args.user_id = exact_matching_users.index[0]
        elif len(user_str) >= 3 and not partially_matching_users.empty:
            command_args.user = partially_matching_users.iloc[0]["final_username"]
            command_args.user_id = partially_matching_users.index[0]
        else:
            error = f"User {user_str} doesn't exist and cannot hurt you. Existing users are: {users_df['final_username'].tolist()}"
            log.error(error)
            return command_args, error

        return command_args, ""

    @staticmethod
    def parse_number(command_args, arg_str, positive_only=False) -> tuple[int | None, CommandArgs, str]:
        if arg_str == "":
            return None, command_args, ""

        number, error = ArgParser.parse_int(arg_str, positive_only)
        if error != "":
            return None, command_args, error

        if number > command_args.max_number:
            error = f"Given number is too big ({ArgParser.x_to_light_years_str(number)}), make it smaller!"
            log.error(error)
            return number, command_args, error

        if number < command_args.min_number:
            error = f"Given number is too small ({number}), it has to be in range <{command_args.min_number}, {command_args.max_number}>!"
            log.error(error)
            return number, command_args, error

        command_args.number = number
        return number, command_args, ""

    @staticmethod
    def get_error(command_args: CommandArgs) -> str:
        return "\n".join(command_args.errors).strip()

    @staticmethod
    def parse_int(num_str, positive_only=False):
        error = ""
        num = None
        try:
            num = int(num_str)
            if num > MAX_INT:
                error = f"Kuba's dick is too big ({ArgParser.x_to_light_years_str(num)}), make it smaller!"
                log.error(error)
            if positive_only and num < 0:
                error = "Number cannot be negative!"
                num = -1
                log.error(error)
        except ValueError:
            error = f"{num_str} is not a number."
            log.error(error)

        return num, error

    @staticmethod
    def x_to_light_years_str(x):
        """Kinda to last years, keep small numbers the same."""
        if x < 10000000:
            return str(x)

        ly = x / 9460730472580.8
        ly = round(ly, 6) if ly < 1 else round(ly, 2)
        return f"{ly} light years"

    @staticmethod
    def parse_string(command_args: CommandArgs, text: str) -> tuple[str, CommandArgs, str]:
        error = ""
        if len(text) < command_args.min_string_length:
            error = f"{command_args.label} {text} is too short, it should have at least {command_args.min_string_length} characters."
        if len(text) > command_args.max_string_length:
            error = f"{command_args.label} {text} is too long, it should have {command_args.max_string_length} characters or less."

        if "&" in text and "http" not in text:  # user for 'AND' filtering but don't do it for links
            command_args.strings = text.split("&")
        else:
            command_args.string = text
        return text, command_args, error

    @staticmethod
    def has_numbers(num_str):
        return any(char.isdigit() for char in num_str)
