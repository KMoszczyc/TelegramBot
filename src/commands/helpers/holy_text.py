import pandas as pd

import src.core.utils as core_utils
from src.config.enums import ErrorMessage, HolyTextType, SiglumType
from src.models.core.bot_state import BotState
from src.models.data.command_args import CommandArgs


def handle_holy_text_named_params(
    command_args: CommandArgs,
    filtered_df: pd.DataFrame,
    raw_df: pd.DataFrame,
    bot_state: BotState,
    filter_phrase: str,
    holy_text_type: HolyTextType,
) -> tuple[str, str]:
    """Handles query arguments and paging options for Bible and Quran verses."""
    error = ""
    last_verse_id = bot_state.last_bible_verse_id if holy_text_type == HolyTextType.BIBLE else bot_state.last_quran_verse_id
    if filtered_df.empty:
        response = ErrorMessage.NO_SUCH_VERSE
    elif "num" in command_args.named_args:
        df = filtered_df.head(command_args.named_args["num"])
        response = core_utils.display_holy_text_df(
            df, bot_state, holy_text_type, label=f'{len(df)} {holy_text_type.value} verses with "{filter_phrase}"'
        )
    elif "all" in command_args.named_args:
        response = core_utils.display_holy_text_df(
            filtered_df, bot_state, holy_text_type, label=f'{len(filtered_df)} {holy_text_type.value} verses with "{filter_phrase}"'
        )
    elif "prev" in command_args.named_args and last_verse_id != -1:
        start_index = max(0, last_verse_id - command_args.named_args["prev"])
        filtered_df = raw_df.iloc[start_index:last_verse_id]
        label = f"{command_args.named_args['prev']} {holy_text_type.value} verses before {core_utils.get_siglum(raw_df.iloc[last_verse_id], holy_text_type, SiglumType.FULL)}"
        response = core_utils.display_holy_text_df(filtered_df, bot_state, holy_text_type, label=label, show_siglum=False)
    elif "next" in command_args.named_args and last_verse_id != -1:
        end_index = min(len(filtered_df), last_verse_id + command_args.named_args["next"] + 1)
        filtered_df = raw_df.iloc[last_verse_id + 1 : end_index]
        label = f"{command_args.named_args['next']} {holy_text_type.value} verses after {core_utils.get_siglum(raw_df.iloc[last_verse_id], holy_text_type, SiglumType.FULL)}"
        response = core_utils.display_holy_text_df(filtered_df, bot_state, holy_text_type, label=label, show_siglum=False)
    elif "count" in command_args.named_args:
        random_row = filtered_df.iloc[0]
        bot_state.set_holy_text_last_verse_id(random_row.name, holy_text_type)
        response = f'{len(filtered_df)} {holy_text_type.value} verses with "{filter_phrase}": \n\n'
        response += f"[{core_utils.get_siglum(random_row, holy_text_type, SiglumType.SHORT)}] {random_row['text']}"
    elif "verse" in command_args.named_args:
        response, error = core_utils.parse_quran_verse_arg(raw_df, command_args.named_args["verse"], bot_state, holy_text_type)
    else:
        random_row = filtered_df.iloc[0]
        bot_state.set_holy_text_last_verse_id(random_row.name, holy_text_type)
        response = f"[{core_utils.get_siglum(random_row, holy_text_type, SiglumType.SHORT)}] {random_row['text']}"

    return response, error
