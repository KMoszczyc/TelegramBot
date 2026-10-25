import logging

import telegram
from telegram.ext import ContextTypes

import src.models.quizzes.tip_message as tip_message

log = logging.getLogger(__name__)


async def remove_tip_button(context: ContextTypes.DEFAULT_TYPE, cached_quiz: dict) -> None:
    """Removes the tip inline button from a quiz message if present."""
    if cached_quiz.get("message_id") is None:
        return
    try:
        await context.bot.edit_message_reply_markup(chat_id=cached_quiz["chat_id"], message_id=cached_quiz["message_id"], reply_markup=None)
    except telegram.error.TelegramError as e:
        log.warning(f"Could not remove tip button: {e}")


async def update_quiz_message(
    context: ContextTypes.DEFAULT_TYPE, cached_quiz: dict, text: str, reply_markup: telegram.InlineKeyboardMarkup | None
) -> None:
    """Updates either text or caption of an existing quiz message."""
    edit_kwargs = {
        "chat_id": cached_quiz["chat_id"],
        "message_id": cached_quiz["message_id"],
        "parse_mode": telegram.constants.ParseMode.MARKDOWN_V2,
        "reply_markup": reply_markup,
    }
    try:
        if cached_quiz.get("has_photo"):
            await context.bot.edit_message_caption(caption=text, **edit_kwargs)
        else:
            await context.bot.edit_message_text(text=text, **edit_kwargs)
    except telegram.error.BadRequest as e:
        log.warning(f"Could not edit quiz message with MarkdownV2: {e}. Retrying without markdown.")
        edit_kwargs["parse_mode"] = None
        try:
            if cached_quiz.get("has_photo"):
                await context.bot.edit_message_caption(caption=text, **edit_kwargs)
            else:
                await context.bot.edit_message_text(text=text, **edit_kwargs)
        except telegram.error.TelegramError as e2:
            log.warning(f"Could not edit quiz message even without markdown: {e2}")
    except telegram.error.TelegramError as e:
        log.warning(f"Could not edit quiz message: {e}")


async def show_quiz_tip(
    context: ContextTypes.DEFAULT_TYPE,
    cached_quiz: dict,
    user_id: int,
    quiz_class,
    quiz_instance,
    tip_line: str,
    fallback_reply_to: int | None = None,
) -> None:
    """Displays a revealed tip in the quiz message (or as reply if overflowing)."""
    plan = tip_message.plan_tip_display(cached_quiz, quiz_instance, quiz_class, user_id, tip_line)
    if plan.can_edit:
        await update_quiz_message(context, cached_quiz, plan.edit_text, plan.keyboard)
        return

    await context.bot.send_message(
        chat_id=plan.chat_id,
        reply_to_message_id=plan.message_id if plan.message_id is not None else fallback_reply_to,
        text=plan.overflow_text,
        parse_mode=telegram.constants.ParseMode.MARKDOWN_V2,
        message_thread_id=plan.thread_id,
    )
    if plan.message_id is not None:
        try:
            await context.bot.edit_message_reply_markup(chat_id=plan.chat_id, message_id=plan.message_id, reply_markup=plan.keyboard)
        except telegram.error.TelegramError as e:
            log.warning(f"Could not update tip button: {e}")


async def delete_quiz_answer(update, quiz_message_id: int | None = None) -> None:
    """Best-effort delete the user's answer message from the chat."""
    if not getattr(update, "message", None):
        return
    try:
        await update.message.delete()
    except telegram.error.TelegramError as e:
        log.warning(f"Could not delete quiz answer message: {e}")


delete_answer_if_reply = delete_quiz_answer
