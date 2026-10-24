from unittest.mock import AsyncMock, MagicMock

import pytest

from src.commands.helpers.quiz_helpers import remove_tip_button, show_quiz_tip, update_quiz_message


@pytest.fixture
def mock_context():
    ctx = MagicMock()
    ctx.bot.edit_message_reply_markup = AsyncMock()
    ctx.bot.edit_message_caption = AsyncMock()
    ctx.bot.edit_message_text = AsyncMock()
    ctx.bot.send_message = AsyncMock()
    return ctx


@pytest.mark.asyncio
async def test_remove_tip_button_none_message_id(mock_context):
    await remove_tip_button(mock_context, {"message_id": None, "chat_id": 123})
    mock_context.bot.edit_message_reply_markup.assert_not_called()


@pytest.mark.asyncio
async def test_remove_tip_button_success(mock_context):
    await remove_tip_button(mock_context, {"message_id": 456, "chat_id": 123})
    mock_context.bot.edit_message_reply_markup.assert_awaited_once_with(chat_id=123, message_id=456, reply_markup=None)


@pytest.mark.asyncio
@pytest.mark.parametrize("has_photo,called_method", [(True, "edit_message_caption"), (False, "edit_message_text")])
async def test_update_quiz_message(mock_context, has_photo, called_method):
    cached_quiz = {"chat_id": 123, "message_id": 456, "has_photo": has_photo}
    await update_quiz_message(mock_context, cached_quiz, "new text", None)
    method = getattr(mock_context.bot, called_method)
    method.assert_awaited_once()


@pytest.mark.asyncio
async def test_show_quiz_tip_can_edit(mock_context, mocker):
    plan = MagicMock(can_edit=True, edit_text="tip text", keyboard=None)
    mocker.patch("src.models.quizzes.tip_message.plan_tip_display", return_value=plan)
    cached_quiz = {"chat_id": 123, "message_id": 456, "has_photo": False}
    await show_quiz_tip(mock_context, cached_quiz, 111, None, None, "A tip")
    mock_context.bot.edit_message_text.assert_awaited_once()
