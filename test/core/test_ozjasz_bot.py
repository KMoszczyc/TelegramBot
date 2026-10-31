from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import telegram

from src.core.ozjasz_bot import OzjaszBot


class TestOzjaszBot:
    @patch("src.core.ozjasz_bot.ApplicationBuilder")
    @patch("src.core.ozjasz_bot.Assets")
    @patch("src.core.ozjasz_bot.DB")
    @patch("src.core.ozjasz_bot.BotState")
    @patch("src.core.ozjasz_bot.JobPersistance")
    @patch("src.core.ozjasz_bot.Credits")
    @patch("src.core.ozjasz_bot.Holidays")
    @patch("src.core.ozjasz_bot.CommandLogger")
    @patch("src.core.ozjasz_bot.commands.Commands")
    @patch("src.core.ozjasz_bot.ChatCommands")
    @patch("src.core.ozjasz_bot.CreditCommands")
    def test_ozjasz_bot_initialization_and_commands(
        self,
        mock_credit_commands,
        mock_chat_commands,
        mock_commands,
        mock_logger,
        mock_holidays,
        mock_credits,
        mock_job_persistance,
        mock_bot_state,
        mock_db,
        mock_assets,
        mock_app_builder,
    ):
        mock_app = MagicMock()
        mock_app_builder.return_value.token.return_value.connect_timeout.return_value.read_timeout.return_value.write_timeout.return_value.concurrent_updates.return_value.post_init.return_value.build.return_value = mock_app

        bot = OzjaszBot(test=True)

        # Verify application started polling
        mock_app.run_polling.assert_called_once()

        # Verify commands map has tournament registered
        commands_map = bot.get_commands_map()
        assert "tournament" in commands_map
        assert "quiz" in commands_map
        assert "guessperson" in commands_map

        # Verify handlers and error handler were added
        assert mock_app.add_handlers.called
        assert mock_app.add_handler.call_count >= 3
        mock_app.add_error_handler.assert_called_once_with(bot.error_handler)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("commands_count", [0, 5])
    @patch("src.core.ozjasz_bot.core_utils.get_bot_commands")
    @patch("src.core.ozjasz_bot.ApplicationBuilder")
    @patch("src.core.ozjasz_bot.Assets")
    @patch("src.core.ozjasz_bot.DB")
    @patch("src.core.ozjasz_bot.BotState")
    @patch("src.core.ozjasz_bot.JobPersistance")
    @patch("src.core.ozjasz_bot.Credits")
    @patch("src.core.ozjasz_bot.Holidays")
    @patch("src.core.ozjasz_bot.CommandLogger")
    @patch("src.core.ozjasz_bot.commands.Commands")
    @patch("src.core.ozjasz_bot.ChatCommands")
    @patch("src.core.ozjasz_bot.CreditCommands")
    async def test_ozjasz_bot_post_init(
        self,
        mock_credit_commands,
        mock_chat_commands,
        mock_commands,
        mock_logger,
        mock_holidays,
        mock_credits,
        mock_job_persistance,
        mock_bot_state,
        mock_db,
        mock_assets,
        mock_app_builder,
        mock_get_bot_commands,
        commands_count,
    ):
        mock_app = MagicMock()
        mock_app.bot.set_my_commands = AsyncMock()
        mock_app_builder.return_value.token.return_value.connect_timeout.return_value.read_timeout.return_value.write_timeout.return_value.concurrent_updates.return_value.post_init.return_value.build.return_value = mock_app
        mock_get_bot_commands.return_value = ["cmd"] * commands_count

        bot = OzjaszBot(test=True)
        await bot.post_init(mock_app)

        mock_get_bot_commands.assert_called_once()
        mock_app.bot.set_my_commands.assert_called_once_with(mock_get_bot_commands.return_value)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "transient_error",
        [
            telegram.error.TimedOut("Timed out"),
            telegram.error.NetworkError("Network error"),
        ],
    )
    @patch("src.core.ozjasz_bot.log")
    @patch("src.core.ozjasz_bot.ApplicationBuilder")
    @patch("src.core.ozjasz_bot.Assets")
    @patch("src.core.ozjasz_bot.DB")
    @patch("src.core.ozjasz_bot.BotState")
    @patch("src.core.ozjasz_bot.JobPersistance")
    @patch("src.core.ozjasz_bot.Credits")
    @patch("src.core.ozjasz_bot.Holidays")
    @patch("src.core.ozjasz_bot.CommandLogger")
    @patch("src.core.ozjasz_bot.commands.Commands")
    @patch("src.core.ozjasz_bot.ChatCommands")
    @patch("src.core.ozjasz_bot.CreditCommands")
    async def test_ozjasz_bot_error_handler_transient(
        self,
        mock_credit_commands,
        mock_chat_commands,
        mock_commands,
        mock_logger,
        mock_holidays,
        mock_credits,
        mock_job_persistance,
        mock_bot_state,
        mock_db,
        mock_assets,
        mock_app_builder,
        mock_log,
        transient_error,
    ):
        mock_app = MagicMock()
        mock_app_builder.return_value.token.return_value.connect_timeout.return_value.read_timeout.return_value.write_timeout.return_value.concurrent_updates.return_value.post_init.return_value.build.return_value = mock_app

        bot = OzjaszBot(test=True)

        context = MagicMock()
        context.error = transient_error

        await bot.error_handler(update=MagicMock(), context=context)

        mock_log.warning.assert_called_once()
        mock_log.error.assert_not_called()

    @pytest.mark.asyncio
    @patch("src.core.ozjasz_bot.log")
    @patch("src.core.ozjasz_bot.ApplicationBuilder")
    @patch("src.core.ozjasz_bot.Assets")
    @patch("src.core.ozjasz_bot.DB")
    @patch("src.core.ozjasz_bot.BotState")
    @patch("src.core.ozjasz_bot.JobPersistance")
    @patch("src.core.ozjasz_bot.Credits")
    @patch("src.core.ozjasz_bot.Holidays")
    @patch("src.core.ozjasz_bot.CommandLogger")
    @patch("src.core.ozjasz_bot.commands.Commands")
    @patch("src.core.ozjasz_bot.ChatCommands")
    @patch("src.core.ozjasz_bot.CreditCommands")
    async def test_ozjasz_bot_error_handler_unexpected(
        self,
        mock_credit_commands,
        mock_chat_commands,
        mock_commands,
        mock_logger,
        mock_holidays,
        mock_credits,
        mock_job_persistance,
        mock_bot_state,
        mock_db,
        mock_assets,
        mock_app_builder,
        mock_log,
    ):
        mock_app = MagicMock()
        mock_app_builder.return_value.token.return_value.connect_timeout.return_value.read_timeout.return_value.write_timeout.return_value.concurrent_updates.return_value.post_init.return_value.build.return_value = mock_app

        bot = OzjaszBot(test=True)

        context = MagicMock()
        unexpected_error = ValueError("Something broke")
        context.error = unexpected_error

        await bot.error_handler(update=MagicMock(), context=context)

        mock_log.warning.assert_not_called()
        mock_log.error.assert_called_once_with(f"Exception while handling an update: {unexpected_error}", exc_info=unexpected_error)
