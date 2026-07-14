from unittest.mock import AsyncMock, MagicMock, patch

import pytest

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
        mock_app_builder.return_value.token.return_value.read_timeout.return_value.write_timeout.return_value.concurrent_updates.return_value.post_init.return_value.build.return_value = mock_app

        bot = OzjaszBot(test=True)

        # Verify application started polling
        mock_app.run_polling.assert_called_once()

        # Verify commands map has tournament registered
        commands_map = bot.get_commands_map()
        assert "tournament" in commands_map
        assert "quiz" in commands_map
        assert "guessperson" in commands_map

        # Verify handlers were added
        assert mock_app.add_handlers.called
        assert mock_app.add_handler.call_count >= 3

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
        mock_app_builder.return_value.token.return_value.read_timeout.return_value.write_timeout.return_value.concurrent_updates.return_value.post_init.return_value.build.return_value = mock_app
        mock_get_bot_commands.return_value = ["cmd"] * commands_count

        bot = OzjaszBot(test=True)
        await bot.post_init(mock_app)

        mock_get_bot_commands.assert_called_once()
        mock_app.bot.set_my_commands.assert_called_once_with(mock_get_bot_commands.return_value)
