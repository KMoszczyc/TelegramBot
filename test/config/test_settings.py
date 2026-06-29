import pytest
from pydantic import ValidationError

from src.config.settings import Settings


class TestSettings:
    def test_settings_loads_from_env(self, monkeypatch):
        monkeypatch.setenv("RUNTIME_ENV", "test")
        monkeypatch.setenv("TOKEN", "test-token-123")
        monkeypatch.setenv("CHAT_ID", "12345")
        monkeypatch.setenv("API_ID", "99999")
        monkeypatch.setenv("BOT_ID", "77777")

        s = Settings()
        assert s.RUNTIME_ENV == "test"
        assert s.TOKEN == "test-token-123"
        assert s.CHAT_ID == 12345
        assert s.API_ID == 99999
        assert s.BOT_ID == 77777

    def test_settings_defaults_to_none(self, monkeypatch):
        monkeypatch.delenv("RUNTIME_ENV", raising=False)
        monkeypatch.delenv("TOKEN", raising=False)
        monkeypatch.delenv("CHAT_ID", raising=False)
        monkeypatch.delenv("TEST_CHAT_ID", raising=False)
        monkeypatch.delenv("API_ID", raising=False)
        monkeypatch.delenv("API_HASH", raising=False)
        monkeypatch.delenv("SESSION", raising=False)
        monkeypatch.delenv("BOT_ID", raising=False)
        monkeypatch.delenv("TEST_TOKEN", raising=False)

        s = Settings()
        assert s.RUNTIME_ENV is None
        assert s.TOKEN is None
        assert s.CHAT_ID is None

    @pytest.mark.parametrize(
        "env_val",
        ["not_a_number", "12.5", ""],
    )
    def test_settings_invalid_int_raises(self, monkeypatch, env_val):
        monkeypatch.setenv("CHAT_ID", env_val)
        monkeypatch.delenv("TOKEN", raising=False)
        with pytest.raises(ValidationError):
            Settings()
