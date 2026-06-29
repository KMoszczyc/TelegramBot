import logging

from pydantic_settings import BaseSettings

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)


class Settings(BaseSettings):
    RUNTIME_ENV: str | None = None
    TOKEN: str | None = None
    TEST_TOKEN: str | None = None
    CHAT_ID: int | None = None
    TEST_CHAT_ID: int | None = None
    API_ID: int | None = None
    API_HASH: str | None = None
    SESSION: str | None = None
    BOT_ID: int | None = None


settings = Settings()

RUNTIME_ENV = settings.RUNTIME_ENV
TOKEN = settings.TOKEN
TEST_TOKEN = settings.TEST_TOKEN
CHAT_ID = settings.CHAT_ID
TEST_CHAT_ID = settings.TEST_CHAT_ID
API_ID = settings.API_ID
API_HASH = settings.API_HASH
SESSION = settings.SESSION
BOT_ID = settings.BOT_ID

log.info(f"============ RUNTIME ENVIRONMENT: {RUNTIME_ENV} ============")
