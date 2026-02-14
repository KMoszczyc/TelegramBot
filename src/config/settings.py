import logging
import os

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:
    load_dotenv = None

if load_dotenv is not None:
    load_dotenv()

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)

TOKEN = os.getenv("TOKEN")
TEST_TOKEN = os.getenv("TEST_TOKEN")
CHAT_ID = int(os.getenv("CHAT_ID")) if os.getenv("CHAT_ID") else None
TEST_CHAT_ID = int(os.getenv("TEST_CHAT_ID")) if os.getenv("TEST_CHAT_ID") else None
API_ID = int(os.getenv("API_ID")) if os.getenv("API_ID") else None
API_HASH = os.getenv("API_HASH")
SESSION = os.getenv("SESSION")
BOT_ID = int(os.getenv("BOT_ID")) if os.getenv("BOT_ID") else None
