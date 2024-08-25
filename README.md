# Telegram Bot - Chat Statistics and More

## Overview
This Telegram bot provides detailed chat statistics, including message counts, reactions, and other metrics. Additionally, it offers extra functionalities such as generating top user lists, tracking specific keywords, and providing fun facts about the chat activity.

## Features
- **Chat Statistics**: Pull all the historical chat messages and media and update it every 5 minutes (or less if you change it in `docker/cron_etl`).
- **Top messages/memes/etc**: Display top messages/images/videos/gifs/audio by the number of reactions
- **Filters**: Filtering the stats by time period/user
- **Charts**: Display activity charts
- **Extras**: Quotes by Janusz Korwin Mikke, Jacek Bartosiak, TVP headlines and the entire fucking Bible that's fully searchable by regex and filter phrases.

## Installation
1. Clone the repository:
    ```bash
    git clone https://github.com/KMoszczyc/TelegramBot.git
    ```

2. Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```

3. Set up your environment variables (e.g., Telegram API key) in a `.env` file:
    ```
    TOKEN=
    API_ID=
    API_HASH=
    CHAT_ID=
    BOT_ID=
    SESSION=
    ```
 4. Run `docker compose up -d --build`
    - which will run the bot via `src/main.py` and pull the chat messages every 5 minutes via `src/main_etl.py`, both put into  `telegram-bot` and  `telegram-bot-etl` containers respectively.

## Usage
1. Either use docker or run `python src/main.py`

2. Add the bot to your Telegram group and use the following commands:
    - `/addnickname (new_nickname)` - Add a new nickname, so it's easier for you and others to use filtering.
    - `/bartosiak text_filter or [regex]` - Best of our favourite geopolitician Jacek Bartosiak.
    - `/displayusers` - Just display all existing users on the chat and their nicknames.
    - `/fun (period[optional])` - Fun metric for all users.
    - `/funchart (username[optional], period[optional])` - Fun metric chart.
    - `/help` - A list of commands.
    - `/lastmessages (username, number[optional])` - Display last x messages from chat history.
    - `/ozjasz text_filter or [regex]` - Best of Cyborg aus der Zukunft.
    - `/sadmemes (username[optional], period[optional])` - Top sad memes (images) sorted by a number of reactions.
    - `/sadmessages (username[optional], period[optional])` - Top sad messages sorted by a number of reactions, filtered on negative emojis: `['👎', '😢', '😭', '🤬', '🤡', '💩', '😫', '😩', '🥶', '🤨', '🧐', '🙃', '😒', '😠', '😣']`.
    - `/setusername (new_username)` - Set your new username.
    - `/starababa` - Using the current date (e.g., `20240709`) and the last 2 digits of your user ID, a magic number is created. If it's prime, you are lucky today!
    - `/summary (username[optional], period[optional])` - Summary of chat stats.
    - `/topaudio (username[optional], period[optional])` - Top audio (and voice messages) sorted by a number of reactions.
    - `/topgifs (username[optional], period[optional])` - Top gifs sorted by a number of reactions.
    - `/topmemes (username[optional], period[optional])` - Top memes (images) sorted by a number of reactions.
    - `/topmessages (username[optional], period[optional])` - Top messages sorted by a number of reactions.
    - `/topvideos (username[optional], period[optional])` - Top videos (and video notes) sorted by a number of reactions.
    - `/tvp text_filter or [regex]` - All TVP headlines from the past few years.
    - `/tvp_latest text_filter or [regex]` - Latest TVP headlines.
    - `/tusk text_filter or [regex]` - Epic TVP Tusk reference.
    - `/wholesome (period[optional])` - Summary metric for all users.

## Project Structure
- `data/` - all the data used in the project. That's also were chat messages are saved into.
- `docker/` - dockerfiles, cron_file
- `src/` - Project code
  - `core/` - Core bot logic.
  - `models/` - Data models (e.g., `CommandArgs`).
  - `stats/` - Chat statistics related functions,
  - `main.py` - entry point for running the Bot.
  - `main_etl.py` - entry point for running the chat etl process (cron).
- `test/` - Unit and integration tests.
- `definitions.py` - Constants and enums used throughout the project.

## Contributing
Feel free to submit issues or pull requests. Contributions are welcome!

## License
This project is licensed under the MIT License.
