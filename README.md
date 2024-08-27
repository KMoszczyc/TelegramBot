# Telegram Bot - Chat Statistics and More

## Overview
This Telegram bot provides detailed chat statistics, including top messages, memes, videos, charts and other metrics. Additionally, it offers extra functionalities such as random quotes by Janusz Korwin-Mikke, TVP headlines and the entire Bible, all fully searchable by keywords and regex.

## Features
- **Chat Statistics**: Pull all the historical chat messages and media and update it every 5 minutes (or less if you change it in `docker/cron_etl`).
- **Top messages/memes/etc**: Display top messages/images/videos/gifs/audio by the number of reactions
- **Filters**: Filtering the stats by time period/user
- **Charts**: Display activity charts
- **Extras**: Quotes by Janusz Korwin Mikke, Jacek Bartosiak, TVP headlines and the entire fucking Bible that's fully searchable by regex and filter phrases.

<p float="left">
    <img src="https://github.com/user-attachments/assets/d34015d3-e4bf-4037-b1b3-9369f2378adc" width="400" /> 
    <img src="https://github.com/user-attachments/assets/92bf63d0-6b5e-4e7d-9069-7014576dfbb3" width="400" /> 
    <img src="https://github.com/user-attachments/assets/75df4193-716d-4aaf-802a-a10a2a7c3460" width="400" />
    <img src="https://github.com/user-attachments/assets/5f716e96-823a-4949-b1b1-b0ee92986fb5" width="400" />
     <img src="https://github.com/user-attachments/assets/c24f133e-49c4-4e43-8a0b-7654347b8573"  width="250"/>
<img src="https://github.com/user-attachments/assets/19ad79d1-fa4d-453d-b22d-787279330dff"  width="250"/>
    <img src="https://github.com/user-attachments/assets/e77a3290-ebd1-439b-a94a-66ae34b709b5"  width="250"/>
</p>

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
    - **`addnickname (new_nickname)`** - Add a new nickname, making it easier for you and others to use filtering.
    - **`bartosiak (text_filter or [regex])`** - Best of our favorite geopolitician Jacek Bartosiak.
    - **`bible (text_filter)`** - Just a whole Bible.
    - **`displayusers`** - Display all existing users on the chat and their nicknames.
    - **`fun (period[optional])`** - Fun metric for all users.
    - **`funchart (username[optional], period[optional])`** - Fun metric chart.
    - **`help`** - A list of commands.
    - **`lastmessages (username, number[optional])`** - Display last x messages from chat history.
    - **`likechart (username[optional], period[optional])`** - Display the number of reactions received per day per user.
    - **`ozjasz (text_filter or [regex])`** - Best of Cyborg aus der Zukunft.
    - **`sadmemes (username[optional], period[optional])`** - Top sad memes (images) sorted by the number of reactions.
    - **`sadmessages (username[optional], period[optional])`** - Top sad messages sorted by the number of reactions, filtered on negative emojis: `['👎', '😢', '😭', '🤬', '🤡', '💩', '😫', '😩', '🥶', '🤨', '🧐', '🙃', '😒', '😠', '😣', '🗿']`.
    - **`setusername (new_username)`** - Set your new username.
    - **`spamchart (username[optional], period[optional])`** - Display the number of messages per day per user.
    - **`starababa`** - Using the current date, e.g., 20240709, and the last two digits of your user_id, a magic number is created. If it's prime, you are lucky today!
    - **`summary (username[optional], period[optional])`** - Summary of chat stats.
    - **`topaudio (username[optional], period[optional])`** - Top audio (and voice messages) sorted by the number of reactions.
    - **`topgifs (username[optional], period[optional])`** - Top GIFs sorted by the number of reactions.
    - **`topmemes (username[optional], period[optional])`** - Top memes (images) sorted by the number of reactions.
    - **`topmessages (username[optional], period[optional])`** - Top messages sorted by the number of reactions.
    - **`topvideos (username[optional], period[optional])`** - Top videos (and video notes) sorted by the number of reactions.
    - **`tvp (text_filter or [regex])`** - All TVP headlines from the past few years.
    - **`tvp_latest (text_filter or [regex])`** - Latest TVP headlines.
    - **`tusk (text_filter or [regex])`** - Epic TVP Tusk reference.
    - **`wholesome (period[optional])`** - Summary metric for all users.


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
