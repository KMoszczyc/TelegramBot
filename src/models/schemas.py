import pandera as pa
from pandera.engines import pandas_engine

from definitions import TIMEZONE

chat_history_schema = pa.DataFrameSchema({
    "message_id": pa.Column(int),  # int64
    "timestamp": pa.Column(pandas_engine.DateTime(tz=TIMEZONE)),
    "user_id": pa.Column(int),  # object (string)
    "first_name": pa.Column(str, nullable=True),  # string
    "last_name": pa.Column(str, nullable=True),  # string
    "username": pa.Column(str, nullable=True),  # string
    "text": pa.Column(str, nullable=True),  # string
    "image_text": pa.Column(str, nullable=True),  # string (nullable)
    "reaction_emojis": pa.Column(object, nullable=True),  # list (nullable)
    "reaction_user_ids": pa.Column(object, nullable=True),  # list (nullable)
    "message_type": pa.Column(str)  # string
}, name="chat_history")

# 1. Cleaned Chat History Schema
cleaned_chat_history_schema = pa.DataFrameSchema({
    'message_id': pa.Column(int),  # int64
    'timestamp': pa.Column(pandas_engine.DateTime(tz=TIMEZONE)),  # datetime64[ns]
    'user_id': pa.Column(int),  # string
    'final_username': pa.Column(str),  # string
    'text': pa.Column(str, nullable=True),  # string
    "image_text": pa.Column(str, nullable=True),
    'reaction_emojis': pa.Column(object, nullable=True),  # list (nullable)
    'reaction_user_ids': pa.Column(object, nullable=True),  # list (nullable)
    'message_type': pa.Column(str)  # string
}, name="cleaned_chat_history")

# 2. Commands Usage Schema
commands_usage_schema = pa.DataFrameSchema({
    'timestamp': pa.Column(pandas_engine.DateTime(tz=TIMEZONE)),  # datetime64[ns]
    'user_id': pa.Column(int),  # string
    'command_name': pa.Column(str)  # string
}, name="commands_usage")

# 3. Reactions Schema
reactions_schema = pa.DataFrameSchema({
    'message_id': pa.Column(int),  # string
    'timestamp': pa.Column(pandas_engine.DateTime(tz=TIMEZONE)),  # datetime64[ns]
    'reacted_to_username': pa.Column(str),  # string
    'reacting_username': pa.Column(str),  # string
    'text': pa.Column(str, nullable=True),  # string
    'emoji': pa.Column(str)  # string
}, name="reactions")

# 4. Users Schema
users_schema = pa.DataFrameSchema({
    'first_name': pa.Column(str, nullable=True),  # string
    'last_name': pa.Column(str, nullable=True),  # string
    'username': pa.Column(str, nullable=True),  # string
    'final_username': pa.Column(str),  # string
    'nicknames': pa.Column(object, nullable=True)  # list (nullable)
}, index=pa.Index(int, name="user_id"), name="users")

# 5. Cwel schema
cwel_schema = pa.DataFrameSchema({
    'timestamp': pa.Column(pandas_engine.DateTime(tz=TIMEZONE)),  # datetime64[ns]
    'receiver_username': pa.Column(str),
    'giver_username': pa.Column(str),
    'reply_message_id': pa.Column(int),
    'value': pa.Column(int)  # string
})

# 6. Credit History ['timestamp', 'user_id', 'robbed_user_id','credit_change', 'action_type', 'bet_type', 'success']
credit_history_schema = pa.DataFrameSchema({
    'timestamp': pa.Column(pandas_engine.DateTime(tz=TIMEZONE)),  # datetime64[ns]
    'user_id': pa.Column(int),
    'robbed_user_id': pa.Column(int),
    'credit_change': pa.Column(int),
    'action_type': pa.Column(str),
    'bet_type': pa.Column(str),
    'success': pa.Column(bool)
})


#
# chat_history_schema = {
#     'message_id': "int64",
#     'timestamp': "datetime64[ns]",
#     'user_id': "object",
#     'first_name': "string",
#     'last_name': "string",
#     'username': "string",
#     'text': "string",
#     'image_text': "string",
#     'reaction_emojis': "list",
#     'reaction_user_ids': "list",
#     'message_type': "string"
# }
#
# cleaned_chat_history_schema = {
#     'message_id': "int64",
#     'timestamp': "datetime64[ns]",
#     'user_id': "string",
#     'final_username': "string",
#     'text': "string",
#     'reaction_emojis': "list",
#     'reaction_user_ids': "list",
#     'message_type': "string"
# }
#
# commands_usage_schema = {
#     'timestamp': "datetime64[ns]",
#     'user_id': "string",
#     'command_name': "string"
# }
#
# reactions_schema = {
#     'message_id': "string",
#     'timestamp': "datetime64[ns]",
#     'reacted_to_username': "string",
#     'reacting_username': "string",
#     'text': "string",
#     'emoji': "string"
# }
#
# users_schema = {
#     'user_id': "string",
#     'first_name': "string",
#     'last_name': "string",
#     'username': "string",
#     'final_username': "string",
#     'nicknames': "list"
# }
