import pytest

from src.config.enums import Table
from src.models.db.db import DB


@pytest.fixture()
def db(monkeypatch, tmp_path):
    monkeypatch.setattr("src.models.db.db.DB_PATH", tmp_path / "test_bot.db")
    return DB()


class TestRecordAndPopUpdatedMessageIds:
    def test_round_trip(self, db):
        db.record_updated_message_ids([10, 20, 30])
        result = db.pop_updated_message_ids()
        assert sorted(result) == [10, 20, 30]

    def test_clears_after_pop(self, db):
        db.record_updated_message_ids([1, 2])
        db.pop_updated_message_ids()
        assert db.pop_updated_message_ids() == []

    def test_ignores_duplicate_ids(self, db):
        db.record_updated_message_ids([5, 5, 5])
        result = db.pop_updated_message_ids()
        assert result == [5]

    def test_empty_input(self, db):
        assert db.pop_updated_message_ids() == []


class TestLoadRowsByMessageIds:
    def _insert_chat_rows(self, db, rows):
        db.conn.executemany(
            "INSERT INTO cleaned_chat_history (message_id, timestamp, user_id, final_username, text, image_text, reaction_emojis, reaction_user_ids, message_type) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        db.conn.commit()

    def test_returns_matching_rows(self, db):
        self._insert_chat_rows(
            db,
            [
                (1, "2025-01-01T10:00:00+00:00", 111, "user_a", "hello", None, "[]", "[]", "text"),
                (2, "2025-01-01T11:00:00+00:00", 222, "user_b", "world", None, "[]", "[]", "text"),
                (3, "2025-01-01T12:00:00+00:00", 333, "user_c", "foo", None, "[]", "[]", "text"),
            ],
        )
        df = db.load_rows_by_message_ids(Table.CLEANED_CHAT_HISTORY, [1, 3])
        assert sorted(df["message_id"].tolist()) == [1, 3]

    def test_empty_message_ids_returns_empty_df(self, db):
        df = db.load_rows_by_message_ids(Table.CLEANED_CHAT_HISTORY, [])
        assert df.empty

    def test_nonexistent_ids_returns_empty_df(self, db):
        df = db.load_rows_by_message_ids(Table.CLEANED_CHAT_HISTORY, [9999])
        assert df.empty
