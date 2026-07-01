from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from src.config.enums import CreditActionType
from src.models.quiz_model import BOOLEAN_QUIZ_CREDIT_PENALTY, MULTIPLE_QUIZ_CREDIT_PENALTY, QUIZ_CREDIT_PAYOUT, QuizModel
from src.models.random_event import EffectResult, RandomEvent, RandomFailureEvent, RandomSuccessEvent
from src.models.schemas import ChatMessageRow, CreditHistoryRow

# ── EffectResult ────────────────────────────────────────────────────


class TestEffectResult:
    def test_creation(self):
        result = EffectResult(credit_change=100, message="You won!")
        assert result.credit_change == 100
        assert result.message == "You won!"

    def test_equality(self):
        a = EffectResult(credit_change=100, message="You won!")
        b = EffectResult(credit_change=100, message="You won!")
        assert a == b

    def test_inequality(self):
        a = EffectResult(credit_change=100, message="You won!")
        b = EffectResult(credit_change=200, message="You won!")
        assert a != b

    def test_repr(self):
        result = EffectResult(credit_change=-50, message="Lost it")
        assert "credit_change=-50" in repr(result)
        assert "Lost it" in repr(result)


# ── RandomEvent ─────────────────────────────────────────────────────


class TestRandomEvent:
    def test_creation(self):
        func = MagicMock(return_value=100)
        event = RandomEvent(description="test", effect_func=func, event_type="success")
        assert event.description == "test"
        assert event.event_type == "success"

    def test_apply_effect(self):
        func = MagicMock(return_value=200)
        event = RandomEvent(description="bonus", effect_func=func, event_type="success")
        result = event.apply_effect(amount=50)
        func.assert_called_once_with(amount=50)
        assert result == EffectResult(credit_change=200, message="bonus")

    def test_equality(self):
        func = MagicMock()
        a = RandomEvent(description="x", effect_func=func, event_type="success")
        b = RandomEvent(description="x", effect_func=func, event_type="success")
        assert a == b


class TestRandomFailureEvent:
    def test_event_type_is_failure(self):
        event = RandomFailureEvent(description="oops", effect_func=lambda: -100)
        assert event.event_type == "failure"

    def test_apply_effect(self):
        event = RandomFailureEvent(description="penalty", effect_func=lambda amount: -amount)
        result = event.apply_effect(amount=50)
        assert result.credit_change == -50
        assert result.message == "penalty"


class TestRandomSuccessEvent:
    def test_event_type_is_success(self):
        event = RandomSuccessEvent(description="great", effect_func=lambda: 100)
        assert event.event_type == "success"

    def test_apply_effect(self):
        event = RandomSuccessEvent(description="jackpot", effect_func=lambda amount: amount * 2)
        result = event.apply_effect(amount=50)
        assert result.credit_change == 100
        assert result.message == "jackpot"


# ── QuizModel ───────────────────────────────────────────────────────


class TestQuizModel:
    @pytest.fixture()
    def quiz(self):
        return QuizModel(
            quiz_id=1,
            user_id="123",
            question="What is 2+2?",
            difficulty="easy",
            type="multiple",
            correct_answer="4",
            display_answer="4",
            start_dt=datetime(2024, 1, 1, tzinfo=UTC),
            seconds_to_answer=15,
        )

    def test_fields_accessible(self, quiz):
        assert quiz.quiz_id == 1
        assert quiz.type == "multiple"
        assert quiz.difficulty == "easy"

    def test_type_in_repr(self, quiz):
        """type field was previously missing from dataclass fields — this validates the fix."""
        r = repr(quiz)
        assert "type='multiple'" in r

    def test_equality(self, quiz):
        other = QuizModel(
            quiz_id=1,
            user_id="123",
            question="What is 2+2?",
            difficulty="easy",
            type="multiple",
            correct_answer="4",
            display_answer="4",
            start_dt=datetime(2024, 1, 1, tzinfo=UTC),
            seconds_to_answer=15,
        )
        assert quiz == other

    @pytest.mark.parametrize(
        "quiz_type, difficulty, expected",
        [
            ("boolean", "easy", BOOLEAN_QUIZ_CREDIT_PENALTY["easy"]),
            ("boolean", "medium", BOOLEAN_QUIZ_CREDIT_PENALTY["medium"]),
            ("boolean", "hard", BOOLEAN_QUIZ_CREDIT_PENALTY["hard"]),
            ("multiple", "easy", MULTIPLE_QUIZ_CREDIT_PENALTY["easy"]),
            ("multiple", "medium", MULTIPLE_QUIZ_CREDIT_PENALTY["medium"]),
            ("multiple", "hard", MULTIPLE_QUIZ_CREDIT_PENALTY["hard"]),
        ],
    )
    def test_get_credit_penalty(self, quiz_type, difficulty, expected):
        quiz = QuizModel(
            quiz_id=1,
            user_id="123",
            question="q",
            difficulty=difficulty,
            type=quiz_type,
            correct_answer="a",
            display_answer="a",
            start_dt=datetime.now(tz=UTC),
            seconds_to_answer=10,
        )
        assert quiz.get_credit_penalty() == expected

    @pytest.mark.parametrize("difficulty", ["easy", "medium", "hard"])
    def test_get_credit_payout(self, difficulty):
        quiz = QuizModel(
            quiz_id=1,
            user_id="123",
            question="q",
            difficulty=difficulty,
            type="multiple",
            correct_answer="a",
            display_answer="a",
            start_dt=datetime.now(tz=UTC),
            seconds_to_answer=10,
        )
        assert quiz.get_credit_payout() == QUIZ_CREDIT_PAYOUT[difficulty]


# ── ChatMessageRow ──────────────────────────────────────────────────


class TestChatMessageRow:
    def test_valid_creation(self):
        row = ChatMessageRow(
            message_id=1,
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            user_id=100,
            first_name="Jan",
            last_name="Kowalski",
            username="jkowalski",
            text="hello",
            message_type="text",
        )
        assert row.message_id == 1
        assert row.user_id == 100
        assert row.reaction_emojis == []

    def test_nullable_fields(self):
        row = ChatMessageRow(
            message_id=1,
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            user_id=100,
            first_name=None,
            last_name=None,
            username=None,
            text=None,
            message_type="text",
        )
        assert row.first_name is None
        assert row.text is None

    def test_model_dump_keys(self):
        row = ChatMessageRow(
            message_id=1,
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            user_id=100,
            first_name="A",
            last_name="B",
            username="ab",
            text="hi",
            message_type="text",
        )
        d = row.model_dump()
        expected_keys = {
            "message_id",
            "timestamp",
            "user_id",
            "first_name",
            "last_name",
            "username",
            "text",
            "image_text",
            "reaction_emojis",
            "reaction_user_ids",
            "message_type",
        }
        assert set(d.keys()) == expected_keys

    def test_invalid_message_id_type(self):
        with pytest.raises(ValidationError):
            ChatMessageRow(
                message_id="not_an_int",
                timestamp=datetime(2024, 1, 1, tzinfo=UTC),
                user_id=100,
                first_name="A",
                last_name="B",
                username="ab",
                text="hi",
                message_type="text",
            )

    def test_coerces_int_from_string(self):
        """Pydantic coerces '123' → 123 by default."""
        row = ChatMessageRow(
            message_id="123",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            user_id="200",
            first_name="A",
            last_name="B",
            username="ab",
            text="hi",
            message_type="text",
        )
        assert row.message_id == 123
        assert row.user_id == 200


# ── CreditHistoryRow ───────────────────────────────────────────────


class TestCreditHistoryRow:
    def test_creation(self):
        row = CreditHistoryRow(
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            user_id=100,
            target_user_id=200,
            credit_change=50,
            action_type=CreditActionType.BET.value,
            bet_type="red",
            success=True,
        )
        assert row.user_id == 100
        assert row.credit_change == 50

    def test_to_list_order_matches_credit_history_columns(self):
        from src.config.constants import CREDIT_HISTORY_COLUMNS

        ts = datetime(2024, 1, 1, tzinfo=UTC)
        row = CreditHistoryRow(
            timestamp=ts,
            user_id=100,
            target_user_id=200,
            credit_change=50,
            action_type="bet",
            bet_type="red",
            success=True,
        )
        values = row.to_list()
        assert len(values) == len(CREDIT_HISTORY_COLUMNS)
        assert values[0] == ts
        assert values[1] == 100
        assert values[2] == 200
        assert values[3] == 50
        assert values[4] == "bet"
        assert values[5] == "red"
        assert values[6] is True

    def test_nullable_target_user_id(self):
        row = CreditHistoryRow(
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            user_id=100,
            target_user_id=None,
            credit_change=200,
            action_type="get",
            bet_type=None,
            success=True,
        )
        assert row.target_user_id is None
        assert row.to_list()[2] is None

    def test_equality(self):
        ts = datetime(2024, 1, 1, tzinfo=UTC)
        a = CreditHistoryRow(
            timestamp=ts, user_id=1, target_user_id=None, credit_change=100, action_type="get", bet_type=None, success=True
        )
        b = CreditHistoryRow(
            timestamp=ts, user_id=1, target_user_id=None, credit_change=100, action_type="get", bet_type=None, success=True
        )
        assert a == b

    def test_defaults(self):
        row = CreditHistoryRow(
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            user_id=100,
            target_user_id=None,
            credit_change=-50,
            action_type=CreditActionType.TOURNAMENT.value,
            bet_type=None,
            success=True,
        )
        assert row.bet_type is None
        assert row.success is True
