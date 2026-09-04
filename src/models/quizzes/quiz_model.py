from dataclasses import dataclass
from datetime import datetime

QUIZ_CREDIT_PAYOUT = {"easy": 50, "medium": 100, "hard": 250}

MULTIPLE_QUIZ_CREDIT_PENALTY = {"easy": -20, "medium": -40, "hard": -100}

BOOLEAN_QUIZ_CREDIT_PENALTY = {"easy": -50, "medium": -100, "hard": -250}


@dataclass
class QuizModel:
    quiz_id: int
    user_id: str
    question: str
    difficulty: str
    type: str
    correct_answer: str
    display_answer: str
    start_dt: datetime
    seconds_to_answer: int

    def get_credit_penalty(self):
        if self.type == "boolean":
            return BOOLEAN_QUIZ_CREDIT_PENALTY[self.difficulty]
        else:
            return MULTIPLE_QUIZ_CREDIT_PENALTY[self.difficulty]

    def get_credit_payout(self):
        return QUIZ_CREDIT_PAYOUT[self.difficulty]
