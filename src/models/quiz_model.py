from dataclasses import dataclass
from datetime import datetime

QUIZ_CREDIT_PAYOUT = {
    'easy': 15,
    'medium': 25,
    'hard': 100
}

MULTIPLE_QUIZ_CREDIT_PENALTY = {
    'easy': -5,
    'medium': -10,
    'hard': -30
}

BOOLEAN_QUIZ_CREDIT_PENALTY = {
    'easy': -15,
    'medium': -25,
    'hard': -100
}


@dataclass
class QuizModel:
    user_id: str
    question: str
    correct_answer: str
    display_answer: str
    start_dt: datetime
    seconds_to_answer: int
    difficulty: str

    def __init__(self, user_id, question, difficulty, type, correct_answer, display_answer, start_dt, seconds_to_answer):
        self.user_id = user_id
        self.question = question
        self.difficulty = difficulty
        self.type = type
        self.correct_answer = correct_answer
        self.display_answer = display_answer
        self.start_dt = start_dt
        self.seconds_to_answer = seconds_to_answer

    def get_credit_penalty(self):
        if self.type == 'boolean':
            return BOOLEAN_QUIZ_CREDIT_PENALTY[self.difficulty]
        else:
            return MULTIPLE_QUIZ_CREDIT_PENALTY[self.difficulty]

    def get_credit_payout(self):
        return QUIZ_CREDIT_PAYOUT[self.difficulty]