import difflib
import re
import unicodedata

# Generic tokens that should not serve as standalone country answers
STOP_WORDS: set[str] = {
    # English
    "republic",
    "democratic",
    "united",
    "islands",
    "island",
    "the",
    "of",
    "and",
    "federation",
    "federal",
    "state",
    "states",
    "kingdom",
    "principality",
    "union",
    # Polish
    "republika",
    "demokratyczna",
    "zjednoczone",
    "wyspy",
    "wyspa",
    "krolestwo",
    "stany",
    "panstwo",
    "zwiazek",
    "federacja",
    "ksiestwo",
    "unia",
    "i",
    "w",
    "nad",
}

# Supported multi-letter country acronyms mapping to 2-letter country codes
COUNTRY_ACRONYMS: dict[str, str] = {
    "usa": "us",
    "uk": "gb",
    "uae": "ae",
    "prc": "cn",
    "drc": "cd",
    "car": "cf",
    "dprk": "kp",
    "ussr": "ru",
}

CODE_TO_ACRONYMS: dict[str, set[str]] = {
    "us": {"usa"},
    "gb": {"uk"},
    "ae": {"uae"},
    "cn": {"prc"},
    "cd": {"drc"},
    "cf": {"car"},
    "kp": {"dprk"},
    "ru": {"ussr"},
}

ROMAN_NUMERAL_REGEX = re.compile(r"^m{0,4}(cm|cd|d?c{0,3})(xc|xl|l?x{0,3})(ix|iv|v?i{0,3})$")

# Specific character replacements for Polish and common letters
CHAR_REPLACEMENTS = {
    "ł": "l",
    "Ł": "l",
    "ø": "o",
    "Ø": "o",
    "æ": "ae",
    "Æ": "ae",
    "œ": "oe",
    "Œ": "oe",
    "ß": "ss",
    "ð": "d",
    "þ": "th",
}


def normalize_text(text: str | None) -> str:
    """Normalize text by stripping diacritics, quotes, apostrophes, and punctuation.

    Converts to lowercase and collapses extra whitespace.
    """
    if not text:
        return ""

    s = str(text).strip().lower()
    for char, rep in CHAR_REPLACEMENTS.items():
        s = s.replace(char, rep)

    # Decompose unicode to remove diacritics
    s = "".join(c for c in unicodedata.normalize("NFKD", s) if unicodedata.category(c) != "Mn")

    # Remove apostrophes and quotes entirely so people's -> peoples
    s = re.sub(r"['’`\"“”]", "", s)

    # Replace hyphens, slashes, underscores, and commas with spaces
    s = re.sub(r"[-_/,.]", " ", s)

    # Remove any remaining non-alphanumeric characters except spaces
    s = re.sub(r"[^\w\s]", "", s)

    # Collapse multiple whitespaces
    return " ".join(s.split())


def is_roman_numeral(word: str) -> bool:
    """Return True if word is a valid non-empty Roman numeral."""
    if not word:
        return False
    return bool(ROMAN_NUMERAL_REGEX.match(word))


def is_stop_word(word: str) -> bool:
    """Return True if normalized word is in the generic stop words set."""
    return normalize_text(word) in STOP_WORDS


def get_country_acronyms(country_code: str | None) -> set[str]:
    """Return common acronyms for a 2-letter country code."""
    if not country_code:
        return set()
    return CODE_TO_ACRONYMS.get(country_code.strip().lower(), set())


def phrase_similarity(user_text: str, candidate: str) -> float:
    """Return difflib similarity ratio if length difference, roman numerals, and cutoff pass, else 0.0."""
    if is_roman_numeral(candidate) or is_roman_numeral(user_text):
        return 1.0 if user_text == candidate else 0.0

    user_romans = {w for w in user_text.split() if is_roman_numeral(w)}
    cand_romans = {w for w in candidate.split() if is_roman_numeral(w)}
    if user_romans != cand_romans:
        return 0.0

    user_len = len(user_text)
    cand_len = len(candidate)
    max_len_diff = 1 if user_len <= 6 else 2
    if abs(user_len - cand_len) > max_len_diff:
        return 0.0

    cutoff = 0.82 if user_len <= 6 else 0.78
    ratio = difflib.SequenceMatcher(None, user_text, candidate).ratio()
    return ratio if ratio >= cutoff else 0.0


def _match_word_in_candidate(user_word: str, candidate_words: list[str]) -> bool:
    """Return True if user_word exactly or fuzzily matches a word in candidate_words."""
    if user_word in candidate_words:
        return True

    if len(user_word) <= 3 or is_roman_numeral(user_word):
        return False

    matches = difflib.get_close_matches(user_word, candidate_words, n=1, cutoff=0.78)
    if not matches:
        return False

    max_diff = 1 if len(user_word) <= 6 else 2
    return abs(len(user_word) - len(matches[0])) <= max_diff


def _matches_multiword_tokens(user_words: list[str], candidate: str) -> bool:
    """Check if all user words match words inside candidate in order or subset."""
    cand_words = candidate.split()
    if len(cand_words) < len(user_words):
        return False
    return all(_match_word_in_candidate(word, cand_words) for word in user_words)


def is_answer_correct(
    user_answer: str,
    valid_answers: set[str],
    known_country_names: set[str] | None = None,
) -> bool:
    """Determine whether *user_answer* correctly matches any entry in *valid_answers*.

    Enforces:
    - 2-letter inputs require strict exact match against raw valid answers.
    - Collision prevention: guesses that are another country's exact name do not fuzzy-match.
    - Inputs <= 3 characters require exact match (0 typos).
    - Roman numerals require exact match.
    - Full phrase matching and token matching with diacritic and punctuation normalization.
    """
    raw_user = user_answer.strip().lower()
    if not raw_user:
        return False

    # 1. 2-letter inputs: strict exact match only
    if len(raw_user) <= 2:
        return raw_user in {a.strip().lower() for a in valid_answers}

    clean_user = normalize_text(user_answer)
    if not clean_user:
        return False

    norm_valid = {normalize_text(a) for a in valid_answers if a}
    non_two_letter_valids = {a for a in norm_valid if len(a) > 2}

    # 2. Direct normalized exact match
    if clean_user in non_two_letter_valids:
        return True

    # 3. Guard against collisions, short inputs, and roman numerals
    if (known_country_names and clean_user in known_country_names) or len(clean_user) <= 3 or is_roman_numeral(clean_user):
        return False

    # 4. Full phrase fuzzy matching against non-2-letter valid answers
    if any(phrase_similarity(clean_user, cand) > 0 for cand in non_two_letter_valids):
        return True

    # 5. Word-by-word token matching (e.g. 'katarzyna wielk' -> 'katarzyna ii wielka')
    user_words = clean_user.split()
    return len(user_words) > 1 and any(_matches_multiword_tokens(user_words, cand) for cand in non_two_letter_valids)


def find_best_top_country_match(
    user_answer: str,
    entries_valid_answers: list[set[str]],
    indices: list[int],
    known_country_names: set[str] | None = None,
) -> int | None:
    """Find the best matching index among *indices* for *user_answer*.

    First checks exact matches, then evaluates similarity scores and selects
    the candidate with the highest similarity.
    """
    raw_user = user_answer.strip().lower()
    if not raw_user:
        return None

    # Check 2-letter exact matches first
    if len(raw_user) <= 2:
        for idx in indices:
            raw_valids = {a.strip().lower() for a in entries_valid_answers[idx]}
            if raw_user in raw_valids:
                return idx
        return None

    clean_user = normalize_text(user_answer)
    if not clean_user:
        return None

    # Check exact matches on normalized text
    for idx in indices:
        norm_valid = {normalize_text(a) for a in entries_valid_answers[idx] if a}
        if clean_user in norm_valid:
            return idx

    # Reject non-eligible inputs (collisions, short inputs <= 3, roman numerals)
    if (known_country_names and clean_user in known_country_names) or len(clean_user) <= 3 or is_roman_numeral(clean_user):
        return None

    # Score each eligible candidate
    best_index: int | None = None
    best_score: float = 0.0

    for idx in indices:
        norm_valid = {normalize_text(a) for a in entries_valid_answers[idx] if len(normalize_text(a)) > 2}
        cand_score = max((phrase_similarity(clean_user, cand) for cand in norm_valid), default=0.0)

        if cand_score > best_score:
            best_score = cand_score
            best_index = idx

    return best_index


_ALL_COUNTRY_NAMES_CACHE: set[str] | None = None


def get_known_country_names() -> set[str]:
    """Return a cached set of all normalized Polish and English country names from Countries."""
    global _ALL_COUNTRY_NAMES_CACHE
    if _ALL_COUNTRY_NAMES_CACHE is None:
        try:
            from src.models.data.countries import Countries

            c = Countries()
            names = set()
            for _, row in c.df.iterrows():
                n_pl = normalize_text(row.get("country_name"))
                n_en = normalize_text(row.get("country_name_en"))
                if n_pl:
                    names.add(n_pl)
                    for p in n_pl.split():
                        if len(p) >= 3 and not is_stop_word(p):
                            names.add(p)
                if n_en:
                    names.add(n_en)
                    for p in n_en.split():
                        if len(p) >= 3 and not is_stop_word(p):
                            names.add(p)
            _ALL_COUNTRY_NAMES_CACHE = names
        except Exception:
            _ALL_COUNTRY_NAMES_CACHE = set()
    return _ALL_COUNTRY_NAMES_CACHE
