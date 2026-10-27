import pytest

from src.models.quizzes.quiz_matcher import (
    find_best_top_country_match,
    get_country_acronyms,
    get_known_country_names,
    is_answer_correct,
    is_roman_numeral,
    is_stop_word,
    normalize_text,
    phrase_similarity,
)


@pytest.mark.parametrize(
    "input_text, expected",
    [
        ("People's Republic of China", "peoples republic of china"),
        ("Côte d'Ivoire", "cote divoire"),
        ("Guinea-Bissau", "guinea bissau"),
        ("São Tomé and Príncipe", "sao tome and principe"),
        ("Saint Kitts & Nevis", "saint kitts nevis"),
        ("Polska Rzeczpospolita", "polska rzeczpospolita"),
        ("Władysław Łokietek", "wladyslaw lokietek"),
        ("Kraków", "krakow"),
        ("  Spaces   and--Hyphens / Slashes_  ", "spaces and hyphens slashes"),
        ("", ""),
        (None, ""),
    ],
)
def test_normalize_text(input_text, expected):
    assert normalize_text(input_text) == expected


@pytest.mark.parametrize(
    "word, expected",
    [
        ("i", True),
        ("ii", True),
        ("iv", True),
        ("vi", True),
        ("viii", True),
        ("ix", True),
        ("x", True),
        ("xi", True),
        ("xiv", True),
        ("xix", True),
        ("xx", True),
        ("tudor", False),
        ("henryk", False),
        ("", False),
    ],
)
def test_is_roman_numeral(word, expected):
    assert is_roman_numeral(word) == expected


@pytest.mark.parametrize(
    "word, expected",
    [
        ("republic", True),
        ("Republic", True),
        ("republika", True),
        ("Republika", True),
        ("democratic", True),
        ("demokratyczna", True),
        ("united", True),
        ("zjednoczone", True),
        ("islands", True),
        ("wyspy", True),
        ("the", True),
        ("of", True),
        ("and", True),
        ("china", False),
        ("polska", False),
        ("germany", False),
        ("congo", False),
    ],
)
def test_is_stop_word(word, expected):
    assert is_stop_word(word) == expected


@pytest.mark.parametrize(
    "code, expected_acronyms",
    [
        ("cn", {"prc"}),
        ("CN", {"prc"}),
        ("us", {"usa"}),
        ("gb", {"uk"}),
        ("ae", {"uae"}),
        ("cd", {"drc"}),
        ("cf", {"car"}),
        ("kp", {"dprk"}),
        ("ru", {"ussr"}),
        ("pl", set()),
        ("", set()),
        (None, set()),
    ],
)
def test_get_country_acronyms(code, expected_acronyms):
    assert get_country_acronyms(code) == expected_acronyms


def test_get_known_country_names():
    names = get_known_country_names()
    assert isinstance(names, set)
    assert len(names) > 100
    assert "chiny" in names
    assert "china" in names
    assert "niger" in names
    assert "nigeria" in names
    assert "austria" in names
    assert "australia" in names


@pytest.mark.parametrize(
    "user_answer, valid_answers, expected",
    [
        # China variations
        pytest.param(
            "people's republic of china", {"people's republic of china", "china", "chiny", "cn", "prc"}, True, id="china_with_apostrophe"
        ),
        pytest.param(
            "peoples republic of china", {"people's republic of china", "china", "chiny", "cn", "prc"}, True, id="china_without_apostrophe"
        ),
        pytest.param("china", {"people's republic of china", "china", "chiny", "cn", "prc"}, True, id="china_simple_en"),
        pytest.param("chiny", {"people's republic of china", "china", "chiny", "cn", "prc"}, True, id="china_simple_pl"),
        pytest.param("prc", {"people's republic of china", "china", "chiny", "cn", "prc"}, True, id="china_acronym_prc"),
        pytest.param(
            "republic", {"people's republic of china", "china", "chiny", "cn", "prc"}, False, id="china_rejected_generic_republic"
        ),
        # 2-letter country code protection (no fuzzy matching 2-letter codes)
        pytest.param("ga", {"gabon", "ga"}, True, id="gabon_exact_code"),
        pytest.param("gay", {"gabon", "ga"}, False, id="gay_rejected_for_gabon"),
        pytest.param("gy", {"guyana", "gy"}, True, id="guyana_exact_code"),
        pytest.param("gay", {"guyana", "gy"}, False, id="gay_rejected_for_guyana"),
        pytest.param("pl", {"polska", "pl"}, True, id="poland_exact_code"),
        pytest.param("pol", {"polska", "pl"}, False, id="pol_rejected_for_code_pl"),
        pytest.param("us", {"united states", "usa", "us"}, True, id="usa_exact_code"),
        pytest.param("usa", {"united states", "usa", "us"}, True, id="usa_acronym"),
        # Diacritics normalization
        pytest.param("jozef", {"józef piłsudski", "józef", "piłsudski"}, True, id="diacritic_jozef"),
        pytest.param("wladyslaw", {"władysław łokietek", "władysław", "łokietek"}, True, id="diacritic_wladyslaw"),
        pytest.param("krakow", {"kraków"}, True, id="diacritic_krakow"),
        # Roman numerals protection
        pytest.param("henryk vi", {"henryk viii tudor", "henryk", "viii", "tudor"}, False, id="roman_vi_not_viii"),
        pytest.param("vii", {"viii"}, False, id="roman_vii_not_viii"),
        pytest.param("viii", {"viii"}, True, id="roman_viii_exact"),
        # Cross-country collision prevention
        pytest.param("niger", {"nigeria", "ng"}, False, id="niger_rejected_for_nigeria"),
        pytest.param("niger", {"niger", "ne"}, True, id="niger_accepted_for_niger"),
        pytest.param("austria", {"australia", "au"}, False, id="austria_rejected_for_australia"),
        pytest.param("iran", {"iraq", "iq"}, False, id="iran_rejected_for_iraq"),
        pytest.param("slovakia", {"slovenia", "si"}, False, id="slovakia_rejected_for_slovenia"),
    ],
)
def test_is_answer_correct(user_answer, valid_answers, expected):
    known = get_known_country_names()
    assert is_answer_correct(user_answer, valid_answers, known_country_names=known) == expected


def test_find_best_top_country_match_highest_similarity():
    entries_valid = [
        {"slovenia", "slowenia", "si"},
        {"slovakia", "slowacja", "sk"},
    ]
    indices = [0, 1]
    known = get_known_country_names()

    # User has a typo for Slovakia -> should pick Slovakia (index 1), not Slovenia (index 0)
    assert find_best_top_country_match("slovakya", entries_valid, indices, known) == 1
    # User has a typo for Slovenia -> should pick Slovenia (index 0)
    assert find_best_top_country_match("slovenya", entries_valid, indices, known) == 0


def test_find_best_top_country_match_rejects_collisions():
    entries_valid = [
        {"nigeria", "ng"},
    ]
    indices = [0]
    known = get_known_country_names()

    # Niger is a real country, so it must not fuzzy-match Nigeria
    assert find_best_top_country_match("niger", entries_valid, indices, known) is None
    # Real Nigeria typo matches
    assert find_best_top_country_match("nigeriaa", entries_valid, indices, known) == 0


@pytest.mark.parametrize(
    "user_text, candidate, expected_match",
    [
        ("polsk", "polska", True),
        ("polska", "polska", True),
        ("warszaw", "warszawa", True),
        ("germanyy", "germany", True),
        ("totallywrong", "polska", False),
        ("henryk vi", "henryk viii", False),
        ("vi", "viii", False),
    ],
)
def test_phrase_similarity(user_text, candidate, expected_match):
    score = phrase_similarity(user_text, candidate)
    assert (score > 0) == expected_match
