import pandas as pd
import pytest

from src.models.countries import Countries


@pytest.fixture
def mock_countries_df():
    data = {
        "country_name": ["Polska", "Niemcy", "Chiny", "Brazylia", "Egipt", "Kanada", "Słowacja", "Malta"],
        "country_code": ["PL", "DE", "CN", "BR", "EG", "CA", "SK", "MT"],
        "continent": ["Europa", "Europa", "Azja", "Ameryka Południowa", "Afryka", "Ameryka Północna", "Europa", "Europa"],
        "population": [38000000, 83000000, 1400000000, 215000000, 104000000, 38000000, 5400000, 500000],
        "capital": ["Warszawa", "Berlin", "Pekin", "Brasilia", "Kair", "Ottawa", "Bratysława", "Valletta"],
    }
    return pd.DataFrame(data)


def test_countries_init_from_df(mock_countries_df):
    countries = Countries(data=mock_countries_df)
    assert len(countries.df) == len(mock_countries_df)
    assert "easiness_score" in countries.df.columns
    assert "difficulty" in countries.df.columns


def test_easiness_score_eu_multiplier(mock_countries_df):
    countries = Countries(data=mock_countries_df)
    df = countries.df

    polska_row = df[df["country_name"] == "Polska"].iloc[0]
    # Polska pop: 38,000,000 * 10.0 / 100,000 = 3,800.0
    assert polska_row["easiness_score"] == pytest.approx(3800.0)

    chiny_row = df[df["country_name"] == "Chiny"].iloc[0]
    # Chiny pop: 1,400,000,000 * 1.0 / 100,000 = 14,000.0
    assert chiny_row["easiness_score"] == pytest.approx(14000.0)


def test_difficulty_partitioning(mock_countries_df):
    countries = Countries(data=mock_countries_df)
    df = countries.df

    # With 8 rows, 4 difficulties => exactly 2 per difficulty
    for diff in ["easy", "medium", "hard", "crazy"]:
        assert len(df[df["difficulty"] == diff]) == 2


@pytest.mark.parametrize(
    "difficulty, expected_count",
    [
        ("easy", 2),
        ("medium", 2),
        ("hard", 2),
        ("crazy", 2),
    ],
)
def test_get_countries_by_difficulty(mock_countries_df, difficulty, expected_count):
    countries = Countries(data=mock_countries_df)
    res = countries.get_countries(difficulty=difficulty)
    assert len(res) == expected_count
    assert (res["difficulty"] == difficulty).all()


@pytest.mark.parametrize(
    "continent_arg, expected_names",
    [
        ("Europa", {"Polska", "Niemcy", "Słowacja", "Malta"}),
        ("europe", {"Polska", "Niemcy", "Słowacja", "Malta"}),
        ("eu", {"Polska", "Niemcy", "Słowacja", "Malta"}),
        ("Azja", {"Chiny"}),
        ("afryka", {"Egipt"}),
        ("na", {"Kanada"}),
        ("sa", {"Brazylia"}),
    ],
)
def test_get_countries_by_continent_and_aliases(mock_countries_df, continent_arg, expected_names):
    countries = Countries(data=mock_countries_df)
    res = countries.get_countries(continent=continent_arg)
    assert set(res["country_name"]) == expected_names


def test_get_available_difficulties(mock_countries_df):
    countries = Countries(data=mock_countries_df)
    # Overall all 4 difficulties available
    diffs = countries.get_available_difficulties()
    assert set(diffs) == {"easy", "medium", "hard", "crazy"}

    # For Azja, only Chiny exists (1 row), so exactly 1 difficulty will be present
    azja_diffs = countries.get_available_difficulties(continent="Azja")
    assert len(azja_diffs) == 1


def test_countries_init_from_parquet_default():
    countries = Countries()
    assert not countries.df.empty
    assert set(countries.df["difficulty"].unique()) == {"easy", "medium", "hard", "crazy"}


def test_pop_random_country_covers_all_countries(mock_countries_df):
    """Every country must appear exactly once before any country repeats."""
    countries = Countries(data=mock_countries_df)
    n = len(countries.df)
    seen = [countries.pop_random_country()["country_code"] for _ in range(n)]
    assert sorted(seen) == sorted(countries.df["country_code"].tolist())


def test_pop_random_country_refills_after_exhaustion(mock_countries_df):
    """After exhausting the full queue a second cycle returns all countries again."""
    countries = Countries(data=mock_countries_df)
    n = len(countries.df)
    first_cycle = {countries.pop_random_country()["country_code"] for _ in range(n)}
    second_cycle = {countries.pop_random_country()["country_code"] for _ in range(n)}
    all_codes = set(countries.df["country_code"].tolist())
    assert first_cycle == all_codes
    assert second_cycle == all_codes


def test_pop_random_country_filtered_does_not_use_queue(mock_countries_df):
    """When filtered_df is passed the queue is bypassed and the result is in the filtered set."""
    countries = Countries(data=mock_countries_df)
    europa_df = countries.get_countries(continent="Europa")
    europa_codes = set(europa_df["country_code"].tolist())

    result = countries.pop_random_country(filtered_df=europa_df)
    assert result["country_code"] in europa_codes
    # Queue must remain untouched (still empty / not consumed)
    assert len(countries._shuffle_queue) == 0
