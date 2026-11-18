import numpy as np

from src.transform import FEATURES, TARGET, build_dataset, find_year, map_col, records_to_frame
from tests.synthetic import make_records


def test_map_col_variants():
    assert map_col("Happiness.Score") == "happiness_score"
    assert map_col("Ladder score") == "happiness_score"
    assert map_col("Score") == "happiness_score"
    assert map_col("Economy (GDP per Capita)") == "gdp_per_capita"
    assert map_col("Economy..GDP.per.Capita.") == "gdp_per_capita"
    assert map_col("Family") == "social_support"
    assert map_col("Social support") == "social_support"
    assert map_col("Healthy life expectancy") == "life_expectancy"
    assert map_col("Freedom to make life choices") == "freedom"
    assert map_col("Trust (Government Corruption)") == "corruption"
    assert map_col("Perceptions of corruption") == "corruption"
    assert map_col("Overall rank") == "happiness_rank"
    assert map_col("Dystopia Residual") == "dystopia_residual"
    assert map_col("Country or region") == "country"


def test_find_year_sources():
    assert find_year({"year": 2016}) == 2016
    assert find_year({"Year": "2017"}) == 2017
    assert find_year({}, key="WHR_2018.csv") == 2018
    assert find_year({}, headers={"file": "2019.csv"}) == 2019
    assert find_year({}) is None


def test_build_dataset_shape_and_columns():
    full, data = build_dataset(make_records(n_countries=40))
    assert sorted(data["year"].unique()) == [2015, 2016, 2017, 2018, 2019]
    assert len(data) == 40 * 5
    assert list(data.columns) == ["country", "year", TARGET, *FEATURES]
    # sin columnas que causen fuga como predictoras
    assert not {"happiness_rank", "dystopia_residual"} & set(FEATURES)


def test_inconsistent_values_become_nan():
    _, data = build_dataset(make_records(n_countries=10))
    row = data[(data.country == "Country 3") & (data.year == 2018)].iloc[0]
    assert np.isnan(row["corruption"])


def test_no_duplicates_country_year():
    records = make_records(n_countries=10)
    full, _ = build_dataset(records + records)   # mensajes repetidos
    assert not full.duplicated(subset=["country", "year"]).any()


def test_year_in_payload_has_priority():
    recs = make_records(n_countries=3)
    for r in recs:
        r["_key"] = None
        r["payload"]["metadata"]["year"] = 2015
    assert records_to_frame(recs)["year"].eq(2015).all()


REAL_MSG = {"_topic": "etl.records", "_partition": 2, "_offset": 0, "_timestamp": 1794769254624,
            "_key": "2015-4", "_headers": {},
            "payload": {"metadata": {"source_file": "2015.csv", "year": 2015, "record_id": "2015-4"},
                        "data": {"Country": "Norway", "Region": "Western Europe", "Happiness Rank": 4,
                                 "Happiness Score": 7.522, "Standard Error": 0.0388,
                                 "Economy (GDP per Capita)": 1.459, "Family": 1.33095,
                                 "Health (Life Expectancy)": 0.88521, "Freedom": 0.66973,
                                 "Trust (Government Corruption)": 0.36503, "Generosity": 0.34699,
                                 "Dystopia Residual": 2.46531}}}


def test_real_message_format_metadata_data():
    """Formato real del tema etl.records: payload = {metadata, data}."""
    full, data = build_dataset([REAL_MSG])
    row = data.iloc[0]
    assert row["country"] == "Norway" and row["year"] == 2015
    assert row["happiness_score"] == 7.522 and row["freedom"] == 0.66973
    assert row["social_support"] == 1.33095


def test_flat_payload_still_supported():
    flat = {**REAL_MSG, "payload": REAL_MSG["payload"]["data"]}
    _, data = build_dataset([flat])
    assert data.iloc[0]["country"] == "Norway" and data.iloc[0]["year"] == 2015
