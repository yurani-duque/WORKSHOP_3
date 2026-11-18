import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer

from src.state import PipelineState, apply_event, record_predictions
from src.stream_predict import process_batch, validate
from src.transform import FEATURES, build_dataset
from tests.synthetic import make_records


def _model():
    _, data = build_dataset(make_records(30))
    pipe = Pipeline([("imputer", SimpleImputer(strategy="median")), ("model", LinearRegression())])
    return pipe.fit(data[FEATURES], data["happiness_score"])


def test_state_dedup_by_offset_and_event_id():
    st = PipelineState()
    recs = make_records(3)
    assert all(apply_event(st, r) for r in recs[:5])
    assert not apply_event(st, recs[0])                       # mismo (partición, offset)
    a = {"_partition": 1, "_offset": 1, "payload": {"event_id": 7}}
    b = {"_partition": 2, "_offset": 9, "payload": {"event_id": 7}}   # mismo event_id, otro offset
    assert apply_event(st, a) and not apply_event(st, b)
    assert st.duplicates == 2 and st.last_event is a


def test_process_batch_predicts_and_rejects():
    model = _model()
    recs = make_records(5)[:6]
    bad = {"_partition": 0, "_offset": 999, "_key": "WHR_2015.csv", "_headers": {}, "_topic": "t", "_timestamp": 0,
           "payload": {"Country": "Bad", "Happiness Score": 5, "Economy (GDP per Capita)": 99}}
    out = process_batch(recs + [bad], model)
    assert len(out) == 7
    # harmonize agrupa por año, así que se localiza cada fila por su offset (no por posición)
    bad_row = out[out["kafka_offset"] == 999].iloc[0]
    good = out[out["kafka_offset"] != 999]
    assert (good["status"] == "ok").all() and bad_row["status"] == "rejected"
    assert good["predicted_score"].notna().all() and np.isnan(bad_row["predicted_score"])
    assert (good["predicted_score"] - good["actual_score"]).abs().max() < 1.0


def test_validate_rules():
    df = pd.DataFrame({"country": ["A", "B", "C"], "gdp_per_capita": [1, 9, 1], "social_support": [1, 1, np.nan],
                       "life_expectancy": [1, 1, np.nan], "freedom": [1, 1, np.nan], "corruption": [1, 1, np.nan],
                       "generosity": [1, 1, 1]})
    r = validate(df)
    assert r[0] == "" and r[1] == "predictora fuera de rango" and r[2] == "faltan predictoras"


def test_record_predictions_counts_alerts():
    st = PipelineState()
    record_predictions(st, [{"status": "ok"}, {"status": "rejected"}])
    assert st.alerts == 1 and len(st.predictions) == 2
