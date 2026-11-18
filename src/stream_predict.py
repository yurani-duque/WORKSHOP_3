"""Consumer de APLICACIÓN (diapositivas S12): Kafka -> validación -> inferencia -> estado -> base de datos.

- El modelo se carga UNA vez (artefacto serializado); no se reentrena por evento.
- Cada lote se valida, se predice y se guarda en SQLite (tabla stream_predictions).
- El offset se confirma DESPUÉS de guardar en la base (al-menos-una-vez) y la inserción es
  idempotente por (partición, offset): reprocesar un mensaje no duplica filas.
"""
import argparse

import numpy as np
import pandas as pd

from src import config, db
from src.kafka_extract import message_to_record
from src.model_io import load_artifacts
from src.state import PipelineState, apply_event, record_predictions
from src.transform import FEATURES, TARGET, clean, harmonize, records_to_frame

FEATURE_RANGE = (0.0, 3.0)     # los factores del reporte están aprox. en [0, 2]
SCORE_RANGE = (0.0, 10.0)
MIN_FEATURES_PRESENT = 4       # de las 6 predictoras


def validate(df: pd.DataFrame) -> pd.Series:
    """Devuelve una razón de rechazo por fila ('' si el registro es válido)."""
    X = df.reindex(columns=FEATURES)
    reasons = pd.Series("", index=df.index, dtype=object)
    reasons[X.notna().sum(axis=1) < MIN_FEATURES_PRESENT] = "faltan predictoras"
    out_of_range = ((X < FEATURE_RANGE[0]) | (X > FEATURE_RANGE[1])).any(axis=1)
    reasons[out_of_range & (reasons == "")] = "predictora fuera de rango"
    if TARGET in df.columns:
        bad_score = df[TARGET].notna() & ~df[TARGET].between(*SCORE_RANGE)
        reasons[bad_score & (reasons == "")] = "score fuera de rango"
    no_country = df["country"].isna() if "country" in df.columns else pd.Series(True, index=df.index)
    reasons[no_country & (reasons == "")] = "sin país"
    return reasons


def process_batch(batch: list[dict], model) -> pd.DataFrame:
    df = clean(harmonize(records_to_frame(batch), keep_meta=True), strict=False)
    reasons = validate(df)
    ok = reasons == ""
    pred = pd.Series(np.nan, index=df.index)
    if ok.any():
        pred[ok] = model.predict(df.loc[ok].reindex(columns=FEATURES))
    return pd.DataFrame({
        "country": df.get("country"),
        "year": df.get("year"),
        "actual_score": df.get(TARGET, pd.Series(np.nan, index=df.index)),
        "predicted_score": pred,
        "status": np.where(ok, "ok", "rejected"),
        "reason": reasons,
        "kafka_partition": df["_partition"],
        "kafka_offset": df["_offset"],
        "processed_at": pd.Timestamp.now(tz="UTC").isoformat(),
    })


def run(batch_size: int = 50, follow: bool = False) -> PipelineState:
    from confluent_kafka import Consumer, KafkaError   # import local: los tests no lo requieren

    model, meta = load_artifacts()
    if meta.get("features") and meta["features"] != FEATURES:
        raise RuntimeError("Las features del modelo no coinciden con transform.FEATURES: reentrena.")
    print(f"[stream] modelo: {meta.get('model', '?')} | features: {FEATURES}")

    base = config.kafka_conf()["group.id"]
    consumer = Consumer(config.kafka_conf(group_id=base + "-stream"))
    consumer.subscribe([config.TOPIC])

    state, buffer, empty, inserted = PipelineState(), [], 0, 0

    def flush():
        nonlocal buffer, inserted
        if not buffer:
            return
        out = process_batch(buffer, model)
        inserted += db.append_stream_predictions(out)       # 1) guardar
        consumer.commit(asynchronous=False)                 # 2) confirmar offset DESPUÉS de guardar
        record_predictions(state, out.to_dict("records"))
        print(f"[stream] lote={len(buffer)} nuevas={inserted} | estado={state.summary()}")
        buffer = []

    try:
        while follow or empty < 10:
            msg = consumer.poll(1.0)
            if msg is None:
                empty += 1
                flush()                                     # no dejar lotes parciales esperando
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(msg.error())
            empty = 0
            rec = message_to_record(msg)
            if apply_event(state, rec):
                buffer.append(rec)
            if len(buffer) >= batch_size:
                flush()
        flush()
    finally:
        consumer.close()
    print(f"[stream] terminado. Filas insertadas: {inserted} | estado final: {state.summary()}")
    return state


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch-size", type=int, default=50)
    ap.add_argument("--follow", action="store_true", help="seguir escuchando indefinidamente")
    a = ap.parse_args()
    run(a.batch_size, a.follow)
