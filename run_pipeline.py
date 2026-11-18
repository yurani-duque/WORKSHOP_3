"""Orquesta el pipeline ETL + ML.

Uso:
    python run_pipeline.py                 # extract -> transform -> eda -> train -> load
    python run_pipeline.py --from-raw      # reutiliza data/raw/records.jsonl (sin conectar a Kafka)
    python run_pipeline.py --steps train load --from-raw
"""
import argparse

from src import config, db, eda, kafka_extract, train, transform

STEPS = ["extract", "transform", "eda", "train", "load"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--steps", nargs="+", choices=STEPS, default=STEPS)
    ap.add_argument("--from-raw", action="store_true", help="usar el archivo crudo ya descargado")
    a = ap.parse_args()
    steps = [s for s in a.steps if not (a.from_raw and s == "extract")]

    records = kafka_extract.run() if "extract" in steps else kafka_extract.load_jsonl(config.RAW_PATH)
    full, data = transform.build_dataset(records)
    if "transform" in steps:
        config.PROCESSED_PATH.parent.mkdir(parents=True, exist_ok=True)
        full.to_csv(config.CLEAN_PATH, index=False)
        data.to_csv(config.PROCESSED_PATH, index=False)
        print(f"[transform] dataset de modelado: {data.shape} -> {config.PROCESSED_PATH}")
        print(data.groupby("year").size().to_string())
    if "eda" in steps:
        eda.run(records, full, data)
    result = train.run(data) if "train" in steps else None
    if "load" in steps:
        db.load_clean(data)
        if result:
            db.load_test_predictions(data, result["X_test"], result["y_test"], result["pred"])
        print(f"[load] base de datos: {config.DB_PATH}")


if __name__ == "__main__":
    main()
