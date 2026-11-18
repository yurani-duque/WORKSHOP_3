"""Carga a base de datos (SQLite)."""
import sqlite3

import pandas as pd

from src import config


def connect():
    return sqlite3.connect(config.DB_PATH)


def load_clean(data: pd.DataFrame) -> None:
    with connect() as con:
        data.to_sql("happiness_clean", con, if_exists="replace", index=False)


def load_test_predictions(data: pd.DataFrame, X_test, y_test, pred) -> None:
    out = data.loc[X_test.index, ["country", "year"]].copy()
    out["actual_score"] = y_test.values
    out["predicted_score"] = pred
    out["abs_error"] = (out["actual_score"] - out["predicted_score"]).abs()
    with connect() as con:
        out.to_sql("happiness_predictions", con, if_exists="replace", index=False)


def append_stream_predictions(df: pd.DataFrame) -> int:
    """Agrega predicciones de streaming evitando duplicados por (partition, offset)."""
    with connect() as con:
        exists = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='stream_predictions'"
        ).fetchone()
        if exists:
            seen = pd.read_sql("SELECT kafka_partition, kafka_offset FROM stream_predictions", con)
            keys = set(zip(seen.kafka_partition, seen.kafka_offset))
            df = df[[(p, o) not in keys for p, o in zip(df.kafka_partition, df.kafka_offset)]]
        if len(df):
            df.to_sql("stream_predictions", con, if_exists="append", index=False)
        return len(df)


def read_table(name: str, limit: int | None = None) -> pd.DataFrame:
    """Lee una tabla de la base (vacía si aún no existe)."""
    with connect() as con:
        exists = con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone()
        if not exists:
            return pd.DataFrame()
        sql = f"SELECT * FROM {name}" + (f" ORDER BY rowid DESC LIMIT {int(limit)}" if limit else "")
        return pd.read_sql(sql, con)
