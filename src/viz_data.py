"""Funciones puras para el dashboard (sin Streamlit): lectura de la base y KPIs."""
import numpy as np
import pandas as pd

from src import db


def load_predictions(source: str = "stream", limit: int | None = None) -> pd.DataFrame:
    table = "stream_predictions" if source == "stream" else "happiness_predictions"
    return db.read_table(table, limit)


def kpis(df: pd.DataFrame) -> dict:
    if df.empty:
        return {"filas": 0, "rechazados": 0, "MAE": None, "RMSE": None}
    ok = df[df.get("status", "ok") != "rejected"] if "status" in df.columns else df
    both = ok.dropna(subset=["actual_score", "predicted_score"])
    err = both["actual_score"] - both["predicted_score"]
    return {
        "filas": int(len(df)),
        "rechazados": int((df["status"] == "rejected").sum()) if "status" in df.columns else 0,
        "MAE": float(err.abs().mean()) if len(both) else None,
        "RMSE": float(np.sqrt((err ** 2).mean())) if len(both) else None,
    }


def error_by_year(df: pd.DataFrame) -> pd.DataFrame:
    both = df.dropna(subset=["actual_score", "predicted_score"]).copy()
    if both.empty:
        return pd.DataFrame(columns=["year", "MAE"])
    both["abs_error"] = (both["actual_score"] - both["predicted_score"]).abs()
    return both.groupby("year", as_index=False)["abs_error"].mean().rename(columns={"abs_error": "MAE"})
