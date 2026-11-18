"""Transformación: armoniza los 5 años del World Happiness Report en un solo dataset."""
import re

import numpy as np
import pandas as pd

TARGET = "happiness_score"
FEATURES = ["gdp_per_capita", "social_support", "life_expectancy", "freedom", "corruption", "generosity"]
NUMERIC = [TARGET, *FEATURES, "dystopia_residual", "happiness_rank"]

# Se EXCLUYEN rank y dystopia_residual como predictoras (data leakage): derivan del score.

YEAR_CANDIDATES = ["year", "Year", "anio", "año", "source_year", "file", "source_file", "filename"]

# (regex sobre el nombre normalizado, nombre final). El orden importa: gana la primera coincidencia.
RULES = [
    (r"^(country|country_or_region|country_name)$", "country"),
    (r"^region$", "region"),
    (r"lower_confidence|upper_confidence|whisker|standard_error|confidence", "ci_or_se"),
    (r"dystopia", "dystopia_residual"),
    (r"^(happiness_score|score|ladder_score)$", "happiness_score"),
    (r"rank", "happiness_rank"),
    (r"gdp|economy", "gdp_per_capita"),
    (r"social_support|^family$", "social_support"),
    (r"health|life_expectancy", "life_expectancy"),
    (r"^freedom", "freedom"),
    (r"corruption|trust", "corruption"),
    (r"generosity", "generosity"),
]

COUNTRY_FIX = {
    "Taiwan Province of China": "Taiwan",
    "Hong Kong S.A.R., China": "Hong Kong",
    "Trinidad & Tobago": "Trinidad and Tobago",
    "North Cyprus": "Northern Cyprus",
    "Macedonia": "North Macedonia",
    "Somaliland region": "Somaliland Region",
    "Congo (Kinshasa)": "Democratic Republic of the Congo",
    "Congo (Brazzaville)": "Congo",
}


def norm(col) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(col).lower()).strip("_")


def map_col(col) -> str:
    n = norm(col)
    for pattern, target in RULES:
        if re.search(pattern, n):
            return target
    return n


def _year_from(value):
    m = re.search(r"(20\d{2})", str(value))
    return int(m.group(1)) if m else None


def find_year(payload: dict, headers: dict | None = None, key: str | None = None):
    """Busca el año en el payload; si no está, en la key o en los headers del mensaje."""
    for c in YEAR_CANDIDATES:
        if payload.get(c) is not None:
            y = _year_from(payload[c])
            if y:
                return y
    for src in [key, *(headers or {}).values()]:
        if src is not None:
            y = _year_from(src)
            if y:
                return y
    return None


def unwrap_payload(payload) -> tuple[dict, dict]:
    """Separa un payload en (datos, metadatos).

    Formato real del tema etl.records:  {"metadata": {source_file, year, record_id}, "data": {...columnas del CSV...}}
    Formato plano (compatibilidad):     {...columnas del CSV...}
    """
    if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
        meta = payload.get("metadata")
        return dict(payload["data"]), (dict(meta) if isinstance(meta, dict) else {})
    return (dict(payload) if isinstance(payload, dict) else {}), {}


def records_to_frame(records: list[dict]) -> pd.DataFrame:
    """Lista de mensajes -> DataFrame crudo (datos del CSV + metadatos de Kafka con prefijo '_')."""
    parts = [unwrap_payload(r.get("payload")) for r in records]
    datas = [d for d, _ in parts]
    metas = [m for _, m in parts]
    kafka_meta = pd.DataFrame([{k: v for k, v in r.items() if k not in ("payload", "_headers")} for r in records])
    body = pd.json_normalize(datas)
    body = body.drop(columns=[c for c in body.columns if norm(c) == "year"])
    # metadatos del mensaje (source_file, record_id) conservados con prefijo '_' para trazabilidad
    msg_meta = pd.DataFrame([{f"_{k}": v for k, v in m.items() if k != "year"} for m in metas])
    df = pd.concat([kafka_meta, msg_meta, body], axis=1)
    df["year"] = [find_year({**metas[i], **datas[i]}, records[i].get("_headers"), records[i].get("_key"))
                  for i in range(len(records))]
    return df


def columns_by_year(raw: pd.DataFrame) -> dict[int, set]:
    out = {}
    for y, g in raw.groupby("year"):
        keep = g.drop(columns=[c for c in g.columns if c.startswith("_")] + ["year"])
        out[int(y)] = set(keep.dropna(axis=1, how="all").columns)
    return out


def harmonize(df: pd.DataFrame, keep_meta: bool = False) -> pd.DataFrame:
    """Unifica los nombres de columnas de cada año y apila los años (formato largo)."""
    parts = []
    for _, g in df.groupby("year", dropna=False):
        g = g.dropna(axis=1, how="all")
        g = g.rename(columns={c: c if (c.startswith("_") or c == "year") else map_col(c) for c in g.columns})
        g = g.loc[:, ~g.columns.duplicated()]
        if not keep_meta:
            g = g.drop(columns=[c for c in g.columns if c.startswith("_")])
        parts.append(g)
    return pd.concat(parts, ignore_index=True, sort=False)


def clean(df: pd.DataFrame, strict: bool = True) -> pd.DataFrame:
    """Tipos, formato de país y duplicados. Los atípicos se marcan en el EDA, no se eliminan."""
    df = df.copy()
    if strict:
        missing = [c for c in ["country", TARGET, *FEATURES] if c not in df.columns]
        if missing:
            raise ValueError(f"Faltan columnas tras la armonización: {missing}. Revisa RULES en transform.py")
    for c in NUMERIC:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")  # formatos inconsistentes -> NaN
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    if "country" in df.columns:
        df["country"] = df["country"].astype(str).str.strip().replace(COUNTRY_FIX)
        df = df.drop_duplicates(subset=["country", "year"])
    return df.reset_index(drop=True)


def build_dataset(records: list[dict]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Devuelve (dataset limpio completo, dataset listo para modelar)."""
    raw = records_to_frame(records)
    full = clean(harmonize(raw))
    data = full[["country", "year", TARGET, *FEATURES]].dropna(subset=[TARGET]).reset_index(drop=True)
    return full, data
