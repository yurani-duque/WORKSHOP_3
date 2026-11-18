"""Genera mensajes sintéticos con los esquemas típicos del World Happiness Report 2015-2019.
Sirve para probar el pipeline SIN Kafka. No usar para el entregable final (usa los datos reales)."""
import numpy as np

SCHEMAS = {
    2015: ["Country", "Region", "Happiness Rank", "Happiness Score", "Standard Error", "Economy (GDP per Capita)",
           "Family", "Health (Life Expectancy)", "Freedom", "Trust (Government Corruption)", "Generosity", "Dystopia Residual"],
    2016: ["Country", "Region", "Happiness Rank", "Happiness Score", "Lower Confidence Interval", "Upper Confidence Interval",
           "Economy (GDP per Capita)", "Family", "Health (Life Expectancy)", "Freedom", "Trust (Government Corruption)",
           "Generosity", "Dystopia Residual"],
    2017: ["Country", "Happiness.Rank", "Happiness.Score", "Whisker.high", "Whisker.low", "Economy..GDP.per.Capita.", "Family",
           "Health..Life.Expectancy.", "Freedom", "Generosity", "Trust..Government.Corruption.", "Dystopia.Residual"],
    2018: ["Overall rank", "Country or region", "Score", "GDP per capita", "Social support", "Healthy life expectancy",
           "Freedom to make life choices", "Generosity", "Perceptions of corruption"],
    2019: ["Overall rank", "Country or region", "Score", "GDP per capita", "Social support", "Healthy life expectancy",
           "Freedom to make life choices", "Generosity", "Perceptions of corruption"],
}
# Posición de cada concepto en cada esquema (por palabra clave)
KEYS = {"gdp": ("gdp",), "soc": ("family", "social"), "life": ("health", "life expectancy"),
        "free": ("freedom",), "gen": ("generosity",), "cor": ("corruption", "trust")}


def make_records(n_countries=60, seed=0):
    rng = np.random.default_rng(seed)
    countries = [f"Country {i}" for i in range(n_countries)]
    base = {c: rng.uniform(0, 1, 6) for c in countries}
    records, offset = [], 0
    for year, cols in SCHEMAS.items():
        for c in countries:
            gdp, soc, life, free, gen, cor = np.clip(base[c] + rng.normal(0, 0.05, 6), 0, 1.6)
            score = 2.2 + 1.2 * gdp + 1.5 * soc + 1.0 * life + 1.3 * free + 0.3 * gen + 0.6 * cor + rng.normal(0, 0.25)
            vals = {"gdp": gdp, "soc": soc, "life": life, "free": free, "gen": gen, "cor": cor}
            row = {}
            for col in cols:
                low = col.lower()
                if low.startswith("country"):
                    row[col] = c
                elif low == "region":
                    row[col] = "Region X"
                elif "rank" in low:
                    row[col] = 1
                elif low in ("happiness score", "happiness.score", "score"):
                    row[col] = round(float(score), 3)
                elif any(k in low for k in ("error", "confidence", "whisker", "dystopia")):
                    row[col] = round(float(score) * 0.1, 3)
                else:
                    for k, words in KEYS.items():
                        if any(w in low for w in words):
                            row[col] = round(float(vals[k]), 3)
                            break
            if year == 2018 and c == "Country 3":
                row["Perceptions of corruption"] = "N/A"      # formato inconsistente a propósito
            records.append({"_topic": "etl.records", "_partition": 0, "_offset": offset, "_timestamp": 0,
                            "_key": f"{year}-{offset}", "_headers": {},
                            "payload": {"metadata": {"source_file": f"{year}.csv", "year": year,
                                                     "record_id": f"{year}-{offset}"},
                                        "data": row}})
            offset += 1
    return records
