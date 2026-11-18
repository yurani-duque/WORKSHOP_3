"""EDA: reporte de calidad por año + gráficos. Salidas en reports/."""
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from src import config
from src.transform import FEATURES, TARGET, columns_by_year, records_to_frame

sns.set_theme(style="whitegrid")


def _save(fig, name):
    config.FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(config.FIGURES_DIR / name, dpi=130)
    plt.close(fig)


def iqr_outliers(data: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for c in [TARGET, *FEATURES]:
        q1, q3 = data[c].quantile([0.25, 0.75])
        iqr = q3 - q1
        mask = (data[c] < q1 - 1.5 * iqr) | (data[c] > q3 + 1.5 * iqr)
        rows.append({"variable": c, "n_atipicos": int(mask.sum()), "pct": round(100 * mask.mean(), 2)})
    return pd.DataFrame(rows)


def run(records: list[dict], full: pd.DataFrame, data: pd.DataFrame) -> None:
    config.REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    lines = ["# Resumen EDA (generado automáticamente)\n"]

    # 1) Columnas comunes / únicas por año (nombres originales)
    cby = columns_by_year(records_to_frame(records))
    common = set.intersection(*cby.values())
    lines.append(f"## Columnas comunes a todos los años\n{sorted(common)}\n")
    lines.append("## Columnas únicas por año")
    for y, cols in sorted(cby.items()):
        others = set.union(*[c for yy, c in cby.items() if yy != y]) if len(cby) > 1 else set()
        lines.append(f"- {y}: {len(cols)} columnas; únicas: {sorted(cols - others)}")

    # 2) Calidad
    lines.append("\n## Registros por año\n" + data.groupby("year").size().to_string())
    miss = data[FEATURES + [TARGET]].isna().sum()
    lines.append("\n## Faltantes (dataset de modelado)\n" + miss[miss > 0].to_string() if miss.any() else "\n## Faltantes\nNinguno")
    lines.append("\n## Atípicos (IQR, solo marcados)\n" + iqr_outliers(data).to_string(index=False))
    lines.append("\n## Estadísticos descriptivos\n" + data[[TARGET, *FEATURES]].describe().T.round(3).to_string())
    lines.append("\n## Media por año\n" + data.groupby("year")[[TARGET, *FEATURES]].mean().round(3).to_string())
    corr = data[[TARGET, *FEATURES]].corr()
    lines.append("\n## Correlación con el score\n" + corr[TARGET].drop(TARGET).sort_values(ascending=False).round(3).to_string())
    (config.REPORTS_DIR / "eda_summary.md").write_text("\n".join(lines), encoding="utf-8")

    # 3) Gráficos
    fig, ax = plt.subplots(1, 2, figsize=(13, 4))
    sns.histplot(data[TARGET], kde=True, ax=ax[0]).set_title("Distribución del happiness score")
    sns.boxplot(data=data, x="year", y=TARGET, ax=ax[1]).set_title("Score por año")
    _save(fig, "01_target.png")

    fig, ax = plt.subplots(figsize=(8, 6))
    sns.heatmap(corr, annot=True, fmt=".2f", cmap="coolwarm", center=0, ax=ax)
    ax.set_title("Matriz de correlación")
    _save(fig, "02_correlation.png")

    fig, axes = plt.subplots(2, 3, figsize=(15, 8))
    for ax, f in zip(axes.ravel(), FEATURES):
        sns.scatterplot(data=data, x=f, y=TARGET, hue="year", palette="viridis", alpha=0.6,
                        ax=ax, legend=(f == FEATURES[0]))
        ax.set_title(f"{f} vs {TARGET}")
    _save(fig, "03_scatter_features.png")

    fig, ax = plt.subplots(figsize=(10, 4))
    data[FEATURES].plot(kind="box", ax=ax)
    ax.set_title("Boxplots de predictoras")
    _save(fig, "04_boxplots.png")

    fig, ax = plt.subplots(figsize=(7, 3.5))
    data[FEATURES + [TARGET]].isna().mean().mul(100).plot(kind="barh", ax=ax)
    ax.set_title("% de faltantes por variable")
    _save(fig, "05_missing.png")

    print(f"[eda] reporte en {config.REPORTS_DIR / 'eda_summary.md'} y figuras en {config.FIGURES_DIR}")
