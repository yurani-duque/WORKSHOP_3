"""Entrenamiento y evaluación de modelos de regresión (sin fuga de información hacia test)."""
import json

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
from sklearn.impute import SimpleImputer
from sklearn.inspection import permutation_importance
from sklearn.linear_model import LinearRegression, Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import GridSearchCV, GroupShuffleSplit, KFold, train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from src import config
from src.model_io import save_artifacts
from src.transform import FEATURES, TARGET

sns.set_theme(style="whitegrid")


def make_pipe(model, scale: bool = True) -> Pipeline:
    """Imputación + (escalado) + modelo. Todo se ajusta SOLO con los datos de entrenamiento."""
    steps = [("imputer", SimpleImputer(strategy="median"))]
    if scale:
        steps.append(("scaler", StandardScaler()))
    steps.append(("model", model))
    return Pipeline(steps)


def get_models() -> dict[str, Pipeline]:
    rs = config.RANDOM_STATE
    return {
        "LinearRegression": make_pipe(LinearRegression()),
        "Ridge": make_pipe(Ridge(alpha=1.0)),
        "RandomForest": make_pipe(RandomForestRegressor(n_estimators=300, random_state=rs, n_jobs=-1), scale=False),
        "GradientBoosting": make_pipe(GradientBoostingRegressor(random_state=rs), scale=False),
    }


# Búsqueda de hiperparámetros (diapositiva S11): SOLO sobre train, con validación cruzada.
PARAM_GRIDS = {
    "LinearRegression": {},
    "Ridge": {"model__alpha": [0.1, 1, 10, 100]},
    "RandomForest": {"model__max_depth": [None, 6, 10], "model__min_samples_leaf": [1, 3]},
    "GradientBoosting": {"model__learning_rate": [0.05, 0.1], "model__max_depth": [2, 3]},
}


def adjusted_r2(r2: float, n: int, p: int) -> float:
    return 1 - (1 - r2) * (n - 1) / (n - p - 1)


def metrics(y_true, y_pred, n_features: int) -> dict:
    mse = mean_squared_error(y_true, y_pred)
    r2 = r2_score(y_true, y_pred)
    return {
        "MAE": mean_absolute_error(y_true, y_pred),
        "MSE": mse,
        "RMSE": float(np.sqrt(mse)),
        "R2": r2,
        "R2_ajustado": adjusted_r2(r2, len(y_true), n_features),
    }


def _save(fig, name):
    config.FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(config.FIGURES_DIR / name, dpi=130)
    plt.close(fig)


def run(data: pd.DataFrame) -> dict:
    X, y, groups = data[FEATURES], data[TARGET], data["country"]
    X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.2, random_state=config.RANDOM_STATE)
    print(f"[train] train={X_tr.shape} test={X_te.shape}")

    # Ajuste de hiperparámetros + validación cruzada SOLO sobre train (el test queda reservado)
    cv = KFold(n_splits=5, shuffle=True, random_state=config.RANDOM_STATE)
    tuned, cv_rows = {}, []
    for name, pipe in get_models().items():
        gs = GridSearchCV(pipe, PARAM_GRIDS[name], cv=cv, scoring="neg_root_mean_squared_error").fit(X_tr, y_tr)
        tuned[name] = gs.best_estimator_
        cv_rows.append({"modelo": name, "RMSE_cv": -gs.best_score_,
                        "mejores_params": json.dumps(gs.best_params_, default=str)})
    cv_df = pd.DataFrame(cv_rows)
    best = cv_df.sort_values("RMSE_cv").iloc[0]["modelo"]      # se elige con validación, NO con test

    # Evaluación en test (una sola vez, con los modelos ya afinados)
    rows, preds = [], {}
    for name, pipe in tuned.items():
        preds[name] = pipe.predict(X_te)
        rows.append({"modelo": name, **metrics(y_te, preds[name], X_te.shape[1])})
    res = pd.DataFrame(rows).merge(cv_df, on="modelo").sort_values("RMSE_cv")
    print(res.drop(columns="mejores_params").round(4).to_string(index=False))
    print(f"[train] mejor modelo (por validación cruzada): {best}")
    models = tuned

    # Robustez: split por país (países no vistos)
    tr, te = next(GroupShuffleSplit(n_splits=1, test_size=0.2, random_state=config.RANDOM_STATE).split(X, y, groups))
    g_pipe = get_models()["LinearRegression"].fit(X.iloc[tr], y.iloc[tr])
    g_metrics = metrics(y.iloc[te], g_pipe.predict(X.iloc[te]), X.shape[1])
    print("[train] LinearRegression con split por país:", {k: round(v, 4) for k, v in g_metrics.items()})

    # Guardado: modelo serializado + metadatos (el consumer lo carga una vez para inferir)
    best_row = res[res["modelo"] == best].iloc[0]
    save_artifacts(models[best], {
        "model": best, "target": TARGET, "features": FEATURES,
        "n_train": int(len(X_tr)), "n_test": int(len(X_te)),
        "best_params": best_row["mejores_params"],
        "test_metrics": {k: float(best_row[k]) for k in ["MAE", "MSE", "RMSE", "R2", "R2_ajustado"]},
    })
    config.REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    res.to_csv(config.REPORTS_DIR / "model_metrics.csv", index=False)
    (config.REPORTS_DIR / "group_split_check.json").write_text(json.dumps(g_metrics, indent=2))

    # Gráficos
    pred, resid = preds[best], y_te - preds[best]
    fig, ax = plt.subplots(1, 3, figsize=(17, 4.5))
    ax[0].scatter(y_te, pred, alpha=0.6)
    lim = [min(y_te.min(), pred.min()), max(y_te.max(), pred.max())]
    ax[0].plot(lim, lim, "r--")
    ax[0].set(xlabel="Real", ylabel="Predicho", title=f"Real vs predicho ({best})")
    ax[1].scatter(pred, resid, alpha=0.6)
    ax[1].axhline(0, color="r", ls="--")
    ax[1].set(xlabel="Predicho", ylabel="Residuo", title="Residuos vs predicho")
    sns.histplot(resid, kde=True, ax=ax[2]).set_title("Distribución de residuos")
    _save(fig, "06_model_performance.png")

    imp = permutation_importance(models[best], X_te, y_te, n_repeats=20,
                                 random_state=config.RANDOM_STATE, scoring="r2")
    fig, ax = plt.subplots(figsize=(7, 3.5))
    pd.Series(imp.importances_mean, index=FEATURES).sort_values().plot(kind="barh", ax=ax)
    ax.set_title("Importancia por permutación (caída de R²)")
    _save(fig, "07_feature_importance.png")

    fig, ax = plt.subplots(figsize=(7, 3.5))
    comp = res.set_index("modelo")[["MAE", "RMSE"]]
    comp.plot(kind="bar", ax=ax, rot=20)
    ax.set_title("Comparación de modelos (test)")
    _save(fig, "08_model_comparison.png")

    return {"best": best, "metrics": res, "group_split": g_metrics,
            "X_test": X_te, "y_test": y_te, "pred": pred, "model": models[best]}
