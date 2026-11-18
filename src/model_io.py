"""Serialización del modelo + metadatos (diapositiva S11: artefactos de ML)."""
import json
import platform
from datetime import datetime, timezone

import joblib
import sklearn

from src import config

METADATA_PATH = config.MODEL_PATH.with_name("happiness_model_metadata.json")


def save_artifacts(model, metadata: dict) -> None:
    config.MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, config.MODEL_PATH)
    metadata = {
        **metadata,
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "sklearn_version": sklearn.__version__,
        "python_version": platform.python_version(),
    }
    METADATA_PATH.write_text(json.dumps(metadata, indent=2, default=str), encoding="utf-8")


def load_artifacts():
    """Carga el modelo UNA vez (no se reentrena por cada evento) y sus metadatos."""
    model = joblib.load(config.MODEL_PATH)
    metadata = json.loads(METADATA_PATH.read_text(encoding="utf-8")) if METADATA_PATH.exists() else {}
    return model, metadata
