"""Configuración central: rutas y parámetros de conexión a Kafka (vía variables de entorno)."""
import os
from pathlib import Path

import certifi
from dotenv import load_dotenv

load_dotenv()

ROOT = Path(__file__).resolve().parents[1]
RAW_PATH = ROOT / "data" / "raw" / "records.jsonl"
PROCESSED_PATH = ROOT / "data" / "processed" / "happiness_model_data.csv"
CLEAN_PATH = ROOT / "data" / "processed" / "happiness_clean.csv"
MODEL_PATH = ROOT / "models" / "happiness_model.joblib"
DB_PATH = ROOT / "happiness.db"
FIGURES_DIR = ROOT / "reports" / "figures"
REPORTS_DIR = ROOT / "reports"

TOPIC = os.getenv("KAFKA_TOPIC", "etl.records")
RANDOM_STATE = 42


def kafka_conf(group_id: str | None = None) -> dict:
    """Devuelve la configuración del consumidor. La contraseña se lee de KAFKA_PASSWORD."""
    group = group_id or os.getenv("KAFKA_GROUP_ID", "student-yuraniduque")
    if not group.startswith("student-"):
        print(f"[aviso] group.id='{group}': en clase se usó el prefijo 'student-' (p. ej. student-ana). "
              "Confirma con tu profesor cuál debes usar.")
    password = os.getenv("KAFKA_PASSWORD")
    if not password:
        raise RuntimeError(
            "Falta KAFKA_PASSWORD. Copia .env.example a .env y completa la contraseña "
            "que entrega el instructor."
        )
    return {
        "bootstrap.servers": os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            "kafka-classroom-2026.westus.cloudapp.azure.com:9092",
        ),
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "SCRAM-SHA-512",
        "sasl.username": os.getenv("KAFKA_USERNAME", "classroom-students"),
        "sasl.password": password,
        "ssl.ca.location": certifi.where(),
        "group.id": group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
