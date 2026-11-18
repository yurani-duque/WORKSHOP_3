"""Extracción: consume el tema de Kafka y acumula los registros (payload + metadatos)."""
import json
import sys
from typing import Iterator

from src import config
from src.state import PipelineState, apply_event


def decode_payload(raw: bytes | None):
    if raw is None:
        return None
    text = raw.decode("utf-8", errors="replace")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text  # si no es JSON se conserva el texto crudo


def message_to_record(msg) -> dict:
    """Mensaje de Kafka -> dict con metadatos (prefijo '_') + payload original."""
    return {
        "_topic": msg.topic(),
        "_partition": msg.partition(),
        "_offset": msg.offset(),
        "_timestamp": msg.timestamp()[1],
        "_key": msg.key().decode("utf-8", errors="replace") if msg.key() else None,
        "_headers": {
            k: (v.decode("utf-8", errors="replace") if v else None)
            for k, v in (msg.headers() or [])
        },
        "payload": decode_payload(msg.value()),
    }


def iter_messages(conf: dict, topic: str, max_empty_polls: int | None = 10) -> Iterator[dict]:
    """Genera mensajes como dicts. Termina tras `max_empty_polls` polls vacíos seguidos
    (None = seguir indefinidamente), como en el notebook kafka_intro del curso."""
    from confluent_kafka import Consumer, KafkaError  # import local: los tests no lo requieren

    consumer = Consumer(conf)
    consumer.subscribe([topic])
    empty = 0
    try:
        while max_empty_polls is None or empty < max_empty_polls:
            msg = consumer.poll(1.0)
            if msg is None:
                empty += 1
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    empty += 1
                    continue
                raise RuntimeError(msg.error())
            empty = 0
            yield message_to_record(msg)
    finally:
        consumer.close()


def consume_all(conf: dict, topic: str, max_empty_polls: int = 10) -> PipelineState:
    """Acumula los mensajes en el ESTADO de la aplicación (deduplicado), no en una lista suelta."""
    state = PipelineState()
    for rec in iter_messages(conf, topic, max_empty_polls):
        apply_event(state, rec)
    return state


def save_jsonl(records: list[dict], path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False, default=str) + "\n")


def load_jsonl(path) -> list[dict]:
    with open(path, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def run() -> list[dict]:
    state = consume_all(config.kafka_conf(), config.TOPIC)
    records = state.events
    save_jsonl(records, config.RAW_PATH)
    print(f"[extract] estado: {state.summary()}")
    print(f"[extract] {len(records)} mensajes -> {config.RAW_PATH}")
    if records:
        print("[extract] ejemplo:", json.dumps(records[0], ensure_ascii=False, default=str)[:600])
    return records


if __name__ == "__main__":
    sys.exit(0 if run() else 1)
