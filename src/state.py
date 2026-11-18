"""Estado de la aplicación (diapositiva S12: «el estado es la memoria de la aplicación»).

estado_siguiente = f(estado_actual, evento)

El consumidor NO alimenta la visualización directamente: alimenta este estado, y el estado es
lo que se guarda, se explora y se visualiza. Así se separan adquisición, procesamiento,
estado y presentación.
"""
from collections import Counter
from dataclasses import dataclass, field


@dataclass
class PipelineState:
    events: list = field(default_factory=list)            # todos los eventos únicos recibidos
    last_event: dict | None = None
    by_partition: Counter = field(default_factory=Counter)
    predictions: list = field(default_factory=list)       # últimas predicciones (ventana)
    alerts: int = 0                                        # registros rechazados por validación
    duplicates: int = 0
    _seen_offsets: set = field(default_factory=set, repr=False)
    _seen_event_ids: set = field(default_factory=set, repr=False)

    def summary(self) -> dict:
        return {
            "eventos_unicos": len(self.events),
            "duplicados_descartados": self.duplicates,
            "por_particion": dict(self.by_partition),
            "predicciones": len(self.predictions),
            "alertas": self.alerts,
        }


def apply_event(state: PipelineState, event: dict) -> bool:
    """Incorpora un evento al estado. Devuelve False si era un duplicado.

    Deduplica por (partición, offset) y, si el payload trae `event_id` (como el producer de
    outbox del curso), también por ese identificador: la entrega es al-menos-una-vez.
    """
    uid = (event["_partition"], event["_offset"])
    payload = event.get("payload")
    event_id = None
    if isinstance(payload, dict):
        meta = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
        event_id = payload.get("event_id") or meta.get("record_id")   # record_id del mensaje (p. ej. "2015-4")
    if uid in state._seen_offsets or (event_id is not None and event_id in state._seen_event_ids):
        state.duplicates += 1
        return False
    state._seen_offsets.add(uid)
    if event_id is not None:
        state._seen_event_ids.add(event_id)
    state.events.append(event)
    state.last_event = event
    state.by_partition[event["_partition"]] += 1
    return True


def record_predictions(state: PipelineState, rows: list[dict], keep_last: int = 200) -> None:
    state.predictions.extend(rows)
    state.predictions = state.predictions[-keep_last:]
    state.alerts += sum(1 for r in rows if r.get("status") == "rejected")
