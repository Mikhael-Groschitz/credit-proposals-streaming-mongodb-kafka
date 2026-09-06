from __future__ import annotations

from bson.json_util import RELAXED_JSON_OPTIONS, dumps


def serializar_envelope(envelope: dict) -> bytes:
    return dumps(envelope, json_options=RELAXED_JSON_OPTIONS).encode("utf-8")
