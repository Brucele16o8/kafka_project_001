import json
from dataclasses import dataclass
from typing import Any

from bson.binary import Binary


class PayloadDecodeError(ValueError):
    pass


@dataclass(frozen=True)
class DecodedPayload:
    payload_format: str
    payload: Any


def decode_payload(value: bytes | None, mode: str) -> DecodedPayload:
    if value is None:
        return DecodedPayload("kafka_null", None)

    if mode == "raw":
        return DecodedPayload("raw_binary", Binary(value))

    try:
        text = value.decode("utf-8")
    except UnicodeDecodeError as exc:
        if mode == "json":
            raise PayloadDecodeError("Payload is not valid UTF-8 JSON") from exc
        return DecodedPayload("raw_binary", Binary(value))

    try:
        parsed = json.loads(text)
        return DecodedPayload("json", parsed)
    except json.JSONDecodeError as exc:
        if mode == "json":
            raise PayloadDecodeError("Payload is not valid JSON") from exc
        return DecodedPayload("raw_text", text)
