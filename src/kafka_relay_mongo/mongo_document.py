from __future__ import annotations

from datetime import datetime, timezone

from bson.binary import Binary

from .event_identity import source_position_from_headers, stable_event_id
from .payload_codec import DecodedPayload


def _header_documents(headers: list[tuple[str, bytes | None]] | None) -> list[dict]:
    docs: list[dict] = []
    for key, value in headers or []:
        docs.append({"key": key, "value": Binary(value) if value is not None else None})
    return docs


def build_mongo_document(
    *,
    topic: str,
    partition: int,
    offset: int,
    timestamp_ms: int | None,
    key: bytes | None,
    headers: list[tuple[str, bytes | None]] | None,
    decoded: DecodedPayload,
) -> dict:
    event_id = stable_event_id(
        headers,
        fallback_topic=topic,
        fallback_partition=partition,
        fallback_offset=offset,
    )

    return {
        "_event_id": event_id,
        "_pipeline": {
            "source": source_position_from_headers(headers),
            "target": {
                "topic": topic,
                "partition": partition,
                "offset": offset,
                "timestamp_ms": timestamp_ms,
            },
            "stored_at": datetime.now(timezone.utc),
        },
        "message_key": Binary(key) if key is not None else None,
        "message_headers": _header_documents(headers),
        "payload_format": decoded.payload_format,
        "payload": decoded.payload,
    }
