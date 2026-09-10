from typing import Iterable

RESERVED_RELAY_HEADERS = {
    "relay.source_system",
    "relay.source_topic",
    "relay.source_partition",
    "relay.source_offset",
    "relay.source_timestamp_ms",
}


def _to_bytes(value: str | int | None) -> bytes | None:
    if value is None:
        return None
    return str(value).encode("utf-8")


def build_relay_headers(
    original_headers: list[tuple[str, bytes | None]] | None,
    *,
    source_system: str,
    source_topic: str,
    source_partition: int,
    source_offset: int,
    source_timestamp_ms: int | None,
) -> list[tuple[str, bytes | None]]:
    headers = [
        (k, v)
        for k, v in (original_headers or [])
        if k not in RESERVED_RELAY_HEADERS
    ]
    headers.extend(
        [
            ("relay.source_system", _to_bytes(source_system)),
            ("relay.source_topic", _to_bytes(source_topic)),
            ("relay.source_partition", _to_bytes(source_partition)),
            ("relay.source_offset", _to_bytes(source_offset)),
            ("relay.source_timestamp_ms", _to_bytes(source_timestamp_ms)),
        ]
    )
    return headers


def headers_to_dict(
    headers: Iterable[tuple[str, bytes | None]] | None,
) -> dict[str, bytes | None]:
    result: dict[str, bytes | None] = {}
    for key, value in headers or []:
        result[key] = value
    return result


def _decode_header(value: bytes | None) -> str | None:
    if value is None:
        return None
    return value.decode("utf-8")


def source_position_from_headers(
    headers: Iterable[tuple[str, bytes | None]] | None,
) -> dict[str, object] | None:
    h = headers_to_dict(headers)
    required = [
        "relay.source_system",
        "relay.source_topic",
        "relay.source_partition",
        "relay.source_offset",
    ]
    if any(h.get(key) is None for key in required):
        return None

    return {
        "system_id": _decode_header(h["relay.source_system"]),
        "topic": _decode_header(h["relay.source_topic"]),
        "partition": int(_decode_header(h["relay.source_partition"]) or "0"),
        "offset": int(_decode_header(h["relay.source_offset"]) or "0"),
        "timestamp_ms": (
            int(_decode_header(h["relay.source_timestamp_ms"]) or "0")
            if h.get("relay.source_timestamp_ms") is not None
            else None
        ),
    }


def stable_event_id(
    headers: Iterable[tuple[str, bytes | None]] | None,
    *,
    fallback_topic: str,
    fallback_partition: int,
    fallback_offset: int,
) -> str:
    source = source_position_from_headers(headers)
    if source:
        return (
            f"source:{source['system_id']}:{source['topic']}:"
            f"{source['partition']}:{source['offset']}"
        )
    return f"target:{fallback_topic}:{fallback_partition}:{fallback_offset}"
