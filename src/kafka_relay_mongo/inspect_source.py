import json
import os
import uuid

from confluent_kafka import KafkaError

from .config import source_endpoint_from_env
from .kafka_clients import make_consumer


def _inspect_value(value: bytes | None) -> None:
    if value is None:
        print("value: <Kafka null/tombstone>")
        return

    print(f"value_size_bytes: {len(value)}")
    print(f"first_32_bytes_hex: {value[:32].hex()}")

    if len(value) >= 5 and value[0] == 0:
        schema_id = int.from_bytes(value[1:5], "big")
        print(
            "possible_confluent_schema_registry_frame: "
            f"magic_byte=0 possible_schema_id={schema_id}"
        )

    try:
        text = value.decode("utf-8")
        print("utf8: yes")
    except UnicodeDecodeError:
        print("utf8: no")
        return

    try:
        parsed = json.loads(text)
        print("plain_json: yes")
        print(f"json_top_level_type: {type(parsed).__name__}")
        if isinstance(parsed, dict):
            print(f"json_keys_sample: {list(parsed.keys())[:20]}")
    except json.JSONDecodeError:
        print("plain_json: no")
        print(f"text_preview: {text[:300]!r}")


def main() -> None:
    endpoint = source_endpoint_from_env()
    topic = os.environ["SOURCE_TOPIC"]
    sample_count = int(os.getenv("SOURCE_INSPECT_SAMPLE_COUNT", "5"))
    group_id = f"source-inspector-{uuid.uuid4()}"

    consumer = make_consumer(
        endpoint,
        group_id=group_id,
        auto_offset_reset="earliest",
        client_id="source-inspector",
    )
    consumer.subscribe([topic])

    print(f"Inspecting {sample_count} samples from {topic!r} with temporary group {group_id!r}")
    found = 0
    try:
        while found < sample_count:
            msg = consumer.poll(5.0)
            if msg is None:
                print("No message received in the last 5 seconds...")
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(msg.error())

            found += 1
            print("\n" + "=" * 72)
            print(f"sample={found}")
            print(f"topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}")
            print(f"key_size_bytes={len(msg.key()) if msg.key() is not None else None}")
            print(f"header_count={len(msg.headers() or [])}")
            _inspect_value(msg.value())
    finally:
        consumer.close()
