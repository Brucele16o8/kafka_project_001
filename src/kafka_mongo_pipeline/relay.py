import logging

from confluent_kafka import KafkaError

from .config import relay_settings
from .event_identity import build_relay_headers
from .kafka_clients import make_consumer, make_producer
from .logging_config import configure_logging

log = logging.getLogger(__name__)


def _message_timestamp_ms(message) -> int | None:
    _timestamp_type, timestamp_ms = message.timestamp()
    return timestamp_ms if timestamp_ms is not None and timestamp_ms >= 0 else None


def _produce_and_wait(producer, *, topic: str, message, headers, timeout: int) -> None:
    delivery_error: list[Exception | object] = []

    def callback(err, delivered_msg) -> None:
        if err is not None:
            delivery_error.append(err)
            return
        log.info(
            "Produced target topic=%s partition=%s offset=%s",
            delivered_msg.topic(),
            delivered_msg.partition(),
            delivered_msg.offset(),
        )

    producer.produce(
        topic=topic,
        key=message.key(),
        value=message.value(),
        headers=headers,
        on_delivery=callback,
    )
    remaining = producer.flush(timeout)
    if remaining != 0:
        raise TimeoutError(f"{remaining} Kafka message(s) were not delivered within {timeout}s")
    if delivery_error:
        raise RuntimeError(f"Target Kafka delivery failed: {delivery_error[0]}")


def main() -> None:
    configure_logging()
    settings = relay_settings()

    consumer = make_consumer(
        settings.source_endpoint,
        group_id=settings.source_group_id,
        auto_offset_reset=settings.source_auto_offset_reset,
        client_id="source-to-target-relay",
    )
    producer = make_producer(settings.target_endpoint, client_id="source-to-target-relay")
    consumer.subscribe([settings.source_topic])

    log.info("Relay started: %s -> %s", settings.source_topic, settings.target_topic)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(msg.error())

            source_timestamp_ms = _message_timestamp_ms(msg)
            headers = build_relay_headers(
                msg.headers(),
                source_system=settings.source_system_id,
                source_topic=msg.topic(),
                source_partition=msg.partition(),
                source_offset=msg.offset(),
                source_timestamp_ms=source_timestamp_ms,
            )

            log.info(
                "Read source topic=%s partition=%s offset=%s bytes=%s",
                msg.topic(),
                msg.partition(),
                msg.offset(),
                len(msg.value()) if msg.value() is not None else None,
            )

            _produce_and_wait(
                producer,
                topic=settings.target_topic,
                message=msg,
                headers=headers,
                timeout=settings.delivery_timeout_seconds,
            )

            # Commit source offset only after target Kafka acknowledged the record.
            consumer.commit(message=msg, asynchronous=False)
            log.info(
                "Committed source topic=%s partition=%s offset=%s",
                msg.topic(),
                msg.partition(),
                msg.offset(),
            )
    except KeyboardInterrupt:
        log.info("Relay stopped by user")
    finally:
        producer.flush(settings.delivery_timeout_seconds)
        consumer.close()