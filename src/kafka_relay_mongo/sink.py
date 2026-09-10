import logging

from bson.errors import InvalidDocument
from confluent_kafka import KafkaError
from pymongo import MongoClient
from pymongo.errors import DocumentTooLarge, PyMongoError

from .config import sink_settings
from .event_identity import RESERVED_RELAY_HEADERS
from .kafka_clients import make_consumer, make_producer
from .logging_config import configure_logging
from .mongo_document import build_mongo_document
from .payload_codec import PayloadDecodeError, decode_payload

log = logging.getLogger(__name__)


def _timestamp_ms(message) -> int | None:
    _timestamp_type, value = message.timestamp()
    return value if value is not None and value >= 0 else None


def _dlq_headers(original_headers, reason: str):
    headers = list(original_headers or [])
    headers.append(("dlq.reason", reason.encode("utf-8")[:2048]))
    return headers


def _send_dlq(producer, *, topic: str, message, reason: str, timeout: int) -> None:
    errors: list[object] = []

    def callback(err, delivered_msg) -> None:
        if err is not None:
            errors.append(err)
            return
        log.warning(
            "Sent record to DLQ topic=%s partition=%s offset=%s reason=%s",
            delivered_msg.topic(),
            delivered_msg.partition(),
            delivered_msg.offset(),
            reason,
        )

    producer.produce(
        topic=topic,
        key=message.key(),
        value=message.value(),
        headers=_dlq_headers(message.headers(), reason),
        on_delivery=callback,
    )
    remaining = producer.flush(timeout)
    if remaining != 0:
        raise TimeoutError(f"DLQ delivery timed out with {remaining} message(s) remaining")
    if errors:
        raise RuntimeError(f"DLQ delivery failed: {errors[0]}")


def main() -> None:
    configure_logging()
    settings = sink_settings()

    consumer = make_consumer(
        settings.target_endpoint,
        group_id=settings.sink_group_id,
        auto_offset_reset=settings.sink_auto_offset_reset,
        client_id="mongo-sink",
    )
    dlq_producer = make_producer(settings.target_endpoint, client_id="mongo-sink-dlq")

    mongo = MongoClient(settings.mongo_uri, serverSelectionTimeoutMS=5000)
    mongo.admin.command("ping")
    collection = mongo[settings.mongo_database][settings.mongo_collection]
    collection.create_index("_event_id", unique=True)

    consumer.subscribe([settings.target_topic])
    log.info(
        "Mongo sink started: topic=%s -> %s.%s value_format=%s",
        settings.target_topic,
        settings.mongo_database,
        settings.mongo_collection,
        settings.sink_value_format,
    )

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(msg.error())

            try:
                decoded = decode_payload(msg.value(), settings.sink_value_format)
                document = build_mongo_document(
                    topic=msg.topic(),
                    partition=msg.partition(),
                    offset=msg.offset(),
                    timestamp_ms=_timestamp_ms(msg),
                    key=msg.key(),
                    headers=msg.headers(),
                    decoded=decoded,
                )

                result = collection.update_one(
                    {"_event_id": document["_event_id"]},
                    {"$setOnInsert": document},
                    upsert=True,
                )
                action = "inserted" if result.upserted_id is not None else "already-existed"
                log.info("Mongo %s event_id=%s", action, document["_event_id"])

            except PayloadDecodeError as exc:
                _send_dlq(
                    dlq_producer,
                    topic=settings.target_dlq_topic,
                    message=msg,
                    reason=str(exc),
                    timeout=settings.delivery_timeout_seconds,
                )
            except (InvalidDocument, DocumentTooLarge) as exc:
                _send_dlq(
                    dlq_producer,
                    topic=settings.target_dlq_topic,
                    message=msg,
                    reason=f"Mongo document rejected: {exc}",
                    timeout=settings.delivery_timeout_seconds,
                )
            except PyMongoError:
                # Infrastructure failure: do NOT commit past this record. Exit so a
                # supervisor (or the user locally) can restart and Kafka can replay it.
                log.exception("MongoDB operation failed; leaving Kafka offset uncommitted")
                raise

            # Commit target offset only after Mongo upsert or confirmed DLQ delivery.
            consumer.commit(message=msg, asynchronous=False)

    except KeyboardInterrupt:
        log.info("Mongo sink stopped by user")
    finally:
        dlq_producer.flush(settings.delivery_timeout_seconds)
        consumer.close()
        mongo.close()
