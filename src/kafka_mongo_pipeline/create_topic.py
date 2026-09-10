from __future__ import annotations

import logging
import os

from confluent_kafka import KafkaException, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic

from .config import target_endpoint_from_env
from .logging_config import configure_logging

log = logging.getLogger(__name__)


def main() -> None:
    configure_logging()
    endpoint = target_endpoint_from_env()
    admin = AdminClient(endpoint.client_config())

    target_topic = os.environ["TARGET_TOPIC"]
    dlq_topic = os.environ["TARGET_DLQ_TOPIC"]
    partitions = int(os.getenv("TARGET_TOPIC_PARTITIONS", "3"))
    replication_factor = int(os.getenv("TARGET_TOPIC_REPLICATION_FACTOR", "3"))

    topics = [
        NewTopic(target_topic, num_partitions=partitions, replication_factor=replication_factor),
        NewTopic(dlq_topic, num_partitions=partitions, replication_factor=replication_factor),
    ]

    futures = admin.create_topics(topics)
    for topic_name, future in futures.items():
        try:
            future.result()
            log.info("Created topic %s", topic_name)
        except KafkaException as exc:
            err = exc.args[0]
            if isinstance(err, KafkaError) and err.code() == KafkaError.TOPIC_ALREADY_EXISTS:
                log.info("Topic %s already exists", topic_name)
            else:
                raise
