from confluent_kafka import Consumer, Producer

from .config import KafkaEndpoint


def make_consumer(
    endpoint: KafkaEndpoint,
    *,
    group_id: str,
    auto_offset_reset: str,
    client_id: str,
) -> Consumer:
    cfg = endpoint.client_config()
    cfg.update(
        {
            "group.id": group_id,
            "client.id": client_id,
            "auto.offset.reset": auto_offset_reset,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        }
    )
    return Consumer(cfg)


def make_producer(endpoint: KafkaEndpoint, *, client_id: str) -> Producer:
    cfg = endpoint.client_config()
    cfg.update(
        {
            "client.id": client_id,
            "enable.idempotence": True,
            "acks": "all",
        }
    )
    return Producer(cfg)
