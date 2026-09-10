import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


def _require(name: str) -> str:
    value = os.getenv(name)
    if value is None:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _get_int(name: str, default: int) -> int:
    value = os.getenv(name)
    return int(value) if value not in (None, "") else default


@dataclass(frozen=True)
class KafkaEndpoint:
    bootstrap_servers: str
    security_protocol: str
    sasl_mechanism: str | None = None
    username: str | None = None
    password: str | None = None

    def client_config(self) -> dict[str, object]:
        cfg: dict[str, object] = {
            "bootstrap.servers": self.bootstrap_servers,
            "security.protocol": self.security_protocol,
        }
        if self.security_protocol.startswith("SASL"):
            if not self.sasl_mechanism or not self.username or not self.password:
                raise RuntimeError(
                    "SASL Kafka endpoint requires mechanism, username and password"
                )
            cfg.update(
                {
                    "sasl.mechanism": self.sasl_mechanism,
                    "sasl.username": self.username,
                    "sasl.password": self.password,
                }
            )
        return cfg


@dataclass(frozen=True)
class RelaySettings:
    source_system_id: str
    source_endpoint: KafkaEndpoint
    source_topic: str
    source_group_id: str
    source_auto_offset_reset: str
    target_endpoint: KafkaEndpoint
    target_topic: str
    delivery_timeout_seconds: int


@dataclass(frozen=True)
class SinkSettings:
    target_endpoint: KafkaEndpoint
    target_topic: str
    target_dlq_topic: str
    sink_group_id: str
    sink_auto_offset_reset: str
    sink_value_format: str
    mongo_uri: str
    mongo_database: str
    mongo_collection: str
    delivery_timeout_seconds: int


def source_endpoint_from_env() -> KafkaEndpoint:
    return KafkaEndpoint(
        bootstrap_servers=_require("SOURCE_KAFKA_BOOTSTRAP_SERVERS"),
        security_protocol=os.getenv("SOURCE_KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
        sasl_mechanism=os.getenv("SOURCE_KAFKA_SASL_MECHANISM"),
        username=os.getenv("SOURCE_KAFKA_USERNAME"),
        password=os.getenv("SOURCE_KAFKA_PASSWORD"),
    )


def target_endpoint_from_env() -> KafkaEndpoint:
    return KafkaEndpoint(
        bootstrap_servers=_require("TARGET_KAFKA_BOOTSTRAP_SERVERS"),
        security_protocol=os.getenv("TARGET_KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
        sasl_mechanism=os.getenv("TARGET_KAFKA_SASL_MECHANISM"),
        username=os.getenv("TARGET_KAFKA_USERNAME"),
        password=os.getenv("TARGET_KAFKA_PASSWORD"),
    )


def relay_settings() -> RelaySettings:
    return RelaySettings(
        source_system_id=os.getenv("SOURCE_SYSTEM_ID", "source-kafka"),
        source_endpoint=source_endpoint_from_env(),
        source_topic=_require("SOURCE_TOPIC"),
        source_group_id=_require("SOURCE_GROUP_ID"),
        source_auto_offset_reset=os.getenv("SOURCE_AUTO_OFFSET_RESET", "earliest"),
        target_endpoint=target_endpoint_from_env(),
        target_topic=_require("TARGET_TOPIC"),
        delivery_timeout_seconds=_get_int("KAFKA_DELIVERY_TIMEOUT_SECONDS", 30),
    )


def sink_settings() -> SinkSettings:
    value_format = os.getenv("SINK_VALUE_FORMAT", "auto").lower()
    if value_format not in {"auto", "json", "raw"}:
        raise RuntimeError("SINK_VALUE_FORMAT must be one of: auto, json, raw")

    return SinkSettings(
        target_endpoint=target_endpoint_from_env(),
        target_topic=_require("TARGET_TOPIC"),
        target_dlq_topic=_require("TARGET_DLQ_TOPIC"),
        sink_group_id=_require("SINK_GROUP_ID"),
        sink_auto_offset_reset=os.getenv("SINK_AUTO_OFFSET_RESET", "earliest"),
        sink_value_format=value_format,
        mongo_uri=_require("MONGO_URI"),
        mongo_database=_require("MONGO_DATABASE"),
        mongo_collection=_require("MONGO_COLLECTION"),
        delivery_timeout_seconds=_get_int("KAFKA_DELIVERY_TIMEOUT_SECONDS", 30),
    )