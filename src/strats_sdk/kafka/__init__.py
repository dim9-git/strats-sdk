"""Shared Kafka bus, event schemas, and consume/publish helpers.

Install with the optional extra: ``pip install 'strats-sdk[kafka]'``.

Event schemas live in ``strats_sdk.messaging`` and are re-exported here for
backward compatibility.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any, TypeVar

try:
    from kafka import KafkaConsumer, KafkaProducer
    from kafka.errors import KafkaError
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "kafka-python is required for strats_sdk.kafka. "
        "Install with: pip install 'strats-sdk[kafka]'"
    ) from exc

from strats_sdk.messaging.events import (
    TOPIC_BARS_FETCHED,
    TOPIC_NOTIFICATIONS_SEND,
    BarsFetchedEvent,
    NotificationEvent,
)

logger = logging.getLogger(__name__)
T = TypeVar("T")

__all__ = [
    "TOPIC_BARS_FETCHED",
    "TOPIC_NOTIFICATIONS_SEND",
    "BarsFetchedEvent",
    "NotificationEvent",
    "KafkaBus",
    "publish",
    "consume_forever",
]


class KafkaBus:
    """Producer/consumer factory with SASL/SSL and poll/logging knobs."""

    def __init__(
        self,
        bootstrap_servers: str,
        *,
        security_protocol: str = "PLAINTEXT",
        sasl_mechanism: str = "PLAIN",
        sasl_username: str = "",
        sasl_password: str = "",
        ssl_check_hostname: bool = True,
        ssl_verify: bool = True,
        poll_timeout_ms: int = 5000,
        idle_log_every: int = 0,
        max_request_size: int = 5_000_000,
        max_partition_fetch_bytes: int = 5_000_000,
        consumer_timeout_ms: int = 1000,
        acks: str | int = "all",
        retries: int = 5,
        linger_ms: int = 20,
    ) -> None:
        if not bootstrap_servers.strip():
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS is required")
        self.bootstrap_servers = bootstrap_servers.strip()
        self.security_protocol = security_protocol.strip().upper() or "PLAINTEXT"
        self.sasl_mechanism = (sasl_mechanism or "PLAIN").strip().upper()
        self.sasl_username = sasl_username
        self.sasl_password = sasl_password
        self.ssl_check_hostname = ssl_check_hostname
        self.ssl_verify = ssl_verify
        self.poll_timeout_ms = poll_timeout_ms
        self.idle_log_every = idle_log_every
        self.max_request_size = max_request_size
        self.max_partition_fetch_bytes = max_partition_fetch_bytes
        self.consumer_timeout_ms = consumer_timeout_ms
        self.acks = acks
        self.retries = retries
        self.linger_ms = linger_ms

    def _client_kwargs(self) -> dict[str, Any]:
        servers = [s.strip() for s in self.bootstrap_servers.split(",") if s.strip()]
        kwargs: dict[str, Any] = {
            "bootstrap_servers": servers,
            "security_protocol": self.security_protocol,
        }
        if self.security_protocol.startswith("SASL"):
            if not self.sasl_username or not self.sasl_password:
                raise ValueError(
                    "KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD are required "
                    f"when security_protocol={self.security_protocol}"
                )
            kwargs["sasl_mechanism"] = self.sasl_mechanism
            kwargs["sasl_plain_username"] = self.sasl_username
            kwargs["sasl_plain_password"] = self.sasl_password
        if self.security_protocol in {"SSL", "SASL_SSL"}:
            kwargs["ssl_check_hostname"] = self.ssl_check_hostname
            if not self.ssl_verify:
                import ssl

                ctx = ssl.create_default_context()
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
                kwargs["ssl_context"] = ctx
        return kwargs

    def producer(self) -> KafkaProducer:
        try:
            return KafkaProducer(
                **self._client_kwargs(),
                acks=self.acks,
                retries=self.retries,
                linger_ms=self.linger_ms,
                max_request_size=self.max_request_size,
                value_serializer=lambda v: v if isinstance(v, (bytes, bytearray)) else bytes(v),
                key_serializer=lambda k: None
                if k is None
                else (k if isinstance(k, bytes) else str(k).encode()),
            )
        except KafkaError as exc:
            raise RuntimeError(f"No Kafka brokers at {self.bootstrap_servers!r}") from exc

    def consumer(
        self,
        topics: list[str],
        *,
        group_id: str,
        auto_offset_reset: str = "latest",
    ) -> KafkaConsumer:
        try:
            return KafkaConsumer(
                *topics,
                **self._client_kwargs(),
                group_id=group_id,
                auto_offset_reset=auto_offset_reset,
                enable_auto_commit=True,
                value_deserializer=lambda v: v,
                key_deserializer=lambda k: None if k is None else k.decode(),
                consumer_timeout_ms=self.consumer_timeout_ms,
                max_partition_fetch_bytes=self.max_partition_fetch_bytes,
            )
        except KafkaError as exc:
            raise RuntimeError(f"No Kafka brokers at {self.bootstrap_servers!r}") from exc


def publish(producer: KafkaProducer, topic: str, value: bytes, *, key: str | None = None) -> None:
    producer.send(topic, value=value, key=key).get(timeout=30)


def consume_forever(
    consumer: KafkaConsumer,
    parse: Callable[[bytes], T],
    handle: Callable[[T], None],
    *,
    poll_timeout_ms: int = 5000,
    idle_log_every: int = 0,
) -> None:
    """Poll forever; log when messages arrive. Idle wait logs are off by default."""
    idle_ticks = 0
    try:
        while True:
            polled = consumer.poll(timeout_ms=poll_timeout_ms)
            if not polled:
                idle_ticks += 1
                if idle_log_every and idle_ticks % idle_log_every == 0:
                    logger.info("Waiting for messages…")
                continue
            idle_ticks = 0
            n = sum(len(records) for records in polled.values())
            logger.info("Received %d message(s)", n)
            for _tp, records in polled.items():
                for record in records:
                    try:
                        handle(parse(record.value))
                    except Exception:
                        logger.exception(
                            "Failed handling message topic=%s offset=%s",
                            record.topic,
                            record.offset,
                        )
    finally:
        consumer.close()
