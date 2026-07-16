"""Shared RabbitMQ bus, event schemas, and consume/publish helpers.

Install with the optional extra: ``pip install 'strats-sdk[rabbitmq]'``.

Kafka consumer-group semantics map to RabbitMQ as:

* logical topic → durable **fanout** exchange (same name)
* ``group_id`` → durable queue ``{topic}.{group_id}`` bound to that exchange

So each strategy / notifier group gets every message independently.
"""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, TypeVar
from urllib.parse import urlparse

try:
    import pika
    from pika.exceptions import AMQPConnectionError, AMQPError
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "pika is required for strats_sdk.rabbit. "
        "Install with: pip install 'strats-sdk[rabbitmq]'"
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
    "RabbitBus",
    "RabbitProducer",
    "RabbitConsumer",
    "publish",
    "consume_forever",
    "queue_name_for",
]


def queue_name_for(topic: str, group_id: str) -> str:
    """Durable queue name for a Kafka-style consumer group on ``topic``."""
    return f"{topic}.{group_id}"


@dataclass
class _Record:
    topic: str
    value: bytes
    offset: int
    key: str | None = None
    delivery_tag: int = 0


class RabbitProducer:
    """Publish bytes to a fanout exchange named after the logical topic."""

    def __init__(self, connection: pika.BlockingConnection, channel: Any) -> None:
        self._connection = connection
        self._channel = channel
        self._declared: set[str] = set()

    def _ensure_exchange(self, topic: str) -> None:
        if topic in self._declared:
            return
        self._channel.exchange_declare(exchange=topic, exchange_type="fanout", durable=True)
        self._declared.add(topic)

    def send(self, topic: str, value: bytes, *, key: str | None = None) -> None:
        body = value if isinstance(value, (bytes, bytearray)) else bytes(value)
        self._ensure_exchange(topic)
        props = pika.BasicProperties(
            delivery_mode=2,  # persistent
            content_type="application/json",
            message_id=key,
            headers={"x-key": key} if key is not None else None,
        )
        self._channel.basic_publish(
            exchange=topic,
            routing_key=key or "",
            body=body,
            properties=props,
        )

    def flush(self, timeout: float | int = 10) -> None:  # noqa: ARG002
        # BlockingChannel publishes synchronously; nothing to flush.
        return None

    def close(self) -> None:
        try:
            if self._connection.is_open:
                self._connection.close()
        except AMQPError:
            logger.exception("Error closing RabbitMQ producer connection")


class RabbitConsumer:
    """Poll messages from one durable queue per (topic, group_id)."""

    def __init__(
        self,
        connection: pika.BlockingConnection,
        channel: Any,
        *,
        topic_queues: list[tuple[str, str]],
        prefetch_count: int = 10,
    ) -> None:
        self._connection = connection
        self._channel = channel
        self._topic_queues = list(topic_queues)  # (topic, queue)
        self._queue_to_topic = {q: t for t, q in topic_queues}
        self._pending_acks: list[int] = []
        channel.basic_qos(prefetch_count=prefetch_count)
        for topic, queue in topic_queues:
            channel.exchange_declare(exchange=topic, exchange_type="fanout", durable=True)
            channel.queue_declare(queue=queue, durable=True)
            channel.queue_bind(exchange=topic, queue=queue)

    def poll(self, timeout_ms: int = 5000) -> dict[str, list[_Record]]:
        deadline = time.monotonic() + max(timeout_ms, 0) / 1000.0
        by_topic: dict[str, list[_Record]] = defaultdict(list)
        while True:
            got_any = False
            for _topic, queue in self._topic_queues:
                method, props, body = self._channel.basic_get(queue=queue, auto_ack=False)
                if method is None:
                    continue
                got_any = True
                key = None
                if props is not None:
                    key = props.message_id
                    if key is None and props.headers:
                        raw = props.headers.get("x-key")
                        key = raw.decode() if isinstance(raw, (bytes, bytearray)) else raw
                record = _Record(
                    topic=self._queue_to_topic.get(queue, queue),
                    value=body if isinstance(body, (bytes, bytearray)) else bytes(body or b""),
                    offset=method.delivery_tag,
                    key=key,
                    delivery_tag=method.delivery_tag,
                )
                by_topic[record.topic].append(record)
                self._pending_acks.append(method.delivery_tag)
            if by_topic:
                return dict(by_topic)
            if time.monotonic() >= deadline:
                return {}
            if not got_any:
                # Avoid busy-spin when queues are empty.
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return {}
                time.sleep(min(0.05, remaining))

    def commit(self) -> None:
        """Ack all messages returned by the last poll(s)."""
        for tag in self._pending_acks:
            try:
                self._channel.basic_ack(delivery_tag=tag)
            except AMQPError:
                logger.exception("Failed to ack delivery_tag=%s", tag)
        self._pending_acks.clear()

    def close(self) -> None:
        try:
            self.commit()
        finally:
            try:
                if self._connection.is_open:
                    self._connection.close()
            except AMQPError:
                logger.exception("Error closing RabbitMQ consumer connection")


class RabbitBus:
    """Producer/consumer factory for RabbitMQ (AMQP URL)."""

    def __init__(
        self,
        url: str,
        *,
        poll_timeout_ms: int = 5000,
        idle_log_every: int = 0,
        prefetch_count: int = 10,
        heartbeat: int = 60,
        blocked_connection_timeout: int = 30,
    ) -> None:
        url = (url or "").strip()
        if not url:
            raise ValueError("RABBITMQ_URL / AMQP_URL is required")
        parsed = urlparse(url)
        if parsed.scheme not in {"amqp", "amqps"}:
            raise ValueError(f"RabbitMQ URL must start with amqp:// or amqps://, got {parsed.scheme!r}")
        self.url = url
        self.poll_timeout_ms = poll_timeout_ms
        self.idle_log_every = idle_log_every
        self.prefetch_count = prefetch_count
        self.heartbeat = heartbeat
        self.blocked_connection_timeout = blocked_connection_timeout

    def _connect(self) -> pika.BlockingConnection:
        params = pika.URLParameters(self.url)
        params.heartbeat = self.heartbeat
        params.blocked_connection_timeout = self.blocked_connection_timeout
        try:
            return pika.BlockingConnection(params)
        except AMQPConnectionError as exc:
            raise RuntimeError(f"No RabbitMQ broker at {self.url!r}") from exc

    def producer(self) -> RabbitProducer:
        connection = self._connect()
        channel = connection.channel()
        return RabbitProducer(connection, channel)

    def consumer(
        self,
        topics: list[str],
        *,
        group_id: str,
        auto_offset_reset: str = "latest",  # noqa: ARG002 — API compat with KafkaBus
    ) -> RabbitConsumer:
        if not group_id.strip():
            raise ValueError("group_id is required")
        connection = self._connect()
        channel = connection.channel()
        topic_queues = [(t, queue_name_for(t, group_id)) for t in topics]
        return RabbitConsumer(
            connection,
            channel,
            topic_queues=topic_queues,
            prefetch_count=self.prefetch_count,
        )


def publish(producer: RabbitProducer, topic: str, value: bytes, *, key: str | None = None) -> None:
    producer.send(topic, value, key=key)


def consume_forever(
    consumer: RabbitConsumer,
    parse: Callable[[bytes], T],
    handle: Callable[[T], None],
    *,
    poll_timeout_ms: int = 5000,
    idle_log_every: int = 0,
) -> None:
    """Poll forever; ack after each batch (mirrors Kafka auto-commit)."""
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
            for _topic, records in polled.items():
                for record in records:
                    try:
                        handle(parse(record.value))
                    except Exception:
                        logger.exception(
                            "Failed handling message topic=%s delivery_tag=%s",
                            record.topic,
                            record.offset,
                        )
            consumer.commit()
    finally:
        consumer.close()
