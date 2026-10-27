"""Shared RabbitMQ settings and helpers used by both the producer and the workers.

All connection settings can be overridden with environment variables so the
same code runs against a local Docker broker or a remote one.
"""

from __future__ import annotations

import logging
import os
import sys
import time

import pika
from pika.exceptions import AMQPConnectionError

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")
QUEUE_NAME = os.getenv("ORDER_QUEUE", "orders")

REQUIRED_FIELDS = ("event_id", "order_id", "item_id", "quantity")


def setup_logging(name: str) -> logging.Logger:
    """Configure a line-buffered stdout logger with a timestamp and component name."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d | %(name)-9s | %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )
    # pika logs every failed connect attempt with a traceback; connect() already reports
    # retries in one line and every real failure surfaces as an exception, so mute pika.
    logging.getLogger("pika").setLevel(logging.CRITICAL)
    return logging.getLogger(name)


def connect(logger: logging.Logger, retries: int = 30, delay: float = 2.0) -> pika.BlockingConnection:
    """Open a blocking connection, retrying while the broker is still starting up."""
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD),
        heartbeat=30,
        blocked_connection_timeout=60,
    )
    for attempt in range(1, retries + 1):
        try:
            return pika.BlockingConnection(params)
        except AMQPConnectionError as exc:
            logger.warning(
                "RabbitMQ not reachable at %s:%s (attempt %d/%d): %r",
                RABBITMQ_HOST, RABBITMQ_PORT, attempt, retries, exc,
            )
            if attempt == retries:
                raise
            time.sleep(delay)
    raise RuntimeError("unreachable")


def declare_queue(channel) -> None:
    """Declare the durable order queue. Idempotent, so every component calls it."""
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
