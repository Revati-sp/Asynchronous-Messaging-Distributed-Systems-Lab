"""Order Producer: publishes JSON order events to the durable RabbitMQ queue.

Examples:
    python producer.py                          # 10 orders: evt-0001 .. evt-0010
    python producer.py --count 10 --fail-event evt-fail
    python producer.py --count 4 --start 11     # evt-0011 .. evt-0014
"""

from __future__ import annotations

import argparse
import json
import random
import time
from datetime import datetime, timezone

import pika
from pika.exceptions import UnroutableError

from common import QUEUE_NAME, connect, declare_queue, setup_logging

ITEMS = ["item-keyboard", "item-mouse", "item-monitor", "item-laptop", "item-headset", "item-webcam"]

log = setup_logging("Producer")


def build_orders(count: int, start: int, fail_event: str | None, fail_position: int, seed: int) -> list[dict]:
    """Build a deterministic list of orders (same seed -> same items/quantities)."""
    rng = random.Random(seed)
    orders = []
    for i in range(start, start + count):
        orders.append({
            "event_id": f"evt-{i:04d}",
            "order_id": f"ord-{1000 + i}",
            "item_id": rng.choice(ITEMS),
            "quantity": rng.randint(1, 5),
            "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        })
    if fail_event:
        # A specially tagged event that a worker started with --fail-on-event will crash on.
        fail_order = {
            "event_id": fail_event,
            "order_id": "ord-9999",
            "item_id": rng.choice(ITEMS),
            "quantity": rng.randint(1, 5),
            "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
        position = max(0, min(fail_position - 1, len(orders)))
        orders.insert(position, fail_order)
    return orders


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish order events to RabbitMQ.")
    parser.add_argument("--count", type=int, default=10, help="number of regular orders to publish (default 10)")
    parser.add_argument("--start", type=int, default=1, help="first event number, e.g. 11 -> evt-0011 (default 1)")
    parser.add_argument("--fail-event", metavar="EVENT_ID", default=None,
                        help="also publish one order with this event_id (used for the failure demo)")
    parser.add_argument("--fail-position", type=int, default=3,
                        help="1-based position of the fail event in the batch (default 3)")
    parser.add_argument("--delay", type=float, default=0.0, help="seconds to wait between messages")
    parser.add_argument("--seed", type=int, default=42, help="random seed for item/quantity values")
    args = parser.parse_args()

    orders = build_orders(args.count, args.start, args.fail_event, args.fail_position, args.seed)

    connection = connect(log)
    channel = connection.channel()
    declare_queue(channel)
    # Publisher confirms: basic_publish blocks until the broker has taken responsibility
    # for the message (written to disk for persistent messages on a durable queue).
    channel.confirm_delivery()

    for order in orders:
        body = json.dumps(order)
        try:
            channel.basic_publish(
                exchange="",                 # default exchange routes by queue name
                routing_key=QUEUE_NAME,
                body=body,
                properties=pika.BasicProperties(
                    content_type="application/json",
                    delivery_mode=pika.DeliveryMode.Persistent,  # survive broker restart
                    message_id=order["event_id"],
                ),
                mandatory=True,              # error if the message cannot be routed to a queue
            )
        except UnroutableError:
            log.error("status=UNROUTABLE event_id=%s order_id=%s", order["event_id"], order["order_id"])
            continue
        log.info("status=PUBLISHED event_id=%s order_id=%s item_id=%s quantity=%d",
                 order["event_id"], order["order_id"], order["item_id"], order["quantity"])
        if args.delay:
            time.sleep(args.delay)

    log.info("Published %d message(s) to queue '%s'", len(orders), QUEUE_NAME)
    connection.close()


if __name__ == "__main__":
    main()
