"""Order Worker: a competing consumer on the durable order queue.

Each worker:
  * uses manual acknowledgements (auto_ack=False),
  * uses prefetch_count=1 (fair dispatch) so a busy worker is not handed more work,
  * ACKs a message only after the simulated business action has finished,
  * is idempotent: processed event_ids are recorded in a file shared by all workers
    (see idempotency.py); a repeated event_id is logged as DUPLICATE, the business
    action is skipped, and the message is ACKed.

Failure demo:
    python worker.py --name Worker-1 --fail-on-event evt-fail

When this worker receives `evt-fail` on its FIRST delivery it logs the receipt and
then hard-exits (os._exit) without ACKing. RabbitMQ sees the channel close with an
unacked message, requeues it, and redelivers it (redelivered=True) to another worker.
A redelivered copy is never crashed on, so the message is always completed in the end,
no matter which worker happens to get it first.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import time

from pika.exceptions import AMQPConnectionError, ConnectionClosedByBroker

from common import QUEUE_NAME, REQUIRED_FIELDS, connect, declare_queue, setup_logging
from idempotency import ProcessedEventStore

# Exit code used when the worker deliberately crashes for the failure demo.
CRASH_EXIT_CODE = 3


class OrderWorker:
    def __init__(self, name: str, fail_on_event: str | None, work_seconds: float, prefetch: int,
                 store: ProcessedEventStore):
        self.name = name
        self.fail_on_event = fail_on_event
        self.work_seconds = work_seconds
        self.prefetch = prefetch
        self.log = setup_logging(name)
        self.store = store
        self.processed = 0
        self.duplicates = 0
        self.connection = None
        self.channel = None

    # ---- logging helper -------------------------------------------------------
    def _event(self, status: str, order: dict, method=None, extra: str = "") -> None:
        redelivered = getattr(method, "redelivered", False)
        self.log.info(
            "worker=%s status=%-10s event_id=%s order_id=%s redelivered=%s%s",
            self.name, status, order.get("event_id"), order.get("order_id"),
            redelivered, f" {extra}" if extra else "",
        )

    # ---- business logic -------------------------------------------------------
    def process_order(self, order: dict) -> str:
        """Simulated business action: reserve stock and 'charge' for the order."""
        time.sleep(self.work_seconds)
        unit_price = 10 + (sum(map(ord, order["item_id"])) % 90)   # deterministic fake price
        total = unit_price * int(order["quantity"])
        return f"reserved={order['quantity']}x{order['item_id']} total=${total}"

    # ---- message callback -----------------------------------------------------
    def on_message(self, channel, method, properties, body: bytes) -> None:
        try:
            order = json.loads(body)
            missing = [f for f in REQUIRED_FIELDS if f not in order]
            if missing:
                raise ValueError(f"missing fields {missing}")
        except (ValueError, TypeError) as exc:
            # Malformed message: requeuing would loop forever, so reject it outright.
            self.log.error("worker=%s status=INVALID   body=%r error=%s", self.name, body[:200], exc)
            channel.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
            return

        self._event("RECEIVED", order, method)

        if (
            self.fail_on_event
            and order["event_id"] == self.fail_on_event
            and not method.redelivered
        ):
            self._event("CRASHING", order, method, "reason=--fail-on-event (exiting BEFORE ack)")
            logging.shutdown()          # flush log output before the hard exit
            sys.stdout.flush()
            # Hard exit: no ack, no clean channel close -> broker requeues the message.
            os._exit(CRASH_EXIT_CODE)

        event_id = order["event_id"]
        # Per-event lock: if another worker is processing a copy of this same event right
        # now, wait for it to finish, then the check below sees it as already processed.
        with self.store.event_lock(event_id):
            if self.store.is_processed(event_id):
                first = self.store.get(event_id) or {}
                channel.basic_ack(delivery_tag=method.delivery_tag)
                self.duplicates += 1
                self._event("DUPLICATE", order, method,
                            f"business_action=SKIPPED first_processed_by={first.get('worker')} acked=True")
                return

            self._event("PROCESSING", order, method)
            result = self.process_order(order)
            # Record BEFORE acking: a crash between the two leads to a redelivery that is
            # detected as a duplicate, instead of the business action running twice.
            self.store.mark_processed(event_id, self.name, result)
        # ACK only after processing has fully finished.
        channel.basic_ack(delivery_tag=method.delivery_tag)
        self.processed += 1
        self._event("COMPLETED", order, method, f"{result} business_action=EXECUTED acked=True")

    # ---- lifecycle ------------------------------------------------------------
    def run(self) -> None:
        self.connection = connect(self.log)
        self.channel = self.connection.channel()
        declare_queue(self.channel)
        # Fair dispatch: at most `prefetch` unacked messages per worker.
        self.channel.basic_qos(prefetch_count=self.prefetch)
        self.channel.basic_consume(queue=QUEUE_NAME, on_message_callback=self.on_message, auto_ack=False)
        known = self.store.load()
        self.log.info(
            "worker=%s status=READY      queue=%s prefetch=%d work_seconds=%.2f fail_on_event=%s "
            "store=%s known_processed_events=%d",
            self.name, QUEUE_NAME, self.prefetch, self.work_seconds, self.fail_on_event,
            self.store.path, known,
        )
        self.channel.start_consuming()

    def stop(self) -> None:
        self.log.info("worker=%s status=STOPPING  processed=%d duplicates_skipped=%d",
                      self.name, self.processed, self.duplicates)
        if self.channel is not None and self.channel.is_open:
            self.channel.stop_consuming()


def _raise_keyboard_interrupt(_signum, _frame) -> None:
    # Treat SIGTERM like Ctrl+C so both paths share the same graceful shutdown.
    raise KeyboardInterrupt


def main() -> None:
    parser = argparse.ArgumentParser(description="Consume and process order events from RabbitMQ.")
    parser.add_argument("--name", default=os.getenv("WORKER_NAME", f"Worker-{os.getpid()}"),
                        help="worker name shown in logs (env WORKER_NAME)")
    parser.add_argument("--fail-on-event", metavar="EVENT_ID", default=os.getenv("FAIL_ON_EVENT"),
                        help="crash before ACK the first time this event_id is delivered (env FAIL_ON_EVENT)")
    parser.add_argument("--work-seconds", type=float, default=float(os.getenv("WORK_SECONDS", "1.0")),
                        help="simulated processing time per order (default 1.0)")
    parser.add_argument("--prefetch", type=int, default=1, help="basic.qos prefetch_count (default 1)")
    parser.add_argument("--state-dir", default=os.getenv("STATE_DIR", "state"),
                        help="directory of the shared processed-events store (env STATE_DIR, default ./state)")
    args = parser.parse_args()

    store = ProcessedEventStore(args.state_dir)
    worker = OrderWorker(args.name, args.fail_on_event, args.work_seconds, args.prefetch, store)
    signal.signal(signal.SIGTERM, _raise_keyboard_interrupt)

    try:
        worker.run()
    except KeyboardInterrupt:
        worker.stop()
    except (ConnectionClosedByBroker, AMQPConnectionError) as exc:
        worker.log.error("worker=%s status=DISCONNECTED error=%r", worker.name, exc)
        sys.exit(1)
    finally:
        if worker.connection is not None and worker.connection.is_open:
            worker.connection.close()   # unacked messages (if any) are requeued by the broker


if __name__ == "__main__":
    main()
