# RabbitMQ Asynchronous Order Processing

A small asynchronous messaging system built with **Python** and **RabbitMQ**. An **Order Producer** publishes JSON order events to a durable queue. Two or more **Order Workers**, each running as its own process, compete for those events. Each worker processes an order and then acknowledges it. A worker can be made to crash on purpose before it ACKs a message, and RabbitMQ then redelivers that message to another worker.

```
                       durable queue "orders"
 ┌──────────────┐   persistent JSON messages   ┌─────────────────────┐
 │ producer.py  │ ───────────────────────────► │      RabbitMQ       │
 │ (publisher   │      publisher confirms      │  (Docker container) │
 │  confirms)   │                              └──────────┬──────────┘
 └──────────────┘                   prefetch=1, manual ACK│  round-robin / fair dispatch
                                       ┌──────────────────┴──────────────────┐
                                       ▼                                     ▼
                               ┌───────────────┐                     ┌───────────────┐
                               │ worker.py     │                     │ worker.py     │
                               │ --name        │                     │ --name        │
                               │   Worker-1    │                     │   Worker-2    │
                               └───────────────┘                     └───────────────┘
```

## Project structure

| File | Purpose |
|---|---|
| `docker-compose.yml` | RabbitMQ 3.13 with the management UI, a health check and a persistent volume |
| `common.py` | Shared connection settings (env-configurable), retrying connect, queue declaration, logging |
| `producer.py` | Order Producer: publishes persistent JSON orders with publisher confirms |
| `worker.py` | Order Worker: manual ACKs, `prefetch_count=1`, simulated processing, failure mode |
| `demo.py` | Automated, reproducible end-to-end demo that **checks** the results from the logs |
| `requirements.txt` | Python dependency (`pika`) |

## Prerequisites

- Python 3.9+
- Docker Desktop (or Docker Engine) with Docker Compose v2

## Setup

```bash
# 1. Start RabbitMQ (waits until the broker is healthy)
docker compose up -d --wait

# 2. Create a virtual environment and install the dependency
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

Management UI: <http://localhost:15672> (user `guest`, password `guest`). Open **Queues → orders** to watch the *Ready* and *Unacked* counts.

## Message format

Every message is a JSON object. The four required fields are always present:

```json
{
  "event_id": "evt-0001",
  "order_id": "ord-1001",
  "item_id": "item-webcam",
  "quantity": 1,
  "created_at": "2026-10-26T22:43:07+00:00"
}
```

Each message is published with `delivery_mode=2` (persistent), `content_type=application/json` and `message_id=<event_id>`.

---

## Quick start: one-command demo

```bash
python demo.py
```

`demo.py` launches every component as a **separate OS process** and works through these steps:

1. Purges the `orders` queue so the run starts from a known state.
2. Starts `Worker-1` and `Worker-2`, both with `--fail-on-event evt-fail`.
3. Runs the producer, which publishes `evt-0001` … `evt-0010` plus `evt-fail`.
4. The worker that receives `evt-fail` logs `RECEIVED`, then `CRASHING`, and exits **without ACKing**. RabbitMQ requeues the message and delivers it to the other worker (`redelivered=True`), which completes it.
5. Restarts the crashed worker and publishes `evt-0011` … `evt-0014`. Both workers take part again, which shows the system kept running.
6. Reads `logs/worker-1.log` and `logs/worker-2.log`, prints a summary table and **verifies** the results:

```
event_id   received by (redelivered?)                 completed by
------------------------------------------------------------------------
evt-0001   Worker-2(-)                                Worker-2
evt-0002   Worker-1(-)                                Worker-1
...
evt-fail   Worker-2(-), Worker-1(R)                   Worker-1
========================================================================
  [PASS] every event completed exactly once
  [PASS] no unexpected events completed
  [PASS] both workers processed orders {'Worker-2': 3, 'Worker-1': 12}
  [PASS] evt-fail first received by Worker-2 (redelivered=False)
  [PASS] evt-fail redelivered to the other worker (redelivered=True)
  [PASS] evt-fail completed by the surviving worker

Result: ALL CHECKS PASSED
```

The exit code is `0` only when every check passes. Options: `--count`, `--second-batch` and `--work-seconds`.

> **Why the outcome is deterministic.** A worker crashes on the fail event **only on its first delivery** (`redelivered=False`). Both workers have the flag, so whichever one RabbitMQ picks first crashes, and the copy that is redelivered is always processed. The result is the same whatever the dispatch order. The only thing that changes between runs is *which* worker plays the crashing role.

---

## Manual demo (separate terminals)

Activate the virtual environment in each terminal first.

### A. Competing consumers: each message is processed once

```bash
# Terminal 1
python worker.py --name Worker-1

# Terminal 2
python worker.py --name Worker-2

# Terminal 3
python producer.py --count 10
```

Every `event_id` appears as `COMPLETED` in **exactly one** worker's terminal, and the work alternates between the two workers. Because of `prefetch_count=1`, a worker gets its next message only after it has ACKed the current one. A slower worker therefore receives fewer messages instead of building up a backlog. You can see this by starting one worker with `--work-seconds 3`.

### B. Failure and redelivery (fully deterministic)

```bash
# Terminal 1: Worker-1 is set to crash on evt-fail
python worker.py --name Worker-1 --fail-on-event evt-fail

# Terminal 3: publish only the fail event
python producer.py --count 0 --fail-event evt-fail
```

Worker-1 output:

```
worker=Worker-1 status=RECEIVED   event_id=evt-fail order_id=ord-9999 redelivered=False
worker=Worker-1 status=CRASHING   event_id=evt-fail order_id=ord-9999 redelivered=False reason=--fail-on-event (exiting BEFORE ack)
```

The process exits with code 3. The message was never ACKed, so in the management UI the queue goes back to **Ready = 1**: the message was not lost.

```bash
# Terminal 2: start a second worker
python worker.py --name Worker-2
```

```
worker=Worker-2 status=RECEIVED   event_id=evt-fail order_id=ord-9999 redelivered=True
worker=Worker-2 status=PROCESSING event_id=evt-fail order_id=ord-9999 redelivered=True
worker=Worker-2 status=COMPLETED  event_id=evt-fail order_id=ord-9999 redelivered=True reserved=1xitem-webcam total=$29 acked=True
```

RabbitMQ set the `redelivered=True` flag. The system keeps running: run `python producer.py --count 5 --start 20`, or restart Worker-1, and processing carries on as normal.

To run the crash while both workers are already consuming, start **both** workers with `--fail-on-event evt-fail` and run `python producer.py --count 10 --fail-event evt-fail`. This is the setup `demo.py` uses.

### Configuration

| Worker option | Env var | Default | Meaning |
|---|---|---|---|
| `--name` | `WORKER_NAME` | `Worker-<pid>` | Name shown in logs |
| `--fail-on-event` | `FAIL_ON_EVENT` | *(off)* | Crash before ACK on the first delivery of this `event_id` |
| `--work-seconds` | `WORK_SECONDS` | `1.0` | Simulated processing time |
| `--prefetch` | | `1` | `basic.qos` prefetch count |

| Producer option | Default | Meaning |
|---|---|---|
| `--count` | `10` | Number of regular orders |
| `--start` | `1` | First event number (`--start 11` → `evt-0011`) |
| `--fail-event` | *(none)* | Also publish an order with this `event_id` |
| `--fail-position` | `3` | Position of the fail event within the batch |
| `--delay` | `0` | Seconds between publishes |
| `--seed` | `42` | Seed for item and quantity values (repeatable data) |

Broker connection settings: `RABBITMQ_HOST`, `RABBITMQ_PORT`, `RABBITMQ_USER`, `RABBITMQ_PASSWORD`, `ORDER_QUEUE` (see `.env.example`).

---

## How each requirement is met

| Requirement | Implementation |
|---|---|
| Independent producer and workers | Separate scripts and separate OS processes; they share only the queue name |
| JSON orders with `event_id`, `order_id`, `item_id`, `quantity` | `producer.build_orders()`; the worker validates `REQUIRED_FIELDS` |
| Durable queue | `queue_declare(durable=True)` in `common.declare_queue()` |
| Persistent messages | `delivery_mode=Persistent`. Tested: published messages were still in the queue after `docker compose restart` |
| Reliable publishing | `confirm_delivery()` plus `mandatory=True` (an unroutable message raises an error) |
| Logs worker, event_id, order_id, status | Each line looks like `worker=… status=… event_id=… order_id=… redelivered=…` |
| Simulated business action | `process_order()` waits, reserves stock and calculates a total |
| Manual ACK, only after processing | `auto_ack=False`; `basic_ack` is called after `process_order()` returns |
| Fair dispatch and competing consumers | `basic_qos(prefetch_count=1)` with two consumers on one queue |
| Crash before ACK | `--fail-on-event`: logs `RECEIVED` and `CRASHING`, then calls `os._exit(3)` with no ACK |
| Redelivery to another worker | The broker requeues the unacked message when the connection drops; the next worker sees `redelivered=True` |
| System keeps running | The surviving worker continues; the restarted worker rejoins (step 5 of `demo.py`) |

## Design notes

- **At-least-once delivery.** An ACK is sent only after processing finishes. A crash *after* the work is done but *before* the ACK would therefore cause the message to be processed twice. A production system would make the handler **idempotent**, for example by storing processed `event_id`s in a database with a unique constraint and skipping duplicates. The demo crashes *before* the work starts, so each order is completed exactly once.
- **Poison messages.** A message that isn't valid JSON, or that is missing a required field, is rejected with `requeue=False`. Requeuing it would only redeliver it in an endless loop. In a real system it would be routed to a dead-letter exchange.
- **Graceful shutdown.** Ctrl+C or SIGTERM stops consuming and closes the connection. Any message that was in progress and not yet ACKed is requeued by the broker.
- **Start-up order does not matter.** Every component declares the queue (the declaration is idempotent) and retries the connection while RabbitMQ is still starting.

## Cleanup

```bash
docker compose down        # stop the broker (queued messages are kept in the volume)
docker compose down -v     # also delete the volume and all queued messages
```
