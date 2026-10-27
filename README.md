# RabbitMQ Asynchronous Order Processing

**CMPE-273 Distributed Systems: Asynchronous Messaging Lab**

This is an asynchronous messaging system built with **Python** and **RabbitMQ**:

- An **Order Producer** publishes JSON order events to a durable queue.
- Two **Order Workers** run as independent processes and compete for those events. They use manual acknowledgements and fair dispatch.
- A worker can be made to **crash before it ACKs** a message, which shows RabbitMQ **redelivering** that message to the other worker.
- Workers are **idempotent**. They share a record of processed `event_id`s, so a repeated event is detected, its business action is skipped, and it is ACKed.

## Architecture

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
                               │   Worker-1    │                     │   Worker-2    │
                               └───────┬───────┘                     └───────┬───────┘
                                       │   check / record event_id (locked)  │
                                       └──────────────┐   ┌──────────────────┘
                                                      ▼   ▼
                                        state/processed_events.json
                                        (shared idempotency store)
```

## Project structure

| File | Purpose |
|---|---|
| `docker-compose.yml` | RabbitMQ 3.13 with the management UI, a health check and a persistent volume |
| `common.py` | Shared connection settings (configurable through env vars), connection retries, queue declaration, logging |
| `producer.py` | Order Producer: publishes persistent JSON orders with publisher confirms. It can also publish a fail event and duplicate events |
| `worker.py` | Order Worker: manual ACKs, `prefetch_count=1`, simulated processing, crash-before-ACK mode, duplicate handling |
| `idempotency.py` | Processed-events store saved to a file and safe to share between worker processes |
| `demo.py` | Automated end-to-end demo that **checks** the results using the logs and the store file |
| `evidence/` | Real output from a `python demo.py` run (console output, each process's log, the store file) |
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
  "created_at": "2026-10-27T01:20:58+00:00"
}
```

Each message is published with `delivery_mode=2` (persistent), `content_type=application/json` and `message_id=<event_id>`.

---

## Quick start: one-command demo

```bash
python demo.py
```

`demo.py` launches every component as a **separate OS process**, works through the steps below, and exits with code `0` only if every check passes.

| Step | What happens | What it shows |
|---|---|---|
| 1 | Purges the queue and resets `state/` | The run starts from a known state |
| 2 | Starts `Worker-1` and `Worker-2`, both with `--fail-on-event evt-fail` | Two independent consumer processes |
| 3 | Publishes `evt-0001` … `evt-0010` plus `evt-fail` | Competing consumers |
| 4 | One worker crashes on `evt-fail` **before ACKing**; RabbitMQ redelivers it to the other worker | Failure and redelivery |
| 5 | Restarts the crashed worker (it reloads the store) and publishes `evt-0011` … `evt-0014` | The system recovers and keeps running |
| 6 | Publishes **two identical copies** of `evt-duplicate` while both workers are idle | Duplicate handled across workers |
| 7 | Replays `evt-0001` and `evt-0002`, which were already processed | The store survives restarts and is shared by both workers |

Options: `--count`, `--second-batch` and `--work-seconds`.

> **Why the crash is deterministic.** A worker crashes on the fail event only on its **first** delivery (`redelivered=False`). Both workers have the flag, so whichever one RabbitMQ picks first crashes, and the redelivered copy is always processed. The only thing that changes between runs is *which* worker plays the crashing role.

---

## Evidence (actual run)

The full, unedited output of the run below is in [`evidence/`](evidence/):

- [`demo_run.txt`](evidence/demo_run.txt)
- [`worker-1.log`](evidence/worker-1.log)
- [`worker-2.log`](evidence/worker-2.log)
- [`producer.log`](evidence/producer.log)
- [`processed_events.json`](evidence/processed_events.json)

In this run **Worker-2** received `evt-fail` first and crashed. The *Successful message processing* and *Competing consumers* log blocks are copied verbatim from [`evidence/demo_run.txt`](evidence/demo_run.txt). The excerpts in sections 2–4 leave out the `[Worker-N]` console prefix and some column padding. The files themselves are untouched.

**Where to find each requirement:**

| Requirement | Section |
|---|---|
| Running RabbitMQ, the producer and the workers | [Setup](#setup), [Quick start](#quick-start-one-command-demo), [Manual demo](#manual-demo-separate-terminals) |
| Architecture diagram | [Architecture](#architecture) |
| Successful message processing, ACK after processing | [Successful message processing](#successful-message-processing) |
| Two competing consumers | [1. Competing consumers](#1-competing-consumers-each-message-goes-to-one-worker) |
| Failure before ACK, redelivery with `redelivered=True`, system keeps running | [2. Failure before ACK, then redelivery](#2-failure-before-ack-then-redelivery) |
| Duplicate detected, business action skipped | [3. Duplicate event](#3-duplicate-event-across-two-workers-business-action-runs-once), [4. Replay after restart](#4-replay-after-restart-the-store-is-persistent-and-shared) |
| Reflection | [Reflection](#reflection) |

### Successful message processing

One normal order (`evt-0011`) handled by Worker-1. These are raw lines from [`evidence/demo_run.txt`](evidence/demo_run.txt), and the same lines appear in [`evidence/worker-1.log`](evidence/worker-1.log):

```
  [Worker-1] 18:21:04.290 | Worker-1  | worker=Worker-1 status=RECEIVED   event_id=evt-0011 order_id=ord-1011 redelivered=False
  [Worker-1] 18:21:04.291 | Worker-1  | worker=Worker-1 status=PROCESSING event_id=evt-0011 order_id=ord-1011 redelivered=False
  [Worker-1] 18:21:04.799 | Worker-1  | worker=Worker-1 status=COMPLETED  event_id=evt-0011 order_id=ord-1011 redelivered=False reserved=1xitem-webcam total=$29 business_action=EXECUTED acked=True
```

The worker logs `RECEIVED` when the message arrives and `PROCESSING` when it starts the business action (reserving stock and calculating the total). Only after that action has finished does it send the ACK, which is why the `COMPLETED` line, logged right after `basic_ack`, shows `business_action=EXECUTED acked=True`. Until then, the message stays *Unacked* in RabbitMQ. If the worker had died before this point, the message would have been redelivered (see section 2).

### 1. Competing consumers: each message goes to one worker

Worker-1 and Worker-2 are **separate OS processes**, each with its own connection to RabbitMQ, and both consume from the **same queue**, `orders`. The raw, interleaved output below covers step 5 of the demo, when both workers are running (copied verbatim from [`evidence/demo_run.txt`](evidence/demo_run.txt)):

```
  [Worker-1] 18:21:04.290 | Worker-1  | worker=Worker-1 status=RECEIVED   event_id=evt-0011 order_id=ord-1011 redelivered=False
  [Worker-1] 18:21:04.291 | Worker-1  | worker=Worker-1 status=PROCESSING event_id=evt-0011 order_id=ord-1011 redelivered=False
  [Producer] 18:21:04.292 | Producer  | status=PUBLISHED event_id=evt-0011 order_id=ord-1011 item_id=item-webcam quantity=1
  [Worker-2] 18:21:04.293 | Worker-2  | worker=Worker-2 status=RECEIVED   event_id=evt-0012 order_id=ord-1012 redelivered=False
  [Worker-2] 18:21:04.293 | Worker-2  | worker=Worker-2 status=PROCESSING event_id=evt-0012 order_id=ord-1012 redelivered=False
  [Producer] 18:21:04.293 | Producer  | status=PUBLISHED event_id=evt-0012 order_id=ord-1012 item_id=item-keyboard quantity=3
  [Producer] 18:21:04.295 | Producer  | status=PUBLISHED event_id=evt-0013 order_id=ord-1013 item_id=item-mouse quantity=2
  [Producer] 18:21:04.296 | Producer  | status=PUBLISHED event_id=evt-0014 order_id=ord-1014 item_id=item-mouse quantity=1
  [Producer] 18:21:04.296 | Producer  | Published 4 message(s) to queue 'orders'
  [Worker-1] 18:21:04.799 | Worker-1  | worker=Worker-1 status=COMPLETED  event_id=evt-0011 order_id=ord-1011 redelivered=False reserved=1xitem-webcam total=$29 business_action=EXECUTED acked=True
  [Worker-1] 18:21:04.802 | Worker-1  | worker=Worker-1 status=RECEIVED   event_id=evt-0013 order_id=ord-1013 redelivered=False
  [Worker-2] 18:21:04.802 | Worker-2  | worker=Worker-2 status=COMPLETED  event_id=evt-0012 order_id=ord-1012 redelivered=False reserved=3xitem-keyboard total=$225 business_action=EXECUTED acked=True
  [Worker-1] 18:21:04.802 | Worker-1  | worker=Worker-1 status=PROCESSING event_id=evt-0013 order_id=ord-1013 redelivered=False
  [Worker-2] 18:21:04.804 | Worker-2  | worker=Worker-2 status=RECEIVED   event_id=evt-0014 order_id=ord-1014 redelivered=False
  [Worker-2] 18:21:04.804 | Worker-2  | worker=Worker-2 status=PROCESSING event_id=evt-0014 order_id=ord-1014 redelivered=False
  [Worker-1] 18:21:05.309 | Worker-1  | worker=Worker-1 status=COMPLETED  event_id=evt-0013 order_id=ord-1013 redelivered=False reserved=2xitem-mouse total=$98 business_action=EXECUTED acked=True
  [Worker-2] 18:21:05.311 | Worker-2  | worker=Worker-2 status=COMPLETED  event_id=evt-0014 order_id=ord-1014 redelivered=False reserved=1xitem-mouse total=$49 business_action=EXECUTED acked=True
```

- **Worker-1** received and completed `evt-0011` and `evt-0013`.
- **Worker-2** received and completed `evt-0012` and `evt-0014`.
- In normal processing each message is delivered to **only one** worker. No `event_id` appears in both workers' output.
- The workers use `basic_qos(prefetch_count=1)` (fair dispatch). RabbitMQ never gives a worker a second message while it still has one unacknowledged, so a busy worker receives nothing new until it ACKs. Here Worker-1 receives `evt-0013` only after it has ACKed `evt-0011`, and Worker-2 receives `evt-0014` only after it has ACKed `evt-0012`.

The summary table printed by `demo.py` for the whole run confirms that each event was executed by exactly one worker:

```
event_id       received by (R = redelivered)              executed by  duplicate skipped by
------------------------------------------------------------------------------------------------
evt-0001       Worker-2(-), Worker-2(-)                   Worker-2     Worker-2
evt-0002       Worker-1(-), Worker-1(-)                   Worker-1     Worker-1
evt-0003       Worker-1(-)                                Worker-1     -
evt-0004       Worker-1(-)                                Worker-1     -
evt-0005       Worker-1(-)                                Worker-1     -
evt-0006       Worker-1(-)                                Worker-1     -
evt-0007       Worker-1(-)                                Worker-1     -
evt-0008       Worker-1(-)                                Worker-1     -
evt-0009       Worker-1(-)                                Worker-1     -
evt-0010       Worker-1(-)                                Worker-1     -
evt-0011       Worker-1(-)                                Worker-1     -
evt-0012       Worker-2(-)                                Worker-2     -
evt-0013       Worker-1(-)                                Worker-1     -
evt-0014       Worker-2(-)                                Worker-2     -
evt-duplicate  Worker-2(-), Worker-1(-)                   Worker-2     Worker-1
evt-fail       Worker-2(-), Worker-1(R)                   Worker-1     -
```

Worker-1 handled most of batch 1 because Worker-2 was down from the crash until its restart in step 5. The second receipts of `evt-0001` and `evt-0002` are the deliberate replays from step 7.

### 2. Failure before ACK, then redelivery

```
18:20:59.506 | Worker-2 | worker=Worker-2 status=RECEIVED   event_id=evt-fail order_id=ord-9999 redelivered=False
18:20:59.506 | Worker-2 | worker=Worker-2 status=CRASHING   event_id=evt-fail order_id=ord-9999 redelivered=False reason=--fail-on-event (exiting BEFORE ack)
18:20:59.630 | Demo     | Worker-2 exited with code 3 (no ACK sent)
18:21:00.016 | Worker-1 | worker=Worker-1 status=RECEIVED   event_id=evt-fail order_id=ord-9999 redelivered=True
18:21:00.017 | Worker-1 | worker=Worker-1 status=PROCESSING event_id=evt-fail order_id=ord-9999 redelivered=True
18:21:00.523 | Worker-1 | worker=Worker-1 status=COMPLETED  event_id=evt-fail order_id=ord-9999 redelivered=True reserved=5xitem-keyboard total=$375 business_action=EXECUTED acked=True
```

RabbitMQ marked the second delivery `redelivered=True`. Worker-1 kept processing the rest of the queue, and the restarted Worker-2 rejoined in step 5.

### 3. Duplicate event across two workers: business action runs once

Two identical copies of `evt-duplicate` arrived at **both** workers within 1 ms of each other. Worker-1 waited on the per-event lock until Worker-2 had finished and recorded the event. It then detected the duplicate, skipped the business action and ACKed:

```
18:21:05.390 | Worker-2 | worker=Worker-2 status=RECEIVED   event_id=evt-duplicate order_id=ord-8888 redelivered=False
18:21:05.390 | Worker-2 | worker=Worker-2 status=PROCESSING event_id=evt-duplicate order_id=ord-8888 redelivered=False
18:21:05.391 | Worker-1 | worker=Worker-1 status=RECEIVED   event_id=evt-duplicate order_id=ord-8888 redelivered=False
18:21:05.898 | Worker-2 | worker=Worker-2 status=COMPLETED  event_id=evt-duplicate order_id=ord-8888 redelivered=False reserved=1xitem-webcam total=$29 business_action=EXECUTED acked=True
18:21:05.899 | Worker-1 | worker=Worker-1 status=DUPLICATE  event_id=evt-duplicate order_id=ord-8888 redelivered=False business_action=SKIPPED first_processed_by=Worker-2 acked=True
```

### 4. Replay after restart: the store is persistent and shared

The restarted Worker-2 loaded the 11 events already recorded in the store. Re-published copies of `evt-0001` and `evt-0002` were skipped:

```
18:21:04.205 | Worker-2 | worker=Worker-2 status=READY      ... store=state/processed_events.json known_processed_events=11
18:21:06.095 | Worker-2 | worker=Worker-2 status=DUPLICATE  event_id=evt-0001 order_id=ord-1001 redelivered=False business_action=SKIPPED first_processed_by=Worker-2 acked=True
18:21:06.097 | Worker-1 | worker=Worker-1 status=DUPLICATE  event_id=evt-0002 order_id=ord-1002 redelivered=False business_action=SKIPPED first_processed_by=Worker-1 acked=True
18:21:06.103 | Worker-1 | worker=Worker-1 status=STOPPING  processed=12 duplicates_skipped=2
18:21:06.112 | Worker-2 | worker=Worker-2 status=STOPPING  processed=3 duplicates_skipped=1
```

(`processed=3` counts only the restarted Worker-2 process. Its first process had already executed `evt-0001` before it crashed.)

Store excerpt ([`evidence/processed_events.json`](evidence/processed_events.json), 16 entries):

```json
"evt-duplicate": {
  "processed_at": "2026-10-27T01:21:05.896+00:00",
  "result": "reserved=1xitem-webcam total=$29",
  "worker": "Worker-2"
},
"evt-fail": {
  "processed_at": "2026-10-27T01:21:00.522+00:00",
  "result": "reserved=5xitem-keyboard total=$375",
  "worker": "Worker-1"
}
```

### 5. Automated checks

```
  [INFO] evt-duplicate copies were delivered to: Worker-1, Worker-2 (cross-worker duplicate)
  [PASS] business action executed exactly once per event_id
  [PASS] no unexpected events completed
  [PASS] both workers processed orders {'Worker-2': 4, 'Worker-1': 12}
  [PASS] evt-fail first received by Worker-2 (redelivered=False)
  [PASS] evt-fail redelivered to the other worker (redelivered=True)
  [PASS] evt-fail completed by the surviving worker
  [PASS] evt-duplicate: 2 copies received, executed once, skipped once
  [PASS] replayed evt-0001, evt-0002 after restart detected as DUPLICATE (not re-executed)
  [PASS] state/processed_events.json records all 16 processed event_ids

Result: ALL CHECKS PASSED  (logs in logs/)
```

---

## Manual demo (separate terminals)

Activate the virtual environment in each terminal first. To start from an empty idempotency store, delete the `state/` folder.

### A. Competing consumers

```bash
python worker.py --name Worker-1          # Terminal 1
python worker.py --name Worker-2          # Terminal 2
python producer.py --count 10             # Terminal 3
```

Every `event_id` appears as `COMPLETED` in exactly one worker's terminal. Because of `prefetch_count=1`, a slower worker receives fewer messages instead of building up a backlog. You can see this by starting one worker with `--work-seconds 3`.

### B. Failure and redelivery (fully deterministic)

```bash
python worker.py --name Worker-1 --fail-on-event evt-fail     # Terminal 1
python producer.py --count 0 --fail-event evt-fail            # Terminal 3
```

Worker-1 logs `RECEIVED` and then `CRASHING`, and exits with code 3. The message was never ACKed, so the management UI shows **Ready = 1**: it was not lost. Now start the second worker:

```bash
python worker.py --name Worker-2                              # Terminal 2
```

Worker-2 receives `evt-fail` with `redelivered=True` and completes it. To run the crash while both workers are already consuming, start both with `--fail-on-event evt-fail`, as `demo.py` does.

### C. Duplicate events

With both workers running:

```bash
python producer.py --count 0 --duplicate-event evt-duplicate --duplicate-copies 2
```

One worker logs `status=COMPLETED ... business_action=EXECUTED`. The other logs `status=DUPLICATE ... business_action=SKIPPED acked=True`. If you run the same command again, or replay older events with `python producer.py --count 2 --start 1`, every copy is logged as `DUPLICATE`, even after you restart the workers.

### Configuration

| Worker option | Env var | Default | Meaning |
|---|---|---|---|
| `--name` | `WORKER_NAME` | `Worker-<pid>` | Name shown in logs |
| `--fail-on-event` | `FAIL_ON_EVENT` | *(off)* | Crash before ACK on the first delivery of this `event_id` |
| `--work-seconds` | `WORK_SECONDS` | `1.0` | Simulated processing time |
| `--prefetch` | | `1` | `basic.qos` prefetch count |
| `--state-dir` | `STATE_DIR` | `state` | Folder for the shared idempotency store (all workers must use the same one) |

| Producer option | Default | Meaning |
|---|---|---|
| `--count` | `10` | Number of regular orders |
| `--start` | `1` | First event number (`--start 11` → `evt-0011`) |
| `--fail-event` | *(none)* | Also publish an order with this `event_id` |
| `--fail-position` | `3` | Position of the fail event within the batch |
| `--duplicate-event` | *(none)* | Also publish an order with this `event_id` several times, with an identical payload each time |
| `--duplicate-copies` | `2` | Number of copies of the duplicate event |
| `--delay` | `0` | Seconds between publishes |
| `--seed` | `42` | Seed for item and quantity values (repeatable data) |

Broker connection settings: `RABBITMQ_HOST`, `RABBITMQ_PORT`, `RABBITMQ_USER`, `RABBITMQ_PASSWORD`, `ORDER_QUEUE` (see `.env.example`).

---

## How each requirement is met

| Requirement | Implementation |
|---|---|
| Independent producer and workers | Separate scripts and separate OS processes; they share only the queue (and the store, for the workers) |
| JSON orders with `event_id`, `order_id`, `item_id`, `quantity` | `producer.build_orders()`; the worker checks `REQUIRED_FIELDS` |
| Durable queue, persistent messages | `queue_declare(durable=True)` and `delivery_mode=Persistent`. Tested: messages were still in the queue after `docker compose restart` |
| Reliable publishing | `confirm_delivery()` plus `mandatory=True` |
| Logs worker, event_id, order_id, status | Every line looks like `worker=… status=… event_id=… order_id=… redelivered=…` |
| Simulated business action | `process_order()` waits, reserves stock and calculates a total |
| Manual ACK, only after processing | `auto_ack=False`; `basic_ack` is called after `process_order()` returns and the event has been recorded |
| Competing consumers and fair dispatch | `basic_qos(prefetch_count=1)` with two consumers on one queue |
| Crash before ACK | `--fail-on-event`: logs `RECEIVED` and `CRASHING`, then calls `os._exit(3)` with no ACK |
| Redelivery to another worker | The broker requeues the unacked message when the connection drops; the next worker sees `redelivered=True` |
| System keeps running | The surviving worker continues; the restarted worker rejoins |
| **Duplicate handling (idempotency)** | `idempotency.ProcessedEventStore`, used by `OrderWorker.on_message` (details below) |

## How idempotent duplicate handling works

RabbitMQ provides **at-least-once** delivery. A message can arrive more than once, for example when a worker crashes after doing the work but before its ACK reaches the broker, or when a producer retries a publish. The workers therefore deduplicate by `event_id`:

```
on message:
    if first delivery of --fail-on-event id: crash (no ACK)          # failure demo
    with per-event lock(event_id):                                   # cross-process
        if event_id in processed_events.json:
            ACK; log DUPLICATE business_action=SKIPPED; return
        run business action
        record event_id in processed_events.json                    # BEFORE the ACK
    ACK; log COMPLETED business_action=EXECUTED
```

- **Shared across processes, and survives restarts.** An in-memory set would be private to each worker and lost on restart. All workers read and write `state/processed_events.json`, and each one loads it at start-up (`known_processed_events=N` in the `READY` log line).
- **No race between the two workers.** A per-event file lock (`state/locks/<event_id>.lock`) means that if both workers get the same `event_id` at once, the second waits for the first and then sees the record. Evidence §3 shows this happening. Different events use different locks, so the two workers still process in parallel.
- **No corrupted file.** Every read-modify-write of the store happens under a store lock, and the file is replaced atomically (written to a temp file, then `os.replace`).
- **No stale locks after a crash.** These are OS advisory locks (`flock` on Linux and macOS, `msvcrt` on Windows), which the OS releases when a process dies. A worker that crashes inside the lock therefore cannot block the others.
- **Record before ACK.** If a worker dies after recording an event but before ACKing it, the redelivered copy is detected as a duplicate and the business action is not run again.

**Known limitations, and what a production system would use instead:**
- The file store only works for workers on the **same machine** that share the `state/` folder. Workers on several hosts would use a shared database, such as a table with a `UNIQUE(event_id)` constraint, or Redis `SET NX`.
- A crash *after* the business action but *before* the record is written can still cause the action to run twice. Closing that gap needs the action and the record to be written in **one transaction**, for example the database update and the processed-event insert committed together.

## Reflection

RabbitMQ provides at-least-once delivery, so a message can be delivered more than once if a worker crashes or loses its connection before RabbitMQ receives the ACK. A producer retry can also publish the same event more than once. Idempotent handling records each processed `event_id` and skips the business action when that ID is received again. This prevents duplicate side effects while still acknowledging the repeated message.

## Other design notes

- **Poison messages.** A message that isn't valid JSON, or that is missing a required field, is rejected with `requeue=False`. Requeuing it would only redeliver it in an endless loop. In a real system it would be routed to a dead-letter exchange.
- **Graceful shutdown.** Ctrl+C or SIGTERM stops consuming and closes the connection. Any message that was in progress and not yet ACKed is requeued by the broker.
- **Start-up order does not matter.** Every component declares the queue (the declaration is idempotent) and retries the connection while RabbitMQ is still starting.

## Cleanup

```bash
docker compose down        # stop the broker (queued messages are kept in the volume)
docker compose down -v     # also delete the volume and all queued messages
rm -rf state logs          # reset the idempotency store and the local logs
```
