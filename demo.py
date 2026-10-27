"""End-to-end, reproducible demonstration of competing consumers, redelivery and
idempotent duplicate handling.

What it does (each component runs as its own OS process):
  1. Purges the order queue and resets the processed-events store (./state).
  2. Starts Worker-1 and Worker-2, both with --fail-on-event evt-fail.
  3. Runs the producer: evt-0001..evt-0010 plus evt-fail.
  4. Whichever worker receives evt-fail first crashes BEFORE acking; RabbitMQ
     redelivers it to the surviving worker, which completes it.
  5. Restarts the crashed worker (it reloads the store from disk) and publishes a
     second batch, so both workers are consuming again.
  6. Publishes two identical copies of evt-duplicate while both workers are idle, so
     each worker normally gets one copy at the same moment. The business action must
     run once; the other copy is logged as DUPLICATE, skipped and ACKed.
  7. Replays evt-0001/evt-0002 (already processed) - they must be detected as
     duplicates, proving the store is persistent and shared by both workers.
  8. Parses the worker logs and the store file and verifies the outcome.

Usage:
    docker compose up -d
    python demo.py
Logs are written to ./logs/. Exit code 0 means every check passed.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
import threading
import time
from collections import Counter, defaultdict
from pathlib import Path

from common import QUEUE_NAME, connect, declare_queue, setup_logging

ROOT = Path(__file__).resolve().parent
LOG_DIR = ROOT / "logs"
STATE_DIR = ROOT / "state"
STORE_FILE = STATE_DIR / "processed_events.json"
FAIL_EVENT = "evt-fail"
DUPLICATE_EVENT = "evt-duplicate"
REPLAYED = ["evt-0001", "evt-0002"]
LINE_RE = re.compile(r"^(\S+) .*worker=(\S+) status=(\S+)\s+event_id=(\S+) order_id=(\S+) redelivered=(True|False)")

log = setup_logging("Demo")


class ManagedProcess:
    """A child process whose output is echoed to the console and saved to a log file."""

    def __init__(self, label: str, args: list[str]):
        self.label = label
        self.log_path = LOG_DIR / f"{label.lower()}.log"
        self.lines: list[str] = []
        self._log_file = self.log_path.open("a")
        self.proc = subprocess.Popen(
            [sys.executable, "-u", *args], cwd=ROOT,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1,
        )
        self._reader = threading.Thread(target=self._pump, daemon=True)
        self._reader.start()

    def _pump(self) -> None:
        for line in self.proc.stdout:
            line = line.rstrip("\n")
            self.lines.append(line)
            self._log_file.write(line + "\n")
            self._log_file.flush()
            print(f"  [{self.label:<8}] {line}", flush=True)

    def wait_for(self, text: str, timeout: float) -> bool:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if any(text in l for l in self.lines):
                return True
            if self.proc.poll() is not None:
                self._reader.join(timeout=1)
                return any(text in l for l in self.lines)
            time.sleep(0.1)
        return False

    def stop(self) -> None:
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
        self._reader.join(timeout=2)
        self._log_file.close()


def purge_queue() -> None:
    connection = connect(log)
    channel = connection.channel()
    declare_queue(channel)
    purged = channel.queue_purge(QUEUE_NAME).method.message_count
    log.info("Purged %d leftover message(s) from '%s'", purged, QUEUE_NAME)
    connection.close()


def parse_events(log_files: list[Path]) -> list[tuple[str, str, str, bool]]:
    events = []
    for path in log_files:
        for line in path.read_text().splitlines():
            m = LINE_RE.search(line)
            if m:
                timestamp, worker, status, event_id, _order_id, redelivered = m.groups()
                events.append((timestamp, worker, status, event_id, redelivered == "True"))
    # Chronological order across both workers' logs (timestamps are HH:MM:SS.mmm).
    return [e[1:] for e in sorted(events)]


def status_counts(workers: list[ManagedProcess], status: str) -> Counter:
    counts = Counter()
    for w in workers:
        for line in list(w.lines):
            m = LINE_RE.search(line)
            if m and m.group(3) == status:
                counts[m.group(4)] += 1
    return counts


def wait_for_statuses(workers: list[ManagedProcess], completed: set[str], duplicates: Counter,
                      timeout: float) -> bool:
    """Wait until every id in `completed` is COMPLETED and each id in `duplicates`
    has been logged as DUPLICATE at least the given number of times."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        done = status_counts(workers, "COMPLETED")
        dups = status_counts(workers, "DUPLICATE")
        if completed <= set(done) and all(dups[e] >= n for e, n in duplicates.items()):
            return True
        time.sleep(0.2)
    return False


def run_producer(args: list[str]) -> None:
    producer = ManagedProcess("Producer", ["producer.py", *args])
    producer.proc.wait()
    producer.stop()


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the full RabbitMQ order-processing demo.")
    parser.add_argument("--count", type=int, default=10, help="regular orders in the first batch (default 10)")
    parser.add_argument("--second-batch", type=int, default=4, help="orders published after recovery (default 4)")
    parser.add_argument("--work-seconds", type=float, default=0.5, help="simulated processing time (default 0.5)")
    args = parser.parse_args()

    LOG_DIR.mkdir(exist_ok=True)
    for old in LOG_DIR.glob("*.log"):
        old.unlink()

    log.info("Step 1: purge queue and reset the processed-events store for a reproducible run")
    purge_queue()
    shutil.rmtree(STATE_DIR, ignore_errors=True)

    log.info("Step 2: start two independent worker processes")
    def worker_args(name: str) -> list[str]:
        return ["worker.py", "--name", name, "--fail-on-event", FAIL_EVENT,
                "--work-seconds", str(args.work_seconds), "--state-dir", str(STATE_DIR.relative_to(ROOT))]

    workers = {name: ManagedProcess(name, worker_args(name)) for name in ("Worker-1", "Worker-2")}
    all_procs: list[ManagedProcess] = list(workers.values())
    try:
        for w in workers.values():
            if not w.wait_for("status=READY", timeout=60):
                log.error("%s did not become ready", w.label)
                return 1

        log.info("Step 3: publish %d orders plus '%s'", args.count, FAIL_EVENT)
        run_producer(["--count", str(args.count), "--fail-event", FAIL_EVENT])

        log.info("Step 4: wait for a worker to crash on '%s' and for redelivery", FAIL_EVENT)
        batch1 = {f"evt-{i:04d}" for i in range(1, args.count + 1)} | {FAIL_EVENT}
        crashed = None
        deadline = time.time() + 60
        while crashed is None and time.time() < deadline:
            for name, w in workers.items():
                if w.proc.poll() is not None:
                    crashed = name
            time.sleep(0.1)
        if crashed is None:
            log.error("No worker crashed - is '%s' being published?", FAIL_EVENT)
            return 1
        log.info("%s exited with code %s (no ACK sent)", crashed, workers[crashed].proc.returncode)

        if not wait_for_statuses(all_procs, batch1, Counter(), timeout=120):
            log.error("Batch 1 did not complete in time")
            return 1
        log.info("Batch 1 fully processed by the surviving worker(s) - the system kept running")

        log.info("Step 5: restart %s and publish %d new orders", crashed, args.second_batch)
        workers[crashed].stop()
        restarted = ManagedProcess(crashed, worker_args(crashed))   # appends to the same log file
        all_procs.append(restarted)
        workers[crashed] = restarted
        restarted.wait_for("status=READY", timeout=60)

        start = args.count + 1
        run_producer(["--count", str(args.second_batch), "--start", str(start)])
        batch2 = {f"evt-{i:04d}" for i in range(start, start + args.second_batch)}
        if not wait_for_statuses(all_procs, batch2, Counter(), timeout=120):
            log.error("Batch 2 did not complete in time")
            return 1

        log.info("Step 6: publish 2 identical copies of '%s' while both workers are idle", DUPLICATE_EVENT)
        run_producer(["--count", "0", "--duplicate-event", DUPLICATE_EVENT, "--duplicate-copies", "2"])
        if not wait_for_statuses(all_procs, {DUPLICATE_EVENT}, Counter({DUPLICATE_EVENT: 1}), timeout=60):
            log.error("Duplicate pair was not handled in time")
            return 1

        log.info("Step 7: replay already-processed %s (same event_id and payload)", ", ".join(REPLAYED))
        run_producer(["--count", str(len(REPLAYED)), "--start", "1"])
        if not wait_for_statuses(all_procs, set(), Counter({e: 1 for e in REPLAYED}), timeout=60):
            log.error("Replayed events were not handled in time")
            return 1
    finally:
        for w in all_procs:
            w.stop()

    return verify(batch1 | batch2 | {DUPLICATE_EVENT}, crashed)


def verify(expected: set[str], crashed: str) -> int:
    events = parse_events([LOG_DIR / "worker-1.log", LOG_DIR / "worker-2.log"])
    completed_by = defaultdict(list)
    duplicate_by = defaultdict(list)
    received = defaultdict(list)
    for worker, status, event_id, redelivered in events:
        if status == "COMPLETED":
            completed_by[event_id].append(worker)
        elif status == "DUPLICATE":
            duplicate_by[event_id].append(worker)
        elif status == "RECEIVED":
            received[event_id].append((worker, redelivered))

    print("\n" + "=" * 96)
    print(f"{'event_id':<14} {'received by (R = redelivered)':<42} {'executed by':<12} duplicate skipped by")
    print("-" * 96)
    for event_id in sorted(expected):
        rec = ", ".join(f"{w}({'R' if r else '-'})" for w, r in received[event_id])
        print(f"{event_id:<14} {rec:<42} {', '.join(completed_by[event_id]):<12} "
              f"{', '.join(duplicate_by[event_id]) or '-'}")
    print("=" * 96)

    store = json.loads(STORE_FILE.read_text()) if STORE_FILE.exists() else {}

    checks = []
    checks.append(("business action executed exactly once per event_id",
                   all(len(completed_by[e]) == 1 for e in expected)))
    checks.append(("no unexpected events completed", set(completed_by) <= expected))
    per_worker = Counter(w for ws in completed_by.values() for w in ws)
    checks.append((f"both workers processed orders {dict(per_worker)}", len(per_worker) == 2))
    fail_rx = received[FAIL_EVENT]
    checks.append((f"{FAIL_EVENT} first received by {crashed} (redelivered=False)",
                   (crashed, False) in fail_rx))
    checks.append((f"{FAIL_EVENT} redelivered to the other worker (redelivered=True)",
                   any(w != crashed and r for w, r in fail_rx)))
    checks.append((f"{FAIL_EVENT} completed by the surviving worker",
                   completed_by[FAIL_EVENT] and completed_by[FAIL_EVENT][0] != crashed))
    checks.append((f"{DUPLICATE_EVENT}: 2 copies received, executed once, skipped once",
                   len(received[DUPLICATE_EVENT]) == 2 and len(completed_by[DUPLICATE_EVENT]) == 1
                   and len(duplicate_by[DUPLICATE_EVENT]) == 1))
    dup_workers = {w for w, _ in received[DUPLICATE_EVENT]}
    print(f"  [INFO] {DUPLICATE_EVENT} copies were delivered to: {', '.join(sorted(dup_workers))}"
          + (" (cross-worker duplicate)" if len(dup_workers) == 2 else ""))
    checks.append((f"replayed {', '.join(REPLAYED)} after restart detected as DUPLICATE (not re-executed)",
                   all(len(duplicate_by[e]) == 1 and len(completed_by[e]) == 1 for e in REPLAYED)))
    checks.append((f"{STORE_FILE.relative_to(ROOT)} records all {len(expected)} processed event_ids",
                   set(store) == expected))

    ok = True
    for label, passed in checks:
        ok &= bool(passed)
        print(f"  [{'PASS' if passed else 'FAIL'}] {label}")
    print(f"\nResult: {'ALL CHECKS PASSED' if ok else 'SOME CHECKS FAILED'}  (logs in {LOG_DIR.relative_to(ROOT)}/)")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
