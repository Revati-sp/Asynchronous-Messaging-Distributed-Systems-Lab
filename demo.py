"""End-to-end, reproducible demonstration of competing consumers and redelivery.

What it does (each component runs as its own OS process):
  1. Purges the order queue so the run starts clean.
  2. Starts Worker-1 and Worker-2, both with --fail-on-event evt-fail.
  3. Runs the producer: 10 orders + evt-fail.
  4. Whichever worker receives evt-fail first crashes BEFORE acking; RabbitMQ
     redelivers it to the surviving worker, which completes it.
  5. Restarts the crashed worker (same name) and publishes a second batch to show
     the system keeps running and the restarted worker rejoins.
  6. Parses the worker logs and verifies:
       - every event was COMPLETED exactly once (no duplicates, nothing lost),
       - both workers did work (competing consumers),
       - evt-fail was received twice, by two different workers, the second time
         with redelivered=True, and was completed by the worker that did NOT crash.

Usage:
    docker compose up -d
    python demo.py
Logs are written to ./logs/. Exit code 0 means every check passed.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import threading
import time
from collections import defaultdict
from pathlib import Path

from common import QUEUE_NAME, connect, declare_queue, setup_logging

ROOT = Path(__file__).resolve().parent
LOG_DIR = ROOT / "logs"
FAIL_EVENT = "evt-fail"
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


def completed_ids(workers: list[ManagedProcess]) -> set[str]:
    done = set()
    for w in workers:
        for line in list(w.lines):
            m = LINE_RE.search(line)
            if m and m.group(3) == "COMPLETED":
                done.add(m.group(4))
    return done


def wait_until_completed(expected: set[str], workers: list[ManagedProcess], timeout: float) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if expected <= completed_ids(workers):
            return True
        time.sleep(0.2)
    return False


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the full RabbitMQ order-processing demo.")
    parser.add_argument("--count", type=int, default=10, help="regular orders in the first batch (default 10)")
    parser.add_argument("--second-batch", type=int, default=4, help="orders published after recovery (default 4)")
    parser.add_argument("--work-seconds", type=float, default=0.5, help="simulated processing time (default 0.5)")
    args = parser.parse_args()

    LOG_DIR.mkdir(exist_ok=True)
    for old in LOG_DIR.glob("*.log"):
        old.unlink()

    log.info("Step 1: purge queue for a clean, reproducible run")
    purge_queue()

    log.info("Step 2: start two independent worker processes")
    def worker_args(name: str) -> list[str]:
        return ["worker.py", "--name", name, "--fail-on-event", FAIL_EVENT,
                "--work-seconds", str(args.work_seconds)]

    workers = {name: ManagedProcess(name, worker_args(name)) for name in ("Worker-1", "Worker-2")}
    all_procs: list[ManagedProcess] = list(workers.values())
    try:
        for w in workers.values():
            if not w.wait_for("status=READY", timeout=60):
                log.error("%s did not become ready", w.label)
                return 1

        log.info("Step 3: publish %d orders plus '%s'", args.count, FAIL_EVENT)
        producer = ManagedProcess("Producer", ["producer.py", "--count", str(args.count), "--fail-event", FAIL_EVENT])
        producer.proc.wait()
        producer.stop()

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

        if not wait_until_completed(batch1, all_procs, timeout=120):
            log.error("Batch 1 did not complete in time")
            return 1
        log.info("Batch 1 fully processed by the surviving worker(s) - the system kept running")

        log.info("Step 5: restart %s and publish %d more orders", crashed, args.second_batch)
        workers[crashed].stop()
        restarted = ManagedProcess(crashed, worker_args(crashed))   # appends to the same log file
        all_procs.append(restarted)
        workers[crashed] = restarted
        restarted.wait_for("status=READY", timeout=60)

        start = args.count + 1
        producer2 = ManagedProcess("Producer", ["producer.py", "--count", str(args.second_batch), "--start", str(start)])
        producer2.proc.wait()
        producer2.stop()
        batch2 = {f"evt-{i:04d}" for i in range(start, start + args.second_batch)}
        if not wait_until_completed(batch2, all_procs, timeout=120):
            log.error("Batch 2 did not complete in time")
            return 1
    finally:
        for w in all_procs:
            w.stop()

    return verify(batch1 | batch2, crashed)


def verify(expected: set[str], crashed: str) -> int:
    events = parse_events([LOG_DIR / "worker-1.log", LOG_DIR / "worker-2.log"])
    completed_by = defaultdict(list)
    received = defaultdict(list)
    for worker, status, event_id, redelivered in events:
        if status == "COMPLETED":
            completed_by[event_id].append(worker)
        elif status == "RECEIVED":
            received[event_id].append((worker, redelivered))

    print("\n" + "=" * 72)
    print(f"{'event_id':<10} {'received by (redelivered?)':<42} completed by")
    print("-" * 72)
    for event_id in sorted(expected):
        rec = ", ".join(f"{w}({'R' if r else '-'})" for w, r in received[event_id])
        print(f"{event_id:<10} {rec:<42} {', '.join(completed_by[event_id])}")
    print("=" * 72)

    checks = []
    checks.append(("every event completed exactly once",
                   all(len(completed_by[e]) == 1 for e in expected)))
    checks.append(("no unexpected events completed", set(completed_by) <= expected))
    per_worker = defaultdict(int)
    for workers in completed_by.values():
        for w in workers:
            per_worker[w] += 1
    checks.append((f"both workers processed orders {dict(per_worker)}", len(per_worker) == 2))
    fail_rx = received[FAIL_EVENT]
    checks.append((f"{FAIL_EVENT} first received by {crashed} (redelivered=False)",
                   (crashed, False) in fail_rx))
    checks.append((f"{FAIL_EVENT} redelivered to the other worker (redelivered=True)",
                   any(w != crashed and r for w, r in fail_rx)))
    checks.append((f"{FAIL_EVENT} completed by the surviving worker",
                   completed_by[FAIL_EVENT] and completed_by[FAIL_EVENT][0] != crashed))

    ok = True
    for label, passed in checks:
        ok &= bool(passed)
        print(f"  [{'PASS' if passed else 'FAIL'}] {label}")
    print(f"\nResult: {'ALL CHECKS PASSED' if ok else 'SOME CHECKS FAILED'}  (logs in {LOG_DIR})")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
