"""File-based idempotency store shared by all worker processes on one machine.

Layout (default directory ./state, override with --state-dir or STATE_DIR):

    state/processed_events.json   {event_id: {"worker": ..., "processed_at": ..., "result": ...}}
    state/processed_events.lock   guards reads/writes of the JSON file
    state/locks/<event_id>.lock   held while one worker is processing that event

Why two kinds of lock:
  * The store lock makes every read-modify-write of the JSON file atomic across
    processes; the file itself is replaced atomically (write temp file + os.replace),
    so a crash can never leave it half-written.
  * The per-event lock closes the check-then-act race: if two copies of the same
    event reach Worker-1 and Worker-2 at the same moment, the second worker blocks
    until the first has finished and recorded the event, then sees it as a duplicate.
    Different events use different lock files, so the workers still run in parallel.

Both are OS advisory locks (flock / msvcrt), which the OS releases automatically if
the process dies, so a crashed worker never leaves a stale lock behind.
"""

from __future__ import annotations

import json
import os
import re
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

if os.name == "nt":  # pragma: no cover - Windows fallback
    import msvcrt

    def _lock(fd: int) -> None:
        os.lseek(fd, 0, os.SEEK_SET)
        msvcrt.locking(fd, msvcrt.LK_LOCK, 1)

    def _unlock(fd: int) -> None:
        os.lseek(fd, 0, os.SEEK_SET)
        msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
else:
    import fcntl

    def _lock(fd: int) -> None:
        fcntl.flock(fd, fcntl.LOCK_EX)

    def _unlock(fd: int) -> None:
        fcntl.flock(fd, fcntl.LOCK_UN)


@contextmanager
def _file_lock(path: Path):
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
    try:
        _lock(fd)
        try:
            yield
        finally:
            _unlock(fd)
    finally:
        os.close(fd)


def _safe_name(event_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]", "_", event_id)


class ProcessedEventStore:
    def __init__(self, state_dir: str | os.PathLike):
        self.dir = Path(state_dir)
        self.lock_dir = self.dir / "locks"
        self.lock_dir.mkdir(parents=True, exist_ok=True)
        self.path = self.dir / "processed_events.json"
        self._store_lock = self.dir / "processed_events.lock"
        self._cache: set[str] = set()

    def _read(self) -> dict:
        try:
            return json.loads(self.path.read_text())
        except FileNotFoundError:
            return {}

    def load(self) -> int:
        """Load all processed event IDs (called at worker start-up). Returns the count."""
        with _file_lock(self._store_lock):
            self._cache = set(self._read())
        return len(self._cache)

    def is_processed(self, event_id: str) -> bool:
        """True if any worker has already recorded this event.

        The local cache is only a fast path; on a miss the shared file is re-read,
        because the other worker may have processed the event since we loaded it.
        """
        if event_id in self._cache:
            return True
        with _file_lock(self._store_lock):
            self._cache = set(self._read())
        return event_id in self._cache

    def get(self, event_id: str) -> dict | None:
        with _file_lock(self._store_lock):
            return self._read().get(event_id)

    def mark_processed(self, event_id: str, worker: str, result: str) -> None:
        with _file_lock(self._store_lock):
            data = self._read()
            data[event_id] = {
                "worker": worker,
                "processed_at": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
                "result": result,
            }
            tmp = self.path.with_suffix(f".{os.getpid()}.tmp")
            tmp.write_text(json.dumps(data, indent=2, sort_keys=True))
            os.replace(tmp, self.path)          # atomic swap: readers never see a partial file
            self._cache = set(data)

    @contextmanager
    def event_lock(self, event_id: str):
        """Hold an exclusive cross-process lock for one event_id."""
        with _file_lock(self.lock_dir / f"{_safe_name(event_id)}.lock"):
            yield
