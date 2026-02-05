import threading
import multiprocessing


class Buffer:
    lock = None
    buffer = None

    def __init__(self, manager: multiprocessing.Manager):
        self.buffer = manager.list()
        self.lock = manager.Lock()

    def push_sample(self, ts: float, avg: float, min: float, max: float):
        with self.lock:
            self.buffer.append({"ts": ts, "avg": avg, "min": min, "max": max})

    def snapshot_and_clear(self):
        """Atomically snapshot buffer for Prefect processing."""
        with self.lock:
            snapshot = list(self.buffer)
            self.buffer[:] = []
        return snapshot
