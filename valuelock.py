import threading
import time


class ThreadValueLock:
    def __init__(self, maxValue: int, checkInterval: float = 0.02):
        self.maxValue = maxValue
        self.checkInterval = checkInterval

        self.value = 0
        self.lock = threading.Lock()

    def acquire(self, value) -> int:
        while True:
            # Try to acquire
            with self.lock:
                # If has sufficient value, acquire it
                if self.value + value < self.maxValue:
                    self.value += value
                    return self.value

            # Otherwise wait for an interval before try again
            time.sleep(self.checkInterval)

    def release(self, value) -> int:
        with self.lock:
            # Release value
            self.value -= value
            return self.value

    def getValue(self) -> int:
        with self.lock:
            return self.value
