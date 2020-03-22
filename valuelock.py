import threading
import time


class ThreadValueLock:
    def __init__(self, maxValue: int, checkInterval: float = 0.02):
        self.maxValue = maxValue
        self.checkInterval = checkInterval

        self.value = 0
        self.lock = threading.Lock()

    def acquire(self, value: int) -> int:
        while True:
            # Get exclusive right to check on the value
            with self.lock:
                # If has sufficient value, acquire it
                if self.value + value < self.maxValue:
                    self.value += value
                    return self.value

            # Otherwise wait for an interval before checking again
            # This is placed outside of "with" to not blocking other threads from checking the value
            time.sleep(self.checkInterval)

    def release(self, value: int) -> int:
        with self.lock:
            # Release value
            self.value -= value
            return self.value

    def getValue(self) -> int:
        with self.lock:
            return self.value
