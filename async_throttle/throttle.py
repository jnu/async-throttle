import asyncio
import time
from contextlib import asynccontextmanager

from .token_bucket import TokenBucket


class Throttle:
    """A limiter for both rate and concurrency.

    Rate limiting uses a token bucket to ensure no more than X operations
    happen per second. Additionally, concurrency is limited so that only Y
    operations can happen concurrently.
    """

    def __init__(self,
                 capacity: float,
                 concurrency: int = 0,
                 period: float = 1.0,
                 ):
        """Create a limiter for frequency and concurrency.

        Allows for `capacity` requests per `period` seconds, and no more than
        `concurrency` requests at a time.

        Args:
            capacity - Max number of tokens
            concurrency - Number of concurrent ops (0 for no limit)
            period - Time in seconds to add `tokens`
        """
        if concurrency < 0:
            raise ValueError(f"Concurrency must be non-negative, got {concurrency}")
        self.unlimited = concurrency == 0
        self.sem = asyncio.Semaphore(concurrency)
        self.bucket = TokenBucket(capacity, capacity, period=period)
        self.sleep_lock = asyncio.Lock()
        self.sleep_until = 0.0

    @asynccontextmanager
    async def __call__(self, n: float = 1.0):
        """Alias for `acquire`."""
        await self.acquire(n)
        yield
        self.release()

    async def acquire(self, n: float = 1.0):
        """Wait until throttle has capacity for this request.

        Args:
            n - Number of tokens (can be fractional)
        """
        if not self.unlimited:
            await self.sem.acquire()

        t_now = time.monotonic()
        while t_now < self.sleep_until:
            await asyncio.sleep(max(0, self.sleep_until - t_now))
            # In theory we might have been asked to sleep again while we were
            # sleeping here, so check again.
            t_now = time.monotonic()
        await self.bucket.consume(n)

    def release(self):
        """Release the lock."""
        if not self.unlimited:
            self.sem.release()

    async def pause(self, td: float):
        """Tell the throttle to stop for the given number of seconds.

        If the throttle is already sleeping and the new `td` is less than the
        remaining sleep time, it is a no-op; otherwise the deadline is extended
        only by the marginal difference. In other words, sleep times are
        absolute and not additive.

        This method does **not** block for the extent of the sleep; instead,
        the sleep is applied on the coroutines that have acquired the
        semaphore (thus blocking every other coroutine as well).

        Args:
            td - Time delta to pause the throttle for, in seconds
        """
        async with self.sleep_lock:
            t_now = time.monotonic()
            t_until = t_now + td
            if t_until > self.sleep_until:
                self.sleep_until = t_until

    async def __aenter__(self):
        """Enter async context."""
        await self.acquire()

    async def __aexit__(self, exc_t, exc_v, exc_tb):
        """Exit the async context."""
        self.release()

    @property
    def capacity(self) -> float:
        """Return the current capacity."""
        return self.bucket.capacity

    @property
    def level(self) -> float:
        """Return the current level."""
        return self.bucket.level
