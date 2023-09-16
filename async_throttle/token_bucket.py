import asyncio
import time
from typing import Optional


class TokenBucket:
    """Rate-limiting async token bucket."""

    def __init__(self,
                 tokens: float,
                 capacity: float,
                 period: float = 1.0,
                 interval: float = 0.0):
        """Create token bucket replenished at `tokens/period` up to `capacity`.

        The `interval` argument is how frequently the bucket will be checked
        for replenishment. This is effectively just a convenience in case you
        want to keep the units of `period` and `interval` intuitive, but also
        want the bucket to be continuously replenished. So, if you have a rate
        of 10 tokens per minute, by default 10 tokens will be added only one
        time every minute. If you specify an interval of 1 second, then 1 token
        will be added every 6 seconds.

        Args:
            tokens - Number of tokens to replenish
            capacity - Max number of tokens
            period - Time in seconds to add `tokens`
            interval - Time in seconds to check for replenishment
        """
        if tokens <= 0:
            raise ValueError(f"Rate must be positive, got {tokens}")
        if capacity <= 0:
            raise ValueError(f"Capacity must be positive, got {capacity}")
        if period <= 0:
            raise ValueError(f"Period must be positive, got {period}")
        if interval < 0:
            raise ValueError(f"Interval must be non-negative, got {interval}")
        # Current number of tokens
        self.level = capacity
        # Max number of tokens
        self.capacity = capacity
        # Token replenish rate
        self.rate = tokens / period
        self.interval = interval or period

        # Timestamp of last token addition
        self._last_add = time.monotonic()
        # Synchronization for token requesters
        self._cv = asyncio.Condition()
        # Scheduled replenishment task.
        self._timer: Optional[asyncio.Task] = None

    async def consume(self, n: float):
        """Take `n` tokens from the bucket.

        Blocks until tokens are available.

        Args:
            n - Number of tokens (can be fractional)
        """
        if n <= 0:
            return
        if n > self.capacity:
            raise ValueError(f"Requested {n} tokens, but capacity is {self.capacity}")
        while True:
            async with self._cv:
                # Ensure bucket is topped off
                self._fill()
                # Check if tokens are available
                if n > self.level:
                    # Make sure a replenishment task is scheduled
                    self._schedule()
                    # Block indefinitely until woken by a replenishment task
                    await self._cv.wait()
                    continue
                else:
                    self.level -= n
                    return

    def _schedule(self):
        """Ensure that a replenishment task is scheduled.

        If one is scheduled already, this is a no-op.
        """
        if self._timer:
            return
        t_since_last = time.monotonic() - self._last_add
        next_add = max(0, self.interval - t_since_last)
        self._timer = asyncio.create_task(self._fill_after(next_add))

    async def _fill_after(self, timeout: float):
        """Replenish the token bucket after napping for an interval.

        Args:
            timeout - Time in seconds to sleep before replenishing
        """
        await asyncio.sleep(timeout)
        async with self._cv:
            # Fill up the bucket now
            self._fill()
            # Wake up a few sleeping tasks. Note that the token level is often
            # fractional, so we round it. We also add an extra sleeping
            # coroutine to wake. This guarantees that if there are more tasks
            # sleeping than we have tokens for, the last awakened task will
            # call `_schedule` again to replenish another time. That way we
            # don't have to explicitly keep track of exactly who's waiting.
            #
            # Lastly, a task might request multiple tokens. That just means
            # that we are waking even more tasks than we need to. But
            # ultimately they will just go back to sleep and try again on the
            # next replenishment.
            self._cv.notify(n=int(self.level) + 1)
            self._timer = None

    def _fill(self):
        """Add new tokens to the bucket.

        Tokens are often fractional, as they are prorated based on the last
        replenishment. Eventually the tokens are limited by the capacity.
        """
        t = time.monotonic()
        delta = t - self._last_add
        self.level = min(self.capacity, self.level + delta * self.rate)
        self._last_add = t
