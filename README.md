Async Throttle
===

Multipurpose concurrency primitive for `asyncio` coroutines.

## Features

This throttle is configured with two related, but different parameters:

```py
Throttle(capacity: float, concurrency: int = 0, period: float = 1.0)
```

`capacity` - Sets the **rate limit** for requests, as `capacity` per `period` seconds.

`concurrency` - The number of jobs that can be executing at a given time.

Usually, servers will set policies on both of these dimensions, and will suspend clients that violate either of them.

### `Throttle#pause(td: int)`

The `pause` method will lock the throttle for the given number of seconds.

For example, if an API bans your client from accessing resources due to violating their rate-limit, you can tell your code to sleep for a period of time and try again later.

## Usage

The throttle can be a drop-in replacement for another primitive like `asyncio.Semaphore`.
In fact, it's really just an `asyncio.Semaphore` (which handles the `concurrency` limit) mixed with a token bucket to provide rate limiting.

```py
throttle = Throttle(10, 2)  # Two concurrent coros limited to 10qps (total).

# Perform one task when the budget allows
async with throttle:
    # Do some (async) thing

# For tasks that should consume more of the budget, you can call the throttle:
async with throttle(5):
    # Do some more expensive thing, equivalent to 5 requests
```

Like other `asyncio` primitives, `Throttle` is not thread-safe.
