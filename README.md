Async Throttle
===

Multipurpose concurrency primitive for `asyncio` coroutines.

## Features

This throttle is configured with two related, but different parameters:

```py
Throttle(rate: float, concurrency: int)
```

`rate` - The **rate limit** (in operations-per-second) for tasks.

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

async with throttle:
    # Do some (async) thing
```

Like other `asyncio` primitives, `Throttle` is not thread-safe.
