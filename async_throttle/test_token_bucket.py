import asyncio
import time

from .token_bucket import TokenBucket


async def test_simple_bucket():
    bucket = TokenBucket(1, 20)

    assert bucket.level == 20

    await bucket.consume(1)
    assert bucket.level == 19
    await bucket.consume(10)
    assert bucket.level == 9

    await asyncio.sleep(1)

    assert round(bucket.level, 1) == 10.

    await asyncio.sleep(2)
    assert round(bucket.level, 1) == 12

    t0 = time.monotonic()
    await bucket.consume(14)
    t1 = time.monotonic()
    assert round(t1 - t0, 1) == 2
    assert round(bucket.level, 1) == 0


async def test_bucket_with_period():
    bucket = TokenBucket(2, 10, period=3)

    assert bucket.level == 10
    
    await bucket.consume(10)
    assert bucket.level == 0

    t0 = time.monotonic()
    await bucket.consume(4)
    t1 = time.monotonic()
    assert round(t1 - t0, 1) == 6
    assert round(bucket.level, 1) == 0

    # By default the interval is the same as the period
    await asyncio.sleep(2)
    assert round(bucket.level, 1) == 0
    await asyncio.sleep(1)
    assert round(bucket.level, 1) == 2


async def test_bucket_with_interval():
    bucket = TokenBucket(1, 10, period=4, interval=2)

    assert bucket.level == 10
    await bucket.consume(10)
    assert bucket.level == 0

    await asyncio.sleep(1)
    assert round(bucket.level, 1) == 0
    await asyncio.sleep(1)
    assert round(bucket.level, 1) == 0.5
    await asyncio.sleep(1)
    assert round(bucket.level, 1) == 0.5
    await asyncio.sleep(1)
    assert round(bucket.level, 1) == 1
