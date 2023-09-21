import asyncio
import time

from .throttle import Throttle


async def test_throttle():
    throttle = Throttle(2)
    async with throttle:
        assert throttle.level == 1

    t0 = time.monotonic()
    async with throttle(2):
        t1 = time.monotonic()
        assert round(t1 - t0, 1) == 1.0
        assert throttle.level == 0


async def test_throttle_concurrency():
    throttle = Throttle(5, concurrency=2)

    ctr = 0

    async def task():
        nonlocal ctr
        async with throttle:
            ctr += 1
            await asyncio.sleep(1)

    tasks = [asyncio.create_task(task()) for _ in range(3)]
    await asyncio.sleep(0.1)
    assert throttle.level == 3
    assert ctr == 2
    await asyncio.sleep(1)
    assert throttle.level == 4
    assert ctr == 3
    await asyncio.gather(*tasks)
    assert throttle.level == 5
