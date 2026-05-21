import asyncio
from collections.abc import Generator

import pytest


@pytest.fixture(scope="session", autouse=True)
def baseline_event_loop() -> Generator[None]:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        yield
    finally:
        asyncio.set_event_loop(None)
        loop.close()
