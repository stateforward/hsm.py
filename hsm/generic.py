import collections
import typing
import concurrent.futures
import asyncio

TReturn = typing.TypeVar("TReturn")
TItem = typing.TypeVar("TItem")


@typing.final
class Awaitable(typing.Generic[TReturn]):
    def __init__(self) -> None:
        self._future = concurrent.futures.Future[TReturn]()

    def done(self) -> bool:
        return self._future.done()

    def set_result(self, result: TReturn = None) -> None:
        try:
            self._future.set_result(result)
        except concurrent.futures.InvalidStateError:
            pass

    async def wait(self) -> None:
        if self._future.done():
            return
        loop = asyncio.get_running_loop()
        waiter: asyncio.Future[TReturn] = loop.create_future()

        def wake(source: concurrent.futures.Future[TReturn]) -> None:
            def complete() -> None:
                if not waiter.done():
                    waiter.set_result(source.result())

            try:
                _ = loop.call_soon_threadsafe(complete)
            except RuntimeError:
                pass

        self._future.add_done_callback(wake)
        await waiter

    def __await__(self):
        return self.wait().__await__()

    def exception(self) -> BaseException | None:
        return self._future.exception()

    def set_exception(self, exception: BaseException) -> None:
        self._future.set_exception(exception)

    def result(self) -> TReturn:
        return self._future.result()

    def cancel(self) -> bool:
        return self._future.cancel()

    def cancelled(self) -> bool:
        return self._future.cancelled()


class Queue(typing.Generic[TItem]):
    """Regular-event FIFO backend for Queue(fifo=...)."""

    def __init__(self) -> None:
        self._items: collections.deque[TItem] = collections.deque()

    def push(self, item: TItem) -> BaseException | None:
        self._items.append(item)
        return None

    def pop(self) -> tuple[TItem, bool, BaseException | None]:
        if not self._items:
            return typing.cast(TItem, None), False, None
        return self._items.popleft(), True, None

    def len(self) -> tuple[int, BaseException | None]:
        return len(self._items), None

    def clear(self) -> None:
        self._items.clear()
