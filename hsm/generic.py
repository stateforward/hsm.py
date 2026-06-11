import collections
import typing
import concurrent.futures
import asyncio
import threading

TReturn = typing.TypeVar("TReturn")
TItem = typing.TypeVar("TItem")
TKey = typing.TypeVar("TKey")
TValue = typing.TypeVar("TValue")

QueuePushResult = tuple[BaseException | None]
type QueuePopResult[TItem] = tuple[TItem, bool, BaseException | None]
QueueLenResult = tuple[int, BaseException | None]


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

    async def wait(self) -> TReturn:
        if self._future.done():
            return self._future.result()
        loop = asyncio.get_running_loop()
        waiter: asyncio.Future[TReturn] = loop.create_future()

        def wake(source: concurrent.futures.Future[TReturn]) -> None:
            def complete() -> None:
                if not waiter.done():
                    try:
                        waiter.set_result(source.result())
                    except BaseException as error:
                        waiter.set_exception(error)

            try:
                _ = loop.call_soon_threadsafe(complete)
            except RuntimeError:
                pass

        self._future.add_done_callback(wake)
        return await waiter

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

    def push(self, item: TItem) -> QueuePushResult:
        self._items.append(item)
        return (None,)

    def pop(self) -> QueuePopResult[TItem]:
        if not self._items:
            return typing.cast(TItem, None), False, None
        return self._items.popleft(), True, None

    def len(self) -> QueueLenResult:
        return len(self._items), None

    def clear(self) -> None:
        self._items.clear()


@typing.final
class Map(typing.Generic[TKey, TValue]):
    """Thread-safe map with Go sync.Map-style tuple results."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._items: dict[TKey, TValue] = {}

    def load(self, key: TKey) -> tuple[TValue, bool]:
        with self._lock:
            if key not in self._items:
                return typing.cast(TValue, None), False
            return self._items[key], True

    def store(self, key: TKey, value: TValue) -> None:
        with self._lock:
            self._items[key] = value

    def swap(self, key: TKey, value: TValue) -> tuple[TValue, bool]:
        with self._lock:
            if key in self._items:
                old_value, exists = self._items[key], True
            else:
                old_value, exists = typing.cast(TValue, None), False
            self._items[key] = value
            return old_value, exists

    def delete(self, key: TKey) -> None:
        with self._lock:
            self._items.pop(key, None)

    def clear(self) -> None:
        with self._lock:
            self._items.clear()

    def items(self) -> tuple[tuple[TKey, TValue], ...]:
        with self._lock:
            return tuple(self._items.items())
