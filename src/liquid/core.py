from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import Executor, Future, as_completed
from functools import reduce
from itertools import chain
from typing import Any


class Flow[T]:
    def __init__(
        self,
        iterable: Iterable[T],
        /,
    ) -> None:
        self.iterable = iterable

    def map[R](
        self, fn: Callable[[T], R], /, recover: Callable[[T, Exception], Iterable[R]] | None = None
    ) -> "Flow[R]":
        """
        Applies a function to each element of the flow and returns a new flow with the results.

        This is very similar to the built-in map function, but it returns a Flow object for method chaining.

        >>> Flow.of(1, 2, 3).map(lambda x: x * 2).to_list()
        [2, 4, 6]

        You can pass a custom `recover` function to handle exceptions raised by the function.

        The `recover` function will be called with the element that caused the error and the exception itself.

        It should return an iterable of results, similar to `flatmap`.

        >>> from collections.abc import Iterator
        >>> def must_positive(x: int) -> int:
        ...     if x <= 0:
        ...         raise ValueError(f"{x} is not positive")
        ...     return x
        >>> Flow.of(-1, 2, 3).map(must_positive, lambda _, e: [0]).to_list()
        [0, 2, 3]

        Your error handler can be even more robust by handling different types of exceptions.

        By not yielding anything, you can skip the element that caused the error

        >>> from collections.abc import Iterator
        >>> class NegativeError(Exception):
        ...     ...

        >>> def must_positive_int(x) -> int:
        ...     if not isinstance(x, int):
        ...         raise TypeError(f"{x!r} is not an integer")
        ...     if x < 0:
        ...         raise NegativeError(f"{x} is negative")
        ...     return x

        >>> def recover(item, e: Exception) -> Iterator[int]:
        ...     if isinstance(e, NegativeError):
        ...         yield 0
        ...     else:
        ...         print(f"Unexpected error: {e} while processing {item!r}")
        >>> Flow.of(-1, 2, "3").map(must_positive_int, recover).to_list()
        Unexpected error: '3' is not an integer while processing '3'
        [0, 2]
        """
        if not recover:
            return Flow(map(fn, self))

        def wrapper(it: T) -> Iterator[R]:
            try:
                yield fn(it)
            except Exception as e:
                yield from recover(it, e)

        return self.flatmap(wrapper)

    def submit[R](self, executor: Executor, fn: Callable[[T], R], /) -> "Flow[Future[R]]":
        """
        Submit the function to each element of the flow using the executor.

        >>> from concurrent.futures import ThreadPoolExecutor
        >>> with ThreadPoolExecutor(max_workers=3) as executor:
        ...     flow = Flow.of(1, 2, 3).submit(executor, lambda x: x * 2).to_list()
        ...     for future in flow:
        ...         print(future.result())
        2
        4
        6
        """
        return Flow([executor.submit(fn, item) for item in self.iterable])

    def submit_map[R](
        self,
        executor: Executor,
        fn: Callable[[T], R],
        /,
        recover: Callable[[T, Exception], Iterable[R]] | None = None,
    ) -> "Flow[R]":
        """
        Equivalent to map, but uses an executor to submit the function with each element of the flow.

        >>> from concurrent.futures import ThreadPoolExecutor
        >>> with ThreadPoolExecutor(max_workers=3) as executor:
        ...     Flow.of(1, 2, 3).submit_map(executor, lambda x: x * 2).to_list()
        [2, 4, 6]

        You can pass a custom `recover` function to handle exceptions raised by the function.
        The `recover` function will be called with the element that caused the error and the exception itself.
        It should return an iterable of results, similar to `flatmap`.

        >>> def must_positive(x: int) -> int:
        ...     if x < 0:
        ...         raise ValueError(f"Expected positive number, got {x}")
        ...     return x
        >>> with ThreadPoolExecutor(max_workers=3) as executor:
        ...     Flow.of(-1, 2, 3).submit_map(executor, must_positive, lambda *_: [0]).to_list()
        [0, 2, 3]
        """
        if not recover:
            return Flow(executor.map(fn, self.iterable))

        def wrapper(it: T) -> Iterator[R]:
            try:
                yield fn(it)
            except Exception as e:
                yield from recover(it, e)

        return self.submit_flatmap(executor, wrapper)

    def submit_map_as_completed[R](
        self,
        executor: Executor,
        fn: Callable[[T], R],
        /,
        recover: Callable[[T, Exception], Iterable[R]] | None = None,
    ) -> "Flow[R]":
        """
        Submit the function to each element of the flow using the executor
        and return a flow of futures in the order they are completed.

        This is useful when you don't care about the order of the results.

        >>> from concurrent.futures import ThreadPoolExecutor
        >>> with ThreadPoolExecutor(max_workers=3) as executor:
        ...     (
        ...         Flow.of(1, 2, 3)
        ...         .submit_map_as_completed(executor, lambda x: x * 2)
        ...         .to_set()
        ...     )
        {2, 4, 6}
        """
        if not recover:
            return self.submit(executor, fn).through(as_completed).map(lambda f: f.result())

        def wrapper(it: T) -> Iterator[R]:
            try:
                yield fn(it)
            except Exception as e:
                yield from recover(it, e)

        return self.submit(executor, wrapper).through(as_completed).flatmap(lambda f: f.result())

    def flatmap[R](
        self,
        fn: Callable[[T], Iterable[R]],
        /,
        recover: Callable[[T, Exception], Iterable[R]] | None = None,
    ) -> "Flow[R]":
        """
        Equivalent to flatmap, but uses an executor to submit the function to each element of the flow.

        >>> Flow.of(1, 2, 3).flatmap(lambda x: (x, x * 2)).to_list()
        [1, 2, 2, 4, 3, 6]

        You can pass a custom `recover` function to handle exceptions raised by the function.
        The `recover` function will be called with the element that caused the error and the exception itself.
        It should return an iterable of results.

        >>> def must_positive(x: int) -> list[int]:
        ...     if x <= 0:
        ...         raise ValueError(f"{x} is not positive")
        ...     return [x, x * 2]
        >>> Flow.of(-1, 2, 3).flatmap(must_positive, lambda item, e: [0]).to_list()
        [0, 2, 4, 3, 6]
        """
        if not recover:
            return Flow(chain.from_iterable(map(fn, self.iterable)))

        def wrapper(it: T) -> Iterator[R]:
            try:
                yield from fn(it)
            except Exception as e:
                yield from recover(it, e)

        return self.flatmap(wrapper)

    def submit_flatmap[R](
        self,
        executor: Executor,
        fn: Callable[[T], Iterable[R]],
        /,
        recover: Callable[[T, Exception], Iterable[R]] | None = None,
    ) -> "Flow[R]":
        """
        Equivalent to flatmap, but uses an executor to submit the mapping tasks.

        >>> from concurrent.futures import ThreadPoolExecutor
        >>> with ThreadPoolExecutor(max_workers=3) as executor:
        ...     Flow.of(1, 2, 3).submit_flatmap(executor, lambda x: [x, x * 2]).to_list()
        [1, 2, 2, 4, 3, 6]

        You can pass a custom `recover` function to handle exceptions raised by the function.
        The `recover` function will be called with the element that caused the error and the exception itself.
        It should return an iterable of results.

        >>> def must_positive(x: int) -> list[int]:
        ...     if x <= 0:
        ...         raise ValueError(f"{x} is not positive")
        ...     return [x, x * 2]
        >>> with ThreadPoolExecutor(max_workers=3) as executor:
        ...     Flow.of(-1, 2, 3).submit_flatmap(executor, must_positive, lambda item, e: [0]).to_list()
        [0, 2, 4, 3, 6]
        """
        if not recover:
            return Flow(chain.from_iterable(executor.map(fn, self.iterable)))

        def wrapper(it: T) -> Iterator[R]:
            try:
                yield from fn(it)
            except Exception as e:
                yield from recover(it, e)

        return self.submit_flatmap(executor, wrapper)

    def submit_flatmap_as_completed[R](
        self,
        executor: Executor,
        fn: Callable[[T], Iterable[R]],
        /,
        recover: Callable[[T, Exception], Iterable[R]] | None = None,
    ) -> "Flow[R]":
        """
        Submit the function to each element of the flow using the executor
        and return a flow of futures in the order they are completed.

        This is useful when you don't care about the order of the results.

        >>> from concurrent.futures import ThreadPoolExecutor
        >>> with ThreadPoolExecutor(max_workers=3) as executor:
        ...     (
        ...         Flow.of(1, 2, 3)
        ...         .submit_flatmap_as_completed(executor, lambda x: [x, x * 2])
        ...         .to_set()
        ...     )
        {1, 2, 3, 4, 6}
        """
        if not recover:
            return self.submit(executor, fn).through(as_completed).flatmap(lambda f: f.result())

        def wrapper(it: T) -> Iterator[R]:
            try:
                yield from fn(it)
            except Exception as e:
                yield from recover(it, e)

        return self.submit(executor, wrapper).through(as_completed).flatmap(lambda f: f.result())

    def through[R](self, fn: Callable[[Iterable[T]], Iterator[R]], /) -> "Flow[R]":
        """
        Pass the flow to a function that takes an iterable and returns an iterable.
        This is useful for integrating with other libraries or custom processing.

        >>> from concurrent.futures import ThreadPoolExecutor, as_completed
        >>> with ThreadPoolExecutor(max_workers=3) as executor:
        ...     (
        ...         Flow.of(1, 2, 3)
        ...         .submit(executor, lambda x: x * 2)
        ...         .through(as_completed)
        ...         .map(lambda f: f.result())
        ...         .to_set()
        ...     )
        {2, 4, 6}
        """
        return Flow(fn(self.iterable))

    def filter(self, fn: Callable[[T], bool], /) -> "Flow[T]":
        """
        Filter elements in the flow based on the given predicate function.

        >>> Flow.of(1, 2, 3, 4, 5).filter(lambda x: x % 2 == 0).to_list()
        [2, 4]

        >>> Flow.of('apple', 'banana', 'cherry').filter(lambda s: 'a' in s).to_list()
        ['apple', 'banana']
        """
        return Flow(filter(fn, self.iterable))

    def zip[R](self, other: "Iterable[R]", /) -> "Flow[tuple[T, R]]":
        """
        Combine each element of the flow with the corresponding element from the other iterable.

        >>> Flow.of(1, 2, 3).zip(['a', 'b', 'c']).to_list()
        [(1, 'a'), (2, 'b'), (3, 'c')]

        >>> Flow.of(1, 2).zip(['a', 'b', 'c']).to_list()  # Stops at shortest iterable
        [(1, 'a'), (2, 'b')]
        """
        return Flow(zip(self.iterable, other))

    def combine[T2, R](self, other: "Iterable[T2]", fn: Callable[[T, T2], R], /) -> "Flow[R]":
        """
        Combine each element of the flow with the corresponding element from the other iterable
        using the provided function.

        >>> Flow.of(1, 2, 3).combine([10, 20, 30], lambda x, y: x + y).to_list()
        [11, 22, 33]

        >>> Flow.of('a', 'b', 'c').combine([1, 2, 3], lambda x, y: x * y).to_list()
        ['a', 'bb', 'ccc']
        """

        def create_iter() -> Iterator[R]:
            for it1, it2 in zip(self, other):
                yield fn(it1, it2)

        return Flow(create_iter())

    def fold[R](self, fn: Callable[[R, T], R], initial: R, /) -> R:
        """
        Apply a function of two arguments cumulatively to the items of the flow,
        from left to right, so as to reduce the flow to a single value.

        >>> Flow.of(1, 2, 3, 4).fold(lambda acc, x: acc + x, 0)
        10

        >>> Flow.of('a', 'b', 'c').fold(lambda acc, x: acc + x, '')
        'abc'
        """
        return reduce(fn, self, initial)

    def reduce(self, fn: Callable[[T, T], T], /) -> T:
        """
        Apply a function of two arguments cumulatively to the items of the flow,
        from left to right, so as to reduce the flow to a single value.

        Unlike fold, this method uses the first item of the flow as the initial value.

        >>> Flow.of(1, 2, 3, 4).reduce(lambda x, y: x + y)
        10

        >>> Flow.of('a', 'b', 'c').reduce(lambda x, y: x + y)
        'abc'
        """
        return reduce(fn, self)

    def take(self, n: int, /) -> "Flow[T]":
        """
        Take the first n items from the flow.

        >>> Flow.of(1, 2, 3, 4, 5).take(3).to_list()
        [1, 2, 3]

        >>> Flow.of(1, 2).take(5).to_list()  # Won't raise an error if not enough items
        [1, 2]
        """
        return Flow(it for _, it in zip(range(n), self))

    def drop(self, n: int, /) -> "Flow[T]":
        """
        Drop the first n items and return the rest of the flow.

        >>> Flow.of(1, 2, 3, 4, 5).drop(2).to_list()
        [3, 4, 5]

        >>> Flow.of(1, 2, 3).drop(5).to_list()  # Returns empty flow if more than available items
        []
        """

        def create_iter() -> Iterator[T]:
            iterator = iter(self)
            for _ in range(n):
                try:
                    next(iterator)
                except StopIteration:
                    break
            yield from iterator

        return Flow(create_iter())

    def on_each(self, fn: Callable[[T], Any], /) -> "Flow[T]":
        """
        Apply a function to each element in the flow and then return the element unchanged.

        This is useful for side effects like logging while preserving the flow.

        >>> Flow.of(1, 2, 3).on_each(print).map(lambda x: x * 2).to_list()
        1
        2
        3
        [2, 4, 6]
        """

        def create_iter() -> Iterator[T]:
            for it in self:
                fn(it)
                yield it

        return Flow(create_iter())

    def collect(self, fn: Callable[[T], Any], /) -> None:
        """
        Collect all elements with a given function, returning None

        >>> Flow.of(1, 2, 3).collect(print)
        1
        2
        3
        """
        for it in self:
            fn(it)

    def to_list(self) -> list[T]:
        """
        Collect all elements into a list

        >>> Flow.of(1, 2, 3).to_list()
        [1, 2, 3]

        This is also equivalent to calling `list()` on the Flow object.
        >>> list(Flow.of(1, 2, 3))
        [1, 2, 3]
        """
        return [it for it in self]

    def to_set(self) -> set[T]:
        return {it for it in self}

    def count(self) -> int:
        """
        Count the number of elements in the flow.

        >>> Flow.of(1, 2, 3, 4, 5).count()
        5
        """
        return sum(1 for _ in self)

    def group_by[K](self, key_fn: Callable[[T], K], /) -> dict[K, list[T]]:
        """
        Group elements by a key function.

        >>> Flow.of(1, 2, 3, 4, 5).group_by(lambda x: x % 2)
        {1: [1, 3, 5], 0: [2, 4]}
        """
        result: dict[K, list[T]] = {}
        for item in self:
            k = key_fn(item)
            if k not in result:
                result[k] = []
            result[k].append(item)
        return result

    def partition_by[K](self, key_fn: Callable[[T], K], /) -> dict[K, "Flow[T]"]:
        """
        Partition elements by a key function.

        >>> Flow.of(1, 2, 3, 4, 5).partition_by(lambda x: x % 2)
        {1: Flow([1, 3, 5]), 0: Flow([2, 4])}
        """
        result = self.group_by(key_fn)
        return {k: Flow(v) for k, v in result.items()}

    def any(self, fn: Callable[[T], bool], /) -> bool:
        """
        Check if any element in the flow satisfies the predicate function.

        >>> Flow.of(1, 2, 3).any(lambda x: x > 2)
        True

        >>> Flow.of(1, 2, 3).any(lambda x: x > 3)
        False
        """
        return any(fn(it) for it in self)

    def all(self, fn: Callable[[T], bool], /) -> bool:
        """
        Check if all elements in the flow satisfy the predicate function.

        >>> Flow.of(1, 2, 3).all(lambda x: x > 0)
        True

        >>> Flow.of(1, 2, 3).all(lambda x: x > 1)
        False
        """
        return all(fn(it) for it in self)

    def __iter__(self) -> Iterator[T]:
        yield from self.iterable

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.iterable})"

    @classmethod
    def of(cls, *items: T) -> "Flow[T]":
        """
        Create a Flow from a predefined sequence of items

        >>> Flow.of(1, 2, 3)
        Flow((1, 2, 3))
        """
        return Flow(items)
