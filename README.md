# ≋ aelv

Reactive streams for Kotlin, built on coroutines.

Requires Kotlin 2.x and JVM 21+.

```kotlin
implementation("se.oyabun:aelv:1.0.0")
```

---

## Why aelv

### Type-safe

Four types. The compiler enforces what each one means.

```kotlin
val items:  Many<Int>   = Many.items(1, 2, 3)          // 0..N items
val single: One<Int>    = One.defer { fetchUser(id) }   // exactly 1 — never empty, never null
val maybe:  Maybe<Int>  = Maybe.defer { findOrNull() }  // 0 or 1 — never null
val effect: None<Unit>  = None.defer { db.commit() }    // side-effect — produces nothing
```

`Mono<T?>` carries no information about whether null is expected or an error — the compiler treats both identically.  
`Maybe<T>` encodes the distinction: a value or nothing, null excluded from the type.

### No-throw

Terminal operators return `Either<Exception, T>`.

```kotlin
when (val result = stream.toList().await()) {
    is Success -> process(result.value)
    is Failure -> handleError(result.value)
}
```

### Backpressure by default

Every `subscribe()` call is bounded. Unbounded demand requires an explicit opt-in.

```kotlin
stream.subscribe(prefetch = 256) { item -> process(item) }  // bounded — default
stream.drain { item -> process(item) }                       // unbounded — explicit
```

### Stack-safe

aelv's work-deque interpreter gives O(1) JVM stack depth for any operator chain.
Reactor throws `StackOverflowError` at depth 10 000. aelv handles 100 000.

```kotlin
// Paginated API traversal — each page fetches the next
fun fetchPage(cursor: String): Many<Item> =
    Many.defer { api.getPage(cursor) }
        .flatMap { page -> if (page.hasNext) fetchPage(page.nextCursor) else Many.empty() }

fetchPage("start").toList().await()
```

---

## Operators

### Many

| Category | Operators |
|---|---|
| Transform | `map` `mapNotNull` `filter` `take` `takeWhile` `skip` `skipWhile` `distinct` `distinctUntilChanged` `distinctUntilChangedBy` |
| Expand | `flatMap` `flatMapOne` `flatMapNone` `concatMap` `flatMapSequential` `switchMap` |
| Combine | `merge` `mergeWith` `concat` `zip` `zipWith` `combineLatest` `takeUntilOther` `delaySubscription` |
| Buffer | `buffer(size)` `buffer(size, skip)` `bufferTimeout` |
| Group | `groupBy` |
| Side-effect | `doOnNext` `doOnComplete` `doOnError` `doOnSubscribe` `doFinally` `doOnRetry` `doOnRecover` |
| Error | `recover` `recoverWith` `retry(n)` `retry(Policy)` `onBackpressureDrop` |
| Utility | `delayElement(Duration)` `interval(Duration)` `discard()` `thenReturn(value)` |
| Context | `publishOn` `subscribeOn` |
| Terminal | `fold` `reduce` `scan` `toList` `toSet` `first` `firstMaybe` `last` `drain` `subscribe` |

### One

| Category | Operators |
|---|---|
| Transform | `map` `flatMap` `flatMapMany` `flatMapMaybe` `flatMapNone` |
| Combine | `zipWith` `concatWith` |
| Side-effect | `doOnNext` `doOnError` `doFinally` |
| Error | `recover` `retry(n)` `retry(Policy)` |
| Utility | `discard()` `thenReturn(value)` |
| Context | `publishOn` `subscribeOn` |
| Terminal | `await` `cache` |

### Maybe

| Category | Operators |
|---|---|
| Transform | `map` `filter` `flatMap` `flatMapMany` `flatMapNone` |
| Combine | `concatWith` |
| Side-effect | `doOnNext` `doOnComplete` `doOnError` `doFinally` |
| Error | `recover` `retry(n)` `retry(Policy)` |
| Utility | `discard()` `thenReturn(value)` |
| Context | `publishOn` `subscribeOn` |
| Terminal | `await` `or` `orMany` `toOne` |

### None

| Category | Operators |
|---|---|
| Chain | `andThen(() -> One<R>)` `andThen(() -> Maybe<R>)` `andThen(() -> Many<R>)` `andThen(() -> None<R>)` |
| Utility | `discard()` `thenReturn(value)` |
| Terminal | `await` |

## Sink

Hot multicast push source. Three variants:

```kotlin
val sink = Sinks.broadcast<Int>()   // no history — present subscribers only
val sink = Sinks.replay<Int>()      // full history — late subscribers see everything
val sink = Sinks.replayLast(n)      // last n items replayed to late subscribers
```

```kotlin
sink.asMany().filter { it > 0 }.subscribe(...)
sink.emit(1)        // throws on overflow or after terminal
sink.tryEmit(1)     // returns false instead of throwing
sink.complete()
```

## Retry

```kotlin
Many.defer { api.fetchItems() }
    .retry(
        Policy.retry()
            .on(IOException::class)
            .on(ExceededTimeoutException::class)
            .withBackoff(100.milliseconds, 10.seconds, jitter = true)
            .maxAttempts(5)
    )
```

`Backoff` options: `None`, `Fixed(delay)`, `Exponential(initial, max, factor, jitter)`.

## Verify

Test DSL ships with the library:

```kotlin
Verify.that(publisher).emitsNext(1, 2, 3).completes()
Verify.that(publisher).assertNext { assertEquals(42, it) }.completes()
Verify.that(maybePublisher).emitsCount(0).completes()
Verify.that(publisher).failsWith<ExceededTimeoutException>()
Verify.that(slowPublisher).timesOut(within = 100.milliseconds)
```

## Configuration

```kotlin
Aelv.loggingEnabled = true      // SLF4J logging — off by default
Aelv.bufferSize     = 8192      // sink ring buffer size
Aelv.cpuPoolSize    = 4         // override for containers where availableProcessors() lies
Aelv.prefetch       = 256L      // default subscribe() prefetch window
Aelv.verifyTimeout  = 10.seconds
```

## Dispatchers

```kotlin
subscribeOn(Dispatchers.cpu)   // aelv's named CPU pool — aelv-cpu-N threads
subscribeOn(Dispatchers.io)    // aelv's IO pool — aelv-io-N virtual threads (JVM 21)
```

All aelv threads are named. `aelv-cpu-N` in a thread dump means an aelv producer coroutine.

## RS Compliance

TCK-verified. 152 tests, 0 failures across `Many`, `One`, `Maybe`, and `None`.

## Performance

**Stack safety:**

| depth | aelv | RxJava | Monix | Mutiny | Reactor |
|---|---:|---:|---:|---:|---|
| 1 000 | 10 | 39 | 4.2 | 1.1 | 16 |
| 10 000 | 1.5 | 3.7 | 0.26 | timeout | **crash** |
| 100 000 | 0.15 | 0.27 | 0.21 | timeout | **crash** |

*ops/ms — higher is better.*

**IO-bound concurrent work:**

```
100 parallel IO calls, 1ms each:
  flatMap(concurrency=256):  ~1ms   — 87× faster than sequential
  concatMap:               ~100ms
```

aelv, RxJava, Reactor, Mutiny, and Monix all achieve the same ~87× speedup.

**Synchronous pipeline throughput** (ops/ms, 1000 items):

| Benchmark | aelv | RxJava | Mutiny | Reactor | Monix |
|---|---:|---:|---:|---:|---:|
| baseline_toList | 153 | **165** | 119 | 49 | 30 |
| map_toList | 92 | **114** | 61 | 46 | 26 |
| filter_toList | 155 | **208** | 91 | 81 | 32 |
| take_toList | 207 | **223** | 98 | 96 | 34 |
| fold_sum | **174** | 154 | 97 | 48 | 42 |
| chain (map→filter→take) | 199 | **277** | 134 | 98 | 36 |
| flatMap_concurrent | 67 | **99** | 40 | 55 | 28 |

aelv is within 15% of RxJava and leads Reactor on all fused benchmarks.

See [BENCHMARKS.md](BENCHMARKS.md) for full methodology and deep-flatMap results.

## Status

See [CHANGELOG.md](CHANGELOG.md) for full history.
