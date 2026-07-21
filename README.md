# ≋ aelv

Minimalistic reactive streams for Kotlin. Implements the [Reactive Streams](https://www.reactive-streams.org/) specification on top of Kotlin coroutines.

## Requirements

- Kotlin 2.x
- JVM 21+

## Install

```kotlin
implementation("se.oyabun:aelv:1.0.0")
```

## Types

Four publisher types, each cold and backpressure-aware:

```mermaid
%%{init: {'flowchart': {'curve': 'linear'}}}%%
flowchart LR
    Many["Many&lt;T&gt; — 0..N"] --> P[Publisher]
    One["One&lt;T&gt; — exactly 1"] --> P
    Maybe["Maybe&lt;T&gt; — 0 or 1, no nulls"] --> P
    None["None&lt;T&gt; — side-effect"] --> P
```

```kotlin
val items: Many<Int>    = Many.items(1, 2, 3)
val single: One<Int>    = One.defer { fetchFromDb() }
val maybe: Maybe<Int>   = Maybe.defer { findOrNull() }
val effect: None<Unit>  = None.defer { db.commit() }
```

`Maybe<T>` emits either one item or completes empty — never null, never more than one element.

## Signals

Every interaction between producer and consumer flows through `Signal`:

```mermaid
%%{init: {'flowchart': {'curve': 'linear'}}}%%
flowchart LR
    N[Next] --> R[Request n]
    C[Complete] --> X[Cancel]
    E[Error] --> X
    N --> X
```

## Operators

### Many

| Category | Operators |
|---|---|
| Transform | `map` `mapNotNull` `filter` `take` `takeWhile` `skip` `skipWhile` `distinct` `distinctUntilChanged` `distinctUntilChangedBy` |
| Expand | `flatMap` `flatMapOne` `flatMapNone` `concatMap` `flatMapSequential` `switchMap` |
| Combine | `merge` `mergeWith` `concat` `zip` `combineLatest` `takeUntilOther` `delaySubscription` |
| Buffer | `buffer(size)` `buffer(size, skip)` `bufferTimeout` |
| Group | `groupBy` |
| Side-effect | `doOnNext` `doOnComplete` `doOnError` `doOnSubscribe` `doFinally` |
| Error | `recover` `recoverWith` `retry(n)` `retry(Policy)` `onBackpressureDrop` |
| Utility | `delayElement(Duration)` `interval(Duration)` `discard()` `thenReturn(value)` |
| Context | `publishOn` `subscribeOn` |
| Terminal | `fold` `reduce` `toList` `toSet` `first` `firstMaybe` `last` `drain` `subscribe` |

`flatMap(T -> Many<R>)` — standard fan-out, returns `Many<R>`  
`flatMapOne(T -> One<R>)` — each element maps to exactly one, returns `Many<R>`  
`flatMapNone(T -> None<R>)` — each element triggers a side-effect, returns `None<R>`

### One

| Category | Operators |
|---|---|
| Transform | `map` `flatMap` `flatMapMany` `flatMapMaybe` `flatMapNone` |
| Combine | `zipWith` |
| Side-effect | `doOnNext` `doOnError` `doFinally` |
| Error | `recover` `retry(n)` `retry(Policy)` |
| Utility | `discard()` `thenReturn(value)` |
| Context | `publishOn` `subscribeOn` |
| Terminal | `await` `cache` |

`flatMap(T -> One<R>)` — returns `One<R>`  
`flatMapMany(T -> Many<R>)` — returns `Many<R>`  
`flatMapMaybe(T -> Maybe<R>)` — returns `Maybe<R>`  
`flatMapNone(T -> None<R>)` — returns `None<R>`

### Maybe

| Category | Operators |
|---|---|
| Transform | `map` `filter` `flatMap` `flatMapMany` `flatMapNone` |
| Side-effect | `doOnNext` `doOnComplete` `doOnError` `doFinally` |
| Error | `recover` `retry(n)` `retry(Policy)` |
| Utility | `discard()` `thenReturn(value)` |
| Context | `publishOn` `subscribeOn` |
| Terminal | `await` `or` `orMany` `toOne` |

`flatMap(T -> Maybe<R>)` — returns `Maybe<R>`  
`flatMapMany(T -> Many<R>)` — returns `Many<R>`  
`flatMapNone(T -> None<R>)` — returns `None<R>`

### None

| Category | Operators |
|---|---|
| Chain | `andThen(() -> One<R>)` `andThen(() -> Maybe<R>)` `andThen(() -> Many<R>)` `andThen(() -> None<R>)` |
| Utility | `discard()` `thenReturn(value)` |
| Terminal | `await` |

`andThen` chains a subsequent publisher that runs after the side-effect completes, returning the appropriate type.

### zip

```kotlin
zip(One.single(1), One.single("a")) { n, s -> "$n$s" }  // One<String> → "1a"
```

## Conversions

| Expression | Result |
|---|---|
| `one.toMaybe()` | `Maybe<T>` — wraps the single value |
| `one.toMany()` | `Many<T>` — stream of one element |
| `many.firstMaybe()` | `Maybe<T>` — first element or empty |
| `none.toMany()` | `Many<T>` — empty stream after effect completes |

```kotlin
val maybeUser: Maybe<User> = One.defer { db.findUser(id) }.toMaybe()
val firstHit: Maybe<Result> = results.firstMaybe()
```

## Sink

Hot multicast push source. Three variants:

```mermaid
%%{init: {'flowchart': {'curve': 'linear'}}}%%
flowchart LR
    E[emit/complete/error] --> S{Sink}
    S -->|broadcast| A[no history]
    S -->|replay| B[full history]
    S -->|replayLast n| C[last n]
```

```kotlin
val sink = Sinks.broadcast<Int>()
sink.asMany().filter { it > 0 }.subscribe(...)
sink.emit(1)           // throws on overflow or after terminal
sink.tryEmit(1)        // returns false instead of throwing
sink.complete()
```

## Retry

```kotlin
Many.items(1, 2, 3)
    .retry(
        Policy.retry()
            .on(ExceededTimeoutException::class)
            .withBackoff(100.milliseconds, 10.seconds)
            .maxAttempts(5)
    )
```

`Backoff` options: `None`, `Fixed(delay)`, `Exponential(initial, max, factor, jitter)`.

## Error handling

Terminal operations return `Either<Exception, T>` — no exceptions thrown at call sites. `Failure` carries the error, `Success` carries the value.

```kotlin
when (val result = stream.toList().await()) {
    is Success -> process(result.value)
    is Failure -> handleError(result.value)
}
```

## Verify

Test DSL included in the main artifact:

```kotlin
Verify.that(publisher)
    .emitsNext(1, 2, 3)
    .completes()

// assert individual items
Verify.that(publisher)
    .assertNext { assertEquals(1, it) }
    .completes()

// empty completion
Verify.that(maybePublisher).emitsCount(0).completes()

// error assertions
Verify.that(publisher).failsWith<ExceededTimeoutException>()
```

| Method | Applicable to |
|---|---|
| `completes()` | Many, One, Maybe, None |
| `emitsNext(vararg values)` | Many, One |
| `assertNext { predicate }` | Many, One, Maybe |
| `emitsCount(n)` | Many |
| `cancels()` | Many, One, Maybe, None |
| `fails()` | Many, One, Maybe, None |
| `failsWith<X>()` | Many, One, Maybe, None |
| `timesOut()` | Many, One, Maybe |

## RS Compliance

TCK-verified. `Many` passes all applicable RS Publisher specs. `One` passes all single-element specs.

## Performance

aelv implements a synchronous fusion protocol for fused pipelines — when the entire chain from
source to terminal is synchronous, the coroutine callback machinery is bypassed in favour of a
tight poll loop. aelv leads on `baseline_toList`, `take_toList`, and `fold_sum`; RxJava leads on
`map`, `filter`, `chain`, and concurrent `flatMap`. aelv leads all libraries on deep recursive
flat-map due to its work-deque interpreter (O(1) JVM stack depth).

| Benchmark | aelv | RxJava | Mutiny | Reactor | Monix |
|---|---:|---:|---:|---:|---:|
| baseline_toList | **186** | 165 | 119 | 49 | 30 |
| map_toList | 100 | **114** | 61 | 46 | 26 |
| filter_toList | 174 | **208** | 91 | 81 | 32 |
| take_toList | **230** | 223 | 98 | 96 | 34 |
| fold_sum | **154** | **154** | 97 | 48 | 42 |
| chain (map→filter→take) | 220 | **277** | 134 | 98 | 36 |
| concatMap_toList | 59 | 69 | **77** | 58 | 32 |
| flatMap_sequential | 64 | **136** | 92 | 83 | 37 |
| flatMap_concurrent | 23 | **99** | 40 | 55 | 28 |

*ops/ms on 1000 items, JMH throughput mode, OpenJDK 21, Intel i9-8950HK. See [BENCHMARKS.md](BENCHMARKS.md) for methodology.*

Backpressure is unconditional — fusion only activates on the internal synchronous terminal path.
Any async operator routes through the full protocol with demand signalling and cancellation.

## Status

See [CHANGELOG.md](CHANGELOG.md) for full history.
