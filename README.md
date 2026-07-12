# ≋ aelv

Minimalistic reactive streams for Kotlin. Implements the [Reactive Streams](https://www.reactive-streams.org/) specification on top of Kotlin coroutines.

## Install

```kotlin
// build.gradle.kts
repositories {
    maven {
        url = uri("https://maven.pkg.github.com/OyabunAB/aelv")
        credentials {
            username = System.getenv("GITHUB_ACTOR")
            password = System.getenv("GITHUB_TOKEN")
        }
    }
}

dependencies {
    implementation("se.oyabun:aelv:1.0.0-rc.1")
}
```

## Types

Three publisher types, each cold and backpressure-aware:

```mermaid
%%{init: {'flowchart': {'curve': 'linear'}}}%%
flowchart LR
    Many["Many&lt;T&gt; — 0..N"] --> P[Publisher]
    One["One&lt;T&gt; — exactly 1"] --> P
    None["None&lt;T&gt; — side-effect"] --> P
```

```kotlin
val items: Many<Int>  = Many.of(1, 2, 3)
val single: One<Int>  = One.defer { fetchFromDb() }
val effect: None<Unit> = None.defer { db.commit() }
```

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
| Expand | `flatMap` `concatMap` `flatMapSequential` `switchMap` |
| Combine | `merge` `mergeWith` `concat` `zip` `combineLatest` `takeUntilOther` `delaySubscription` |
| Buffer | `buffer(size)` `buffer(size, skip)` `bufferTimeout` |
| Group | `groupBy` |
| Side-effect | `doOnNext` `doOnComplete` `doOnError` `doOnSubscribe` `doFinally` |
| Error | `recover` `recoverWith` `retry(n)` `retry(Policy)` `onBackpressureDrop` |
| Context | `publishOn` `subscribeOn` |
| Terminal | `fold` `reduce` `toList` `toSet` `first` `last` `drain` `subscribe` |

### One

| Category | Operators |
|---|---|
| Transform | `map` `flatMap` `flatMapMany` `flatMapNone` |
| Combine | `zipWith` |
| Side-effect | `doOnNext` `doOnError` `doFinally` |
| Error | `recover` `retry(n)` `retry(Policy)` |
| Context | `publishOn` `subscribeOn` |
| Terminal | `get` `cache` |

### None

| Category | Operators |
|---|---|
| Terminal | `await` |

### zip

```kotlin
zip(One.of(1), One.of("a")) { n, s -> "$n$s" }  // One<String> → "1a"
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
val sink = Sink.broadcast<Int>()
sink.asMany().filter { it > 0 }.subscribe(...)
sink.emit(1)
sink.complete()
```

## Retry

```kotlin
Many.of(...)
    .retry(
        Policy.retry()
            .on(TimeoutException::class)
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
    .isSubscribed()
    .runs { source.emit(1) }
    .emitsNext(1)
    .completesNormally()
```

## RS Compliance

TCK-verified. `Many` passes all applicable RS Publisher specs. `One` passes all single-element specs.

## Performance

aelv implements a synchronous fusion protocol for fused pipelines — when the entire chain from
source to terminal is synchronous, the coroutine callback machinery is bypassed in favour of a
tight poll loop.

| Benchmark | aelv | RxJava | Mutiny | Reactor |
|---|---:|---:|---:|---:|
| baseline_toList | **226** | 154 | 57 | 48 |
| map_toList | **132** | 114 | 55 | 47 |
| filter_toList | **225** | 170 | 100 | 60 |
| take_toList | **301** | 236 | 184 | 76 |
| fold_sum | **188** | 160 | 68 | 43 |
| chain (map→filter→take) | **269** | 254 | 136 | 101 |
| concatMap_toList | 49 | 57 | **80** | 56 |
| flatMap_concurrent | 35 | **87** | 33 | 55 |

*ops/ms on 1000 items, JMH throughput mode, OpenJDK 21, Intel i9-8950HK. See [BENCHMARKS.md](BENCHMARKS.md) for methodology.*

Backpressure is unconditional — fusion only activates on the internal synchronous terminal path.
Any async operator routes through the full protocol with demand signalling and cancellation.
