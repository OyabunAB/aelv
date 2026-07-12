# Changelog

## 1.0.0-rc.3 — 2026-07-12

### Added

- ADT pipeline interpreter with heap-allocated trampoline — O(1) JVM stack depth for any operator chain depth
- Lazy pipeline composition via `then()`/`applyTo()` and `Many/One/None.pipelineFrom()`; `MutablePipeline` opt-in escape hatch
- `Either.catching`, `Either.catchingStrict`, `Either.catching(timeout)`, `Either.onLeft` utilities
- `rethrow()`, `sendOrDiscard()`, `collectCancelling()`, `leftUnlessCancelled()`, `using()` internal utilities

### Fixed

- RS §1.9 violation: `onSubscribe`/`onNext` concurrent signaling race — hard gate via `CONFLATED` channel
- Per-subscription thread bomb replaced with shared fixed-pool dispatcher
- Data race in concurrent `flatMap` error path — `AtomicReference` sentinel
- `Sink.emit()` TOCTOU on terminal check
- `RunSource.Range` silent `Int` overflow
- `zip` was using exceptions for control flow — replaced with typed signal channels
- `toList()`/`toSet()` were leaking `MutableList`/`MutableSet` — now truly immutable

### Changed

- Error channel widened from `(AelvException)` to `(Exception)` throughout; `UpstreamErrorException` deleted
- `Signal.Upstream.Error.cause` widened from `AelvException` to `Exception`
- `poll()` returns `Either<Unset, T>` — no nulls, no unchecked casts at fusion boundary
- `connectSource()` returns `Fusion<T>` — no nullable return
- `subscribe()`/`drain()` `onError` callback type changed from `(AelvException)` to `(Exception)`
- `doOnError`, `recover`, `recoverWith` callback type changed from `(AelvException)` to `(Exception)`
- All exception variables named `issue` by convention
- `Unset.kt` renamed `Utils.kt`

---

## 1.0.0-rc.2 — 2026-07-10

### Breaking changes from rc.1

- `Sink` renamed to sealed hierarchy — `BroadcastSink`, `ReplaySink`, `ReplayLastSink` via `Sinks.*` factories
- `Sink.emit()` is now `suspend` — callers block on backpressure instead of dropping
- `Signal.Downstream.Request` is now a `data object` — `Request(n)` pattern matches break
- `flatMap` default concurrency changed from `Int.MAX_VALUE` to `256`

### Added

- Sync fusion protocol — fused pipelines beat RxJava on throughput
- `Many.range()`, `Many.just()`
- `Either.leftOrThrow()`
- JMH benchmark suite vs Reactor, RxJava, Mutiny
- Dependabot configuration

---

## 1.0.0-rc.1 — 2026-07-09

First release candidate.

### Added

- `Many<T>`, `One<T>`, `None<T>` — cold, backpressure-aware Reactive Streams publishers
- Full operator set: `map`, `mapNotNull`, `filter`, `take`, `takeWhile`, `skip`, `skipWhile`, `distinct`, `distinctUntilChanged`, `distinctUntilChangedBy`, `flatMap`, `concatMap`, `flatMapSequential`, `switchMap`, `merge`, `mergeWith`, `concat`, `zip` (Many + One), `combineLatest`, `takeUntilOther`, `delaySubscription`, `buffer`, `bufferTimeout`, `groupBy`, `onBackpressureDrop`, `doOnNext`, `doOnComplete`, `doOnError`, `doOnSubscribe`, `doFinally`, `recover`, `recoverWith`, `retry`, `publishOn`, `subscribeOn`
- Terminal operators: `fold`, `reduce`, `toList`, `toSet`, `first`, `last`, `drain`, `subscribe`
- `One` operators: `map`, `flatMap`, `flatMapMany`, `flatMapNone`, `zipWith`, `recover`, `retry`, `cache`, `await`
- `Sink<T>` — hot multicast push source with `broadcast`, `replay`, and `replayLast(n)` variants
- `Policy.retry()` fluent builder with `Backoff.None`, `Fixed`, and `Exponential` (with jitter) strategies
- `Either<A, B>` — error-as-value return type for all terminal operations
- `Signal` — sealed protocol hierarchy for upstream/downstream communication
- `Verify<T>` — test DSL for asserting publisher behaviour
- RS specification compliance verified via the Reactive Streams TCK
- KDoc on full public API surface
- Coroutines bridge (`Flow.asMany()`, `Publisher.asMany()`, etc.)
