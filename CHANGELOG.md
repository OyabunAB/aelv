# Changelog

## 1.0.0-rc.1 — 2026-07-09

First release candidate.

### Added

- `Many<T>`, `One<T>`, `None<T>` — cold, backpressure-aware Reactive Streams publishers
- Full operator set: `map`, `mapNotNull`, `filter`, `take`, `takeWhile`, `skip`, `skipWhile`, `distinct`, `distinctUntilChanged`, `distinctUntilChangedBy`, `flatMap`, `concatMap`, `flatMapSequential`, `switchMap`, `merge`, `mergeWith`, `concat`, `zip` (Many + One), `combineLatest`, `takeUntilOther`, `delaySubscription`, `buffer`, `bufferTimeout`, `groupBy`, `onBackpressureDrop`, `doOnNext`, `doOnComplete`, `doOnError`, `doOnSubscribe`, `doFinally`, `recover`, `recoverWith`, `retry`, `publishOn`, `subscribeOn`
- Terminal operators: `fold`, `reduce`, `toList`, `toSet`, `first`, `last`, `drain`, `subscribe`
- `One` operators: `map`, `flatMap`, `flatMapMany`, `flatMapNone`, `zipWith`, `recover`, `retry`, `cache`, `get`
- `Sink<T>` — hot multicast push source with `broadcast`, `replay`, and `replayLast(n)` variants
- `Policy.retry()` fluent builder with `Backoff.None`, `Fixed`, and `Exponential` (with jitter) strategies
- `Either<A, B>` — error-as-value return type for all terminal operations
- `Signal` — sealed protocol hierarchy for upstream/downstream communication
- `Verify<T>` — test DSL for asserting publisher behaviour
- RS specification compliance verified via the Reactive Streams TCK
- KDoc on full public API surface
- Coroutines bridge (`Flow.asMany()`, `Publisher.asMany()`, etc.)
