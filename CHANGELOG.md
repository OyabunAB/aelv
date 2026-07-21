# Changelog

## 1.0.0 — 2026-07-21

### Added

- `Aelv` configuration object — `bufferSize`, `maxSlowBuffer`, `prefetch`, `cpuPoolSize`, `verifyTimeout`, `loggingEnabled` (off by default)
- `Dispatchers.cpu` — named fixed CPU pool (`aelv-cpu-N` threads); `Dispatchers.io` — named virtual thread pool (`aelv-io-N`, JVM 21)
- `Many.zipWith(other, transform)` — fluent alias for `zip(this, other, transform)`, consistent with `One.zipWith`
- `One.concatWith(other: One<T>): Many<T>` and `Maybe.concatWith(other: Maybe<T>): Many<T>`
- `Outcome<T>` type alias (`Either<Exception, T>`) — used at all terminal operator return sites
- `OnNext<T>`, `OnComplete`, `OnError` type aliases for the three-callback protocol
- `isSet()` extension on `AtomicReference<Any>` — replaces double-negative `notUnset()` sentinel checks
- `BufferEvent` sealed type in `bufferTimeout` — replaces `Either` abuse (`Success`=timer, `Failure`=source)
- `ProducerState` sealed type in `StreamSubscription`/`CompletionSubscription` — replaces `Job().also { it.cancel() }` sentinel
- `TimerState` sealed type in `bufferTimeout` — replaces `Job().also { it.complete() }` sentinel
- Structured opt-in SLF4J logging with full level coverage: TRACE (lifecycle), DEBUG (sink attach/detach, retry), WARN (errors, side-effect throws), ERROR (onError callback failure, unexpected Throwable)
- `doOnRetry` and `doOnRecover` KDoc with ordering semantics documented
- `interval(period)` precondition: `require(period.isPositive())`
- `UNBOUNDED` and `DEFAULT_PREFETCH` named constants; `subscribe()` defaults to `DEFAULT_PREFETCH = 256`
- IO workload JMH benchmarks (`flatMap_io` / `concatMap_io`) across all four libraries with shared `Workload` constants

### Changed

- **Breaking:** `NoSuchElementException` → `NoElementException`; `TimeoutException` → `ExceededTimeoutException` — avoid shadowing `kotlin.NoSuchElementException` and `java.util.concurrent.TimeoutException`
- **Breaking:** `then()` / `applyTo()` → `andThen()` — unified pipeline composition operator
- `AelvException` extends `RuntimeException`; `InvalidDemandException` extends `IllegalArgumentException` directly (RS §3.9 requires it, not the entire exception hierarchy)
- `flatMap(concurrency)` is now actually concurrent — was sequential (identical to `concatMap`) in all prior releases. Fused inners use fusion fast path (no launch/queue). Async inners launch concurrently with UNDISPATCHED start and lock-free WIP drain.
- `Maybe` constructor changed to `private` (was `internal`) — consistent with `Many`, `One`, `None`
- `Policy.on()` is now additive — multiple `.on()` calls OR their predicates. Previously last call silently replaced the previous filter.
- `doOnRecover` fires once on the first signal after recovery, not on every subsequent item
- `BroadcastSink` fan-out: yield-before-park strategy replaces immediate park — eliminates scheduler round-trips for high-throughput concurrent subscribers (0.37 → 7.7 ops/ms for 4-subscriber broadcast)
- `doEmit` iterates subscribers in a single pass (was three passes)
- `bufferTimeout` uses `BufferEvent` sealed type instead of `Either` for the merged event channel
- `request()` overflow guard uses correct headroom check (`UNBOUNDED - current < n`) — previous guard could overflow before triggering
- `collectCancelling` rethrows external `CancellationException` — was silently swallowing structured concurrency cancellation
- `ObservableKDoc` corrected — `Observable` is public API, not internal

### Fixed

- `Maybe.filter` (suspend overload) called `onComplete` from inside `onNext` — RS §1.3 serial signal violation
- `bracket` (`Many/One/Maybe/None.resource`) evaluated `use(resource)` before establishing release — resource leaked if `use()` threw
- `doOnRecover` fired on every item after a retry instead of once on recovery
- `One.resource` threw raw `IllegalStateException` on empty `use` result — now signals `NoElementException` through `onError`
- Dead `guardedSideEffect` (non-suspend) removed from `Observable`
- Dead `private val log` declarations removed from `OneOperators`, `MaybeOperators`, `NoneOperators`
- Dead `@file:OptIn(ExperimentalTypeInference::class)` removed from `Observable`
- `interval` unreachable `onComplete()` after `while(true)` removed
- Stale `import com.sun.jdi.request.InvalidRequestStateException` removed from `OperatorsTest`
- Vacuous `assertIs<IllegalArgumentException>` in `SinksTest` replaced with `assertFailsWith`
- `switchMap` test now exercises actual cancellation of in-flight inner subscriptions
- `UnicastSink` tests deduplicated — `SinksTest.UnicastSinkTest` removed, `UnicastSinkTest.kt` is canonical
- Stale BackpressureTest comment describing a previously fixed bug removed

---

## 1.0.0-rc.7 — 2026-07-20

### Added

- `timeout(Duration)` operator on `Observable`/`Many`/`One`/`Maybe` — fires `ExceededTimeoutException` if no item or terminal arrives within the deadline; wakeup channel only activates when subscriber is actually waiting
- `Verify.timesOut()` terminal assertion for timeout tests
- TCK: `ManyVerification`, `OneVerification`, `MaybeVerification`, `NoneVerification` — 152 tests, 0 failures

### Changed

- Ring buffer sink — per-subscriber `Channel` fan-out replaced with shared ring buffer and per-subscriber cursor (`SubHandle`); slow subscribers promoted to `BoundedQueue`; `doEmit` lock-free for `BroadcastSink`; `DEFAULT_BUFFER` raised to 4096
- `StreamSubscription` unbounded fast path — `demand == Long.MAX_VALUE` skips `awaitDemand()`/`demand.updateAndGet()` per item; `signal` Channel allocated lazily; `subscribeGate` Channel removed
- `Interpreter.execSuspend` direct `Frame.Collect` dispatch — eliminates `ContinuationImpl` allocation per delivered item
- Fusion propagated across `One` and `Maybe`; `Maybe.fromStep` wraps with `TakeFusion(1)` to enforce at-most-one constraint
- `Observable` is now a public abstract class — `Many`/`One`/`Maybe`/`None` extend it; `Verify<T, S>` preserves source type
- Stream lifecycle log events demoted to TRACE

### Fixed

- `Sink.asMany` register race — `writeStart` snapshot and `subscribers.add` are now co-located; for replay sinks, both occur under `histLock` so the history snapshot and cursor start position are consistent. A one-item duplicate is still theoretically possible in the window between `histLock` release and `writePos` increment, but moving `writePos` inside the lock tanked emit throughput and the window is negligible in practice.
- `doFinally` Cancel path did not fire in all cancellation scenarios
- `Sink.emit` overflow now throws `IllegalStateException` on full slow buffer
- `None.error(Throwable)` now wraps in `Exception` correctly
- `LowPriorityInOverloadResolution` removed from `Observable`, `OneOperators`, `MaybeOperators`, `NoneOperators` (retained only in `ManyOperators` and `Terminations` where fused vs coroutine overloads differ)
- Multiple `Disposable.cancel` before `onSubscribe` now correctly prevents subscription start
- `takeWhile` signals `onComplete` from the completion path, not from the `onNext` callback
- `CompletionSubscription` guards against multiple `request()` calls
- `flatMapSequential` does not emit `Complete` after downstream cancel
- `UnicastSink.asMany` forwards `Complete` signal to downstream
- `retry` intercepts `onError` callback instead of catching exceptions

---

## 1.0.0-rc.6 — 2026-07-17

### Added

- `delaySubscription(Duration)` and `delaySubscription(Publisher)` on `One`, `Maybe`, and `None`
- `retry(times)` and `retry(Policy)` on `One`, `Maybe`, and `None`
- `doOnRetry` and `doOnRecover` on all four types
- KDoc on `delaySubscription` and `retry` for `One`, `Maybe`, and `None`
- Extended test coverage: `delaySubscription(Duration)` on all four types; `retry(times)` on `Maybe` and `None`
- `UnicastSink` enforces single-subscriber — second subscriber receives `IllegalStateException` immediately

---

## 1.0.0-rc.5 — 2026-07-17

### Added

- `resource` bracket operator on `Many`, `One`, `Maybe`, `None` — release always runs even on downstream cancel
- `flatMapNone` on `Many`, `One`, `Maybe`, `None`
- `doOnNext`, `doOnComplete`, `doOnError`, `doOnSubscribe`, `doFinally` on `Maybe` and `None`
- `subscribeOn`/`publishOn` on `Maybe` and `None`
- `Either.success()`/`Either.failure()` companion factories
- Operator files split by type: `ManyOperators.kt`, `OneOperators.kt`, `MaybeOperators.kt`, `NoneOperators.kt`
- `Verify` rewritten as a pure pipeline builder; `Verify<T, S>` generic over source type

### Fixed

- `zip` cancel semantics — downstream cancel correctly stops both sources
- `None.recover` added
- `Maybe.retry` now signals `onComplete` after successful retry
- `firstMaybe` cancel fix

---

## 1.0.0-rc.4 — 2026-07-14

### Added

- `Maybe<T>` — fourth publisher type for 0-or-1 items; no nulls, no more than one element
- `flatMapMany`, `flatMapMaybe`, `flatMapNone` on `One`
- `UnicastSink` — single-subscriber hot source; subsequent subscribers receive `IllegalStateException`
- `Verify.assertNext { }` — assertion lambda over the next item
- `Verify.timesOut(within)` — asserts the stream fires `ExceededTimeoutException` within the deadline
- `scan(initial, accumulate)` operator on `Many`
- `Many.defer(suspend () -> Many<T>)` suspend factory variant
- `Either.rightOrThrow()` utility
- Suspend overloads for `map`, `flatMap`, `concatMap`, `filter`, `doOn*`, `recover`, `fold`, `defer` — annotated `@LowPriorityInOverloadResolution`

### Changed

- Fusion poll loop: per-item `Either` allocation eliminated
- `None.then` chaining — run subsequent publisher after side-effect completes

### Fixed

- Stale `Many.source` override identical to `Observable` base removed

---

## 1.0.0-rc.3 — 2026-07-12

### Added

- ADT pipeline interpreter with heap-allocated trampoline — O(1) JVM stack depth for any operator chain depth
- Lazy pipeline composition via `andThen()` and `Many/One/None.pipelineFrom()`; `MutablePipeline` opt-in escape hatch
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

- Sync fusion protocol — fused pipelines competitive with RxJava on throughput; leads on `concatMap` and deep recursive flat-map
- `Many.range()`, `Many.items()`
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
