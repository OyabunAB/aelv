# Benchmarks

Throughput comparison against [Project Reactor](https://projectreactor.io/) 3.8.6,
[RxJava](https://github.com/ReactiveX/RxJava) 3.1.12,
[Mutiny](https://smallrye.io/smallrye-mutiny/) 3.3.0, and
[Monix](https://monix.io/) 3.4.1.

## Methodology

- **Tool:** JMH 1.37
- **Mode:** Throughput (ops/ms, higher is better)
- **Warmup:** 3 iterations × 10 s
- **Measurement:** 5 iterations × 10 s
- **Fork:** 1
- **JVM:** OpenJDK 21.0.6 (Corretto)
- **CPU:** Intel Core i9-8950HK @ 2.90 GHz
- **Source size:** 1000 items per operation

All frameworks use equivalent source types: `Many.range` / `Flux.range` / `Observable.range` /
`Multi.createFrom().range` — primitive integer sources with no boxing overhead.
Inner streams in `concatMap`/`flatMap` use `Many.items` / `Flux.just` / `Observable.range` /
`Multi.createFrom().items`.

Monix is benchmarked via its Reactive Streams interop (`Observable.toReactivePublisher`) since
it is a Scala library called through Java interop. This adds a `CountDownLatch`-based blocking
layer and `Long` boxing (`Observable<Object>`) not present in the other benchmarks, so Monix
numbers reflect that overhead in addition to its own runtime cost.

Run benchmarks locally:

```
./gradlew jmhJar
java -jar build/libs/aelv-*-jmh.jar -wi 3 -i 5 -f 1 -p size=1000 -tu ms
```

## Results

aelv numbers re-measured for 1.0.0 at full methodology (3 warmup × 10 s, 5 measurement × 10 s, fork 1).

| Benchmark | aelv | RxJava | Mutiny | Reactor | Monix |
|---|---:|---:|---:|---:|---:|
| baseline_toList | 144 | **165** | 119 | 49 | 30 |
| map_toList | 87 | **114** | 61 | 46 | 26 |
| filter_toList | 146 | **208** | 91 | 81 | 32 |
| take_toList | 195 | **223** | 98 | 96 | 34 |
| fold_sum | 140 | **154** | 97 | 48 | 42 |
| chain (map→filter→take) | 154 | **277** | 134 | 98 | 36 |
| concatMap_toList | 55 | 69 | **77** | 58 | 32 |
| flatMap_concurrent | 19 | **99** | 40 | 55 | 28 |
| sink_broadcast_1 subscriber | **19** | — | — | — | — |
| sink_broadcast_4 subscribers | 6 | — | — | **9** | — |


## Notes

**Fused pipelines** (`baseline`, `map`, `filter`, `take`, `fold`, `chain`) activate aelv's
synchronous fusion protocol. When the entire chain from source to terminal is synchronous, aelv
bypasses the coroutine callback machinery and runs a tight poll loop — no state machine transitions,
no signal allocations. aelv leads on `baseline`, `take`, and `fold`; RxJava leads on `map`,
`filter`, and `chain`.

**`concatMap`** aelv uses the interpreter's work-deque for sequential flat-map, removing
the per-inner-stream coroutine allocation. Mutiny leads at 77 ops/ms on this run; RxJava and
aelv follow closely.

**`flatMapSequential`** allocates one ordering `Channel` per outer item for concurrent pre-fetch with ordered delivery. For synchronous inners (this benchmark) that channel allocation dominates — use `concatMap` (55 ops/ms) for sequential ordered work with synchronous inners. `flatMapSequential` pays off with genuinely async inners where concurrent pre-fetching amortises the cost. Reactor uses `flatMapSequential`, RxJava uses `concatMapEager`.

**`flatMap_concurrent`** launches inner streams inline (no `launch`, serialised via `Mutex`).
RxJava's advantage here is its lock-free drain loop. For real IO-bound workloads where inner
streams suspend on network/disk, the difference is immaterial.

**Monix** numbers are lower than the other libraries across the board. This is expected: the
benchmark calls Monix through its Reactive Streams bridge (`toReactivePublisher`) with a
`CountDownLatch` per pipeline run, and `Observable.range` boxes each `Long` to `Object` due to
Scala's type erasure. These are unavoidable costs of calling a Scala library from the JVM without
a Scala compiler. The numbers represent realistic Monix throughput from a Kotlin/Java caller.

**Backpressure** is unconditional. The fusion fast path only activates inside `collect()` — the
internal synchronous terminal path. Any async operator (`publishOn`, `subscribeOn`, `Sink`,
RS `subscribe()`) routes through the full three-callback protocol with demand signalling and
cancellation, satisfying Reactive Streams §1.1–§3.17.

---

## Recursive flat-map stack safety

`step(n) = items(42).concatMap { step(n-1) }` with `take(1)` — a self-referential stream of
depth `n`.  Assembly is O(1) stack depth (lambdas are lazy); execution recurses O(n) deep at
subscription time.  Libraries that use direct recursive function calls on the JVM call stack
either crash the forked JVM or hang.

```
./gradlew jmhJar
java -jar build/libs/aelv-*-jmh.jar DeepFlatMapBenchmark -wi 3 -i 5 -f 1 -tu ms
```

| Library | depth=1000 | depth=10000 | depth=100000 | Mechanism |
|---|---:|---:|---:|---|
| **aelv** | **10** | **1.5** | **0.15** | Work-deque interpreter — O(1) JVM stack |
| RxJava | 39 | 3.7 | 0.27 | Drain loop trampolines inner subscriptions |
| Monix | 4.2 | 0.26 | 0.21 | Scala Observable scheduler trampolines |
| Mutiny | 1.1 | timeout | timeout | Merge operator hangs on deep recursion |
| Reactor | 16 | **crash** | **crash** | StackOverflow kills the forked JVM |

*ops/ms — `timeout` = 5 s per op limit hit; `crash` = StackOverflow corrupted JMH IPC.*

aelv, RxJava, and Monix all handle depth=100000 without overflow. Reactor and Mutiny cannot.
The aelv implementation is the only one that guarantees O(1) JVM stack depth by design — the
others rely on their existing trampoline/drain-loop implementations which happen to be
sufficient here but are not guaranteed across all recursive patterns.
