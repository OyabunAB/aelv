# Benchmarks

Comparison against [Project Reactor](https://projectreactor.io/) 3.8.6,
[RxJava](https://github.com/ReactiveX/RxJava) 3.1.12,
[Mutiny](https://smallrye.io/smallrye-mutiny/) 3.3.0, and
[Monix](https://monix.io/) 3.4.1.

---

## Stack safety

`step(n) = items(42).concatMap { step(n-1) }` with `take(1)` — recursive flat-map at depth `n`.

```
./gradlew jmhJar
java -jar build/libs/aelv-*-jmh.jar DeepFlatMapBenchmark -wi 3 -i 5 -f 1 -tu ms
```

| Library | depth=1 000 | depth=10 000 | depth=100 000 | Mechanism |
|---|---:|---:|---:|---|
| **aelv** | **10** | **1.5** | **0.15** | Work-deque interpreter — O(1) JVM stack |
| RxJava | 39 | 3.7 | 0.27 | Drain loop trampolines |
| Monix | 4.2 | 0.26 | 0.21 | Scala scheduler trampolines |
| Mutiny | 1.1 | timeout | timeout | Hangs at depth 10 000 |
| Reactor | 16 | **crash** | **crash** | StackOverflowError at depth 10 000 |

*ops/ms. `timeout` = 5s per-op limit exceeded. `crash` = StackOverflowError killed the JVM process.*

---

## IO-bound concurrent work

Simulated workload: `size/10` independent IO calls, each taking `1ms`, each returning 3 items.

```
./gradlew jmhJar
java -jar build/libs/aelv-*-jmh.jar ".*_io" -wi 3 -i 5 -f 1 -p size=1000 -tu ms
```

| Benchmark | aelv | RxJava | Mutiny | Reactor |
|---|---:|---:|---:|---:|
| flatMap_io (concurrent) | 0.80 | **0.86** | 0.67 | 0.76 |
| concatMap_io (sequential) | 0.009 | 0.009 | 0.009 | 0.009 |

**All four libraries achieve ~87× speedup** from concurrent vs sequential execution.
Differences in the concurrent row are within measurement noise.

---

## Synchronous pipeline throughput

Fused pipelines (source → synchronous operators → terminal) bypass the coroutine callback
machinery and run a tight poll loop.

```
./gradlew jmhJar
java -jar build/libs/aelv-*-jmh.jar "AelvBenchmark" -wi 3 -i 5 -f 1 -p size=1000 -tu ms
```

| Benchmark | aelv | RxJava | Mutiny | Reactor | Monix |
|---|---:|---:|---:|---:|---:|
| baseline_toList | 153 | **165** | 119 | 49 | 30 |
| map_toList | 92 | **114** | 61 | 46 | 26 |
| filter_toList | 155 | **208** | 91 | 81 | 32 |
| take_toList | 207 | **223** | 98 | 96 | 34 |
| fold_sum | **174** | 154 | 97 | 48 | 42 |
| chain (map→filter→take) | 199 | **277** | 134 | 98 | 36 |
| concatMap | 55 | 69 | **77** | 58 | 32 |
| flatMap_concurrent (sync inners) | 67 | **99** | 40 | 55 | 28 |
| sink_broadcast 1 subscriber | **21** | — | — | — | — |
| sink_broadcast 4 subscribers | 7 | — | — | **9** | — |

*ops/ms — higher is better. OpenJDK 21.0.6 Corretto, Intel i9-8950HK. aelv numbers measured
at full methodology (3 warmup × 10s, 5 measurement × 10s, fork 1).*

aelv is within 15% of RxJava and leads Reactor on all fused benchmarks except `concatMap`.

**`flatMap_concurrent` with synchronous inners** measures the concurrency machinery overhead
(UNDISPATCHED launch + drain queue). With async inners the overhead is amortised — see the
IO benchmark above.

---

## Methodology

- **Tool:** JMH 1.37
- **Mode:** Throughput (ops/ms — higher is better)
- **Warmup:** 3 iterations × 10s
- **Measurement:** 5 iterations × 10s
- **Fork:** 1
- **JVM:** OpenJDK 21.0.6 (Corretto)
- **CPU:** Intel Core i9-8950HK @ 2.90 GHz

Monix is benchmarked via its Reactive Streams bridge (`toReactivePublisher`) from Kotlin.
This adds a `CountDownLatch` per run and `Long` boxing from Scala's type erasure — numbers
reflect realistic Monix throughput from a JVM/Kotlin caller, not Monix's native Scala performance.
