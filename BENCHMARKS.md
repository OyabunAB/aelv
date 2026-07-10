# Benchmarks

Throughput comparison against [Project Reactor](https://projectreactor.io/) 3.7.6,
[RxJava](https://github.com/ReactiveX/RxJava) 3.1.10, and
[Mutiny](https://smallrye.io/smallrye-mutiny/) 2.8.0.

## Methodology

- **Tool:** JMH 1.37
- **Mode:** Throughput (ops/ms, higher is better)
- **Warmup:** 3 iterations × 3 s
- **Measurement:** 5 iterations × 3 s
- **Fork:** 1
- **JVM:** OpenJDK 21.0.6 (Corretto)
- **CPU:** Intel Core i9-8950HK @ 2.90 GHz
- **Source size:** 1000 items per operation

All frameworks use equivalent source types: `Many.range` / `Flux.range` / `Observable.range` /
`Multi.createFrom().range` — primitive integer sources with no boxing overhead.
Inner streams in `concatMap`/`flatMap` use `Many.just` / `Flux.just` / `Observable.just` /
`Multi.createFrom().items`.

Run benchmarks locally:

```
./gradlew jmhJar
java -jar build/libs/aelv-*-jmh.jar -wi 3 -i 5 -f 1 -p size=1000 -tu ms
```

## Results

| Benchmark | aelv | RxJava | Mutiny | Reactor |
|---|---:|---:|---:|---:|
| baseline_toList | **226** | 154 | 57 | 48 |
| map_toList | **132** | 114 | 55 | 47 |
| filter_toList | **225** | 170 | 100 | 60 |
| take_toList | **301** | 236 | 184 | 76 |
| fold_sum | **188** | 160 | 68 | 43 |
| chain (map→filter→take) | **269** | 254 | 136 | 101 |
| concatMap_toList | 49 | 57 | **80** | 56 |
| flatMap_sequential | 54 | — | — | — |
| flatMap_concurrent | 35 | **87** | 33 | 55 |

*ops/ms — higher is better.*

## Notes

**Fused pipelines** (`baseline`, `map`, `filter`, `take`, `fold`, `chain`) activate aelv's
synchronous fusion protocol. When the entire chain from source to terminal is synchronous, aelv
bypasses the coroutine callback machinery and runs a tight poll loop — no state machine transitions,
no signal allocations. This puts aelv ahead of all frameworks on fused pipelines.

**`concatMap`** runs inner streams inline without launching coroutines. aelv's slightly lower
number vs Mutiny reflects the coroutine state machine overhead on each inner `source` call — the
inner streams (`Many.just`) are not yet fused through `concatMap`.

**`flatMap_concurrent`** launches inner streams inline (no `launch`, serialised via `Mutex`).
RxJava's advantage here is its lock-free drain loop. For real IO-bound workloads where inner
streams suspend on network/disk, the difference is immaterial.

**Backpressure** is unconditional. The fusion fast path only activates inside `collect()` — the
internal synchronous terminal path. Any async operator (`publishOn`, `subscribeOn`, `Sink`,
RS `subscribe()`) routes through the full three-callback protocol with demand signalling and
cancellation, satisfying Reactive Streams §1.1–§3.17.
