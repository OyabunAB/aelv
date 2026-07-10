package se.oyabun.aelv.benchmarks

import io.reactivex.rxjava3.core.Observable
import org.openjdk.jmh.annotations.*

@BenchmarkMode(Mode.Throughput)
@State(Scope.Thread)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(1)
open class RxJavaBenchmark {

    @Param("1000", "10000")
    var size: Int = 1000

    @Benchmark
    fun baseline_toList(): Int =
        Observable.range(0, size).toList().blockingGet().size

    @Benchmark
    fun map_toList(): Int =
        Observable.range(0, size).map { it * 2 }.toList().blockingGet().size

    @Benchmark
    fun filter_toList(): Int =
        Observable.range(0, size).filter { it % 2 == 0 }.toList().blockingGet().size

    @Benchmark
    fun take_toList(): Int =
        Observable.range(0, size).take(size.toLong() / 2).toList().blockingGet().size

    @Benchmark
    fun chain_map_filter_take(): Int =
        Observable.range(0, size)
            .map { it * 2 }
            .filter { it % 4 == 0 }
            .take(size.toLong() / 4)
            .toList().blockingGet().size

    @Benchmark
    fun concatMap_toList(): Int =
        Observable.range(0, size / 10)
            .concatMap { i -> Observable.just(i, i + 1, i + 2) }
            .toList().blockingGet().size

    @Benchmark
    fun flatMap_toList(): Int =
        Observable.range(0, size / 10)
            .flatMap { i -> Observable.just(i, i + 1, i + 2) }
            .toList().blockingGet().size

    @Benchmark
    fun fold_sum(): Long =
        Observable.range(0, size)
            .reduce(0L) { acc, i -> acc + i }
            .blockingGet()!!
}
