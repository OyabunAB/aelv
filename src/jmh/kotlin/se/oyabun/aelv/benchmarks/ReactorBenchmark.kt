package se.oyabun.aelv.benchmarks

import org.openjdk.jmh.annotations.*
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

@BenchmarkMode(Mode.Throughput)
@State(Scope.Thread)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(1)
open class ReactorBenchmark {

    @Param("1000", "10000")
    var size: Int = 1000

    @Benchmark
    fun baseline_toList(): Int =
        Flux.range(0, size).collectList().block()!!.size

    @Benchmark
    fun map_toList(): Int =
        Flux.range(0, size).map { it * 2 }.collectList().block()!!.size

    @Benchmark
    fun filter_toList(): Int =
        Flux.range(0, size).filter { it % 2 == 0 }.collectList().block()!!.size

    @Benchmark
    fun take_toList(): Int =
        Flux.range(0, size).take(size.toLong() / 2).collectList().block()!!.size

    @Benchmark
    fun chain_map_filter_take(): Int =
        Flux.range(0, size)
            .map { it * 2 }
            .filter { it % 4 == 0 }
            .take(size.toLong() / 4)
            .collectList().block()!!.size

    @Benchmark
    fun concatMap_toList(): Int =
        Flux.range(0, size / 10)
            .concatMap { i -> Flux.just(i, i + 1, i + 2) }
            .collectList().block()!!.size

    @Benchmark
    fun flatMap_toList(): Int =
        Flux.range(0, size / 10)
            .flatMap { i -> Flux.just(i, i + 1, i + 2) }
            .collectList().block()!!.size

    @Benchmark
    fun fold_sum(): Long =
        Flux.range(0, size)
            .reduce(0L) { acc, i -> acc + i }
            .block()!!
}
