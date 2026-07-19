/*
 * Copyright 2026 Oyabun AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package se.oyabun.aelv.benchmarks

import org.openjdk.jmh.annotations.*
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import reactor.core.scheduler.Schedulers
import java.util.concurrent.CountDownLatch

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
    fun sink_multicast_single_subscriber(): Int {
        val sink = Sinks.many().multicast().onBackpressureBuffer<Int>(size * 2)
        val latch = CountDownLatch(1)
        sink.asFlux().subscribeOn(Schedulers.boundedElastic()).collectList().subscribe { latch.countDown() }
        repeat(size) { sink.tryEmitNext(it) }
        sink.tryEmitComplete()
        latch.await()
        return size
    }

    @Benchmark
    fun sink_multicast_four_subscribers(): Int {
        val sink = Sinks.many().multicast().onBackpressureBuffer<Int>(size * 2)
        val latch = CountDownLatch(4)
        repeat(4) { sink.asFlux().subscribeOn(Schedulers.boundedElastic()).collectList().subscribe { latch.countDown() } }
        repeat(size) { sink.tryEmitNext(it) }
        sink.tryEmitComplete()
        latch.await()
        return size
    }
}
